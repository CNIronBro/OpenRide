package com.ironbro.didi.service.dispatch;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ironbro.didi.config.RabbitMqConfig;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.OrderDispatchLog;
import com.ironbro.didi.enums.AuditStatus;
import com.ironbro.didi.enums.DispatchAction;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.service.DispatchScoreService;
import com.ironbro.didi.service.DriverLocationService;
import com.ironbro.didi.websocket.WebSocketSessionManager;
import com.ironbro.didi.websocket.WsMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 全局派单调度器（tick 调度器）
 *
 * 业务背景：
 * 现有"每订单独立贪心派单"方案在司机稀缺、多订单集中到达时会出现局部最优、全局较差的问题。
 * 本调度器引入全局匹配机制：订单先进入 Redis 等待池，每 2s 触发一次 tick，
 * 统一取出所有待派订单，根据实时供需比决定走 KM 全局匹配还是贪心派单，
 * 每个订单只推送 1 个最优司机（而非批量竞争），最大化总成交率。
 *
 * 核心流程：
 * 1. 从等待池读取所有待派订单
 * 2. 过滤无效订单（状态异常 / 超时滞留）
 * 3. 对每个有效订单 GEO 召回候选司机，评分排序，存入 Redis
 * 4. 计算供需比（所有订单候选司机并集去重 / 订单数）
 * 5. 供给充足（≥1.5）→ 贪心派单；供给紧张（<1.5）→ KM 全局匹配
 * 6. 对每个匹配对推送通知，发延迟重试消息，从等待池移除
 * 7. 未匹配订单留在等待池，下一 tick 继续处理（优先级权重随等待时间自动提升）
 *
 * 多实例安全：
 * tick 入口加 Redisson tryLock（key=lock:dispatch:tick，waitTime=0，leaseTime=5s）。
 * 多实例场景下所有实例竞争同一把锁，抢到的执行，其余立即跳过本次 tick，
 * 保证同一批订单只被处理一次。
 * 单实例场景下若某次 tick 执行超过 2s，锁防止下一个 tick 重入，避免重复处理。
 *
 * 与现有系统的集成边界：
 * - 变化：订单创建后写等待池（阶段 2 已完成），本调度器替代 DispatchConsumer 的初始派单职责
 * - 不变：RetryConsumer（超时重试）、CancelConsumer（取消）、MySQL CAS 接单、xxl-job 补偿
 * - 延迟消息格式兼容现有 RetryConsumer（携带 batchIndex=0），阶段 5 改造 RetryConsumer 后切换为版本号机制
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GlobalDispatchScheduler {

    // ----------------------------------------------------------------
    // 常量
    // ----------------------------------------------------------------

    /**
     * tick 分布式锁 key
     * 多实例场景下所有实例竞争此锁，保证同一 tick 只有一个实例执行
     */
    private static final String TICK_LOCK_KEY = "lock:dispatch:tick";

    /**
     * 供需比阈值：高于此值走贪心，低于此值走 KM 全局匹配
     * 供需比 = 所有订单候选司机并集去重数 / 当前 tick 订单数
     */
    private static final double SUPPLY_DEMAND_THRESHOLD = 1.5;

    /** GEO 召回初始半径（公里） */
    private static final double INITIAL_RADIUS_KM = 5.0;

    /** 最多召回候选司机数量 */
    private static final int MAX_CANDIDATES = 20;

    /** 候选列表在 Redis 中的 TTL（分钟） */
    private static final long CANDIDATES_TTL_MINUTES = 10;

    /**
     * driver:pending:order TTL（秒）
     * 新方案每次只推 1 个司机，10s 窗口到期后 RetryConsumer 推下一个
     */
    private static final int PENDING_TTL_SECONDS = 10;

    /** 延迟重试消息的延迟时长（毫秒） */
    private static final int DISPATCH_DELAY_MS = 10_000;

    /**
     * 优先级权重阈值（毫秒）
     * 等待超过 30s 权重 1.2x，超过 60s 权重 1.5x（上限），防止订单饥饿
     * 见设计文档 3.5 节
     */
    private static final long PRIORITY_WAIT_30S_MS = 30_000L;
    private static final long PRIORITY_WAIT_60S_MS = 60_000L;
    private static final double PRIORITY_WEIGHT_NORMAL = 1.0;
    private static final double PRIORITY_WEIGHT_30S   = 1.2;
    private static final double PRIORITY_WEIGHT_60S   = 1.5;

    // ----------------------------------------------------------------
    // 依赖注入
    // ----------------------------------------------------------------

    private final DispatchWaitingPool waitingPool;
    private final OrderMapper orderMapper;
    private final DriverMapper driverMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;
    private final DriverLocationService locationService;
    private final DispatchScoreService scoreService;
    private final KuhnMunkresAlgorithm kmAlgorithm;
    private final RabbitTemplate rabbitTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;
    private final WebSocketSessionManager wsSessionManager;

    // ----------------------------------------------------------------
    // tick 入口
    // ----------------------------------------------------------------

    /**
     * 每 2s 触发一次全局派单 tick
     *
     * fixedRate=2000：从上一次调用开始计时，2s 后再次触发（不等上次执行完毕）。
     * 若上次 tick 执行超过 2s，Redisson 锁会阻止下一个 tick 重入，
     * 保证同一批订单不被重复处理。
     */
    @Scheduled(fixedRate = 2000)
    public void tick() {
        RLock lock = redissonClient.getLock(TICK_LOCK_KEY);
        boolean locked = false;
        try {
            // waitTime=0：抢不到锁立即返回，不阻塞调度线程
            // leaseTime=5s：大于 tick 间隔（2s），防止锁提前释放导致重入
            locked = lock.tryLock(0, 5, TimeUnit.SECONDS);
            if (!locked) {
                log.debug("tick 锁未获取，跳过本次 tick（另一实例正在执行）");
                return;
            }
            doTick();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("tick 锁等待被中断");
        } catch (Exception e) {
            log.error("tick 执行异常", e);
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    // ----------------------------------------------------------------
    // 核心 tick 逻辑
    // ----------------------------------------------------------------

    /**
     * tick 核心逻辑：读取等待池 → 过滤 → 召回 → 匹配 → 推送
     */
    private void doTick() {
        // 3.3 从等待池读取所有待派订单（按进入时间升序）
        Set<String> orderIdStrs = waitingPool.getAllOrdered();
        if (orderIdStrs.isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();

        // 3.4 过滤无效订单：状态异常或超时滞留的订单从等待池移除，不参与本次匹配
        // 同时缓存查询结果，避免后续召回阶段重复查库
        Map<Long, Order> validOrderMap = new LinkedHashMap<>(); // orderId → Order，过滤阶段缓存，供召回阶段复用
        for (String orderIdStr : orderIdStrs) {
            Long orderId = Long.parseLong(orderIdStr);

            // 兜底 TTL 检查：超过 STALE_THRESHOLD_MS（180s）视为异常滞留，直接清除
            Double score = waitingPool.getScore(orderId);
            if (score != null && (now - score.longValue()) > DispatchWaitingPool.STALE_THRESHOLD_MS) {
                log.warn("等待池发现超时滞留订单，清除 orderId={} waitedMs={}", orderId, now - score.longValue());
                waitingPool.remove(orderId);
                continue;
            }

            // 状态校验：只有 DISPATCHING 状态的订单才参与派单
            Order order = orderMapper.selectById(orderId);
            if (order == null || order.getStatus() != OrderStatus.DISPATCHING) {
                log.info("订单状态异常，从等待池移除 orderId={} status={}",
                        orderId, order != null ? order.getStatus() : "null");
                waitingPool.remove(orderId);
                continue;
            }

            validOrderMap.put(orderId, order); // 缓存，供下方召回阶段直接使用
        }

        if (validOrderMap.isEmpty()) {
            return;
        }

        log.info("tick 开始处理 {} 个待派订单", validOrderMap.size());

        // 3.5 对每个有效订单召回候选司机，评分排序，存入 Redis
        Map<Long, List<Long>> candidatesMap = new LinkedHashMap<>();
        // candidateDetailsMap 保留每个订单的 CandidateDriver 列表（含距离），用于 KM 矩阵构造
        Map<Long, List<DispatchScoreService.CandidateDriver>> candidateDetailsMap = new LinkedHashMap<>();

        for (Long orderId : validOrderMap.keySet()) {
            Order order = validOrderMap.get(orderId); // 复用过滤阶段已查询的订单，避免重复查库

            double originLat = order.getOriginLat().doubleValue();
            double originLng = order.getOriginLng().doubleValue();
            String city = "default";

            List<Long> nearbyIds = locationService.nearbyDrivers(
                    originLat, originLng, INITIAL_RADIUS_KM, city, MAX_CANDIDATES);

            if (nearbyIds.isEmpty()) {
                log.info("订单附近暂无在线司机，留在等待池 orderId={}", orderId);
                continue;
            }

            // 查询司机详情，过滤状态异常的司机
            List<Driver> drivers = driverMapper.selectList(
                    new LambdaQueryWrapper<Driver>()
                            .in(Driver::getId, nearbyIds)
                            .eq(Driver::getStatus, DriverStatus.ONLINE)
                            .eq(Driver::getAuditStatus, AuditStatus.APPROVED));

            if (drivers.isEmpty()) {
                log.info("候选司机均不可用（状态变更竞态），留在等待池 orderId={}", orderId);
                continue;
            }

            List<DispatchScoreService.CandidateDriver> candidateDetails =
                    buildCandidates(drivers, originLat, originLng, city);

            List<Long> sortedDriverIds = scoreService.score(candidateDetails, INITIAL_RADIUS_KM);

            // 将候选列表存入 Redis（TTL=10min），供 RetryConsumer 超时重试时使用
            try {
                redisTemplate.opsForValue().set("order:candidates:" + orderId,
                        objectMapper.writeValueAsString(sortedDriverIds),
                        Duration.ofMinutes(CANDIDATES_TTL_MINUTES));
            } catch (Exception e) {
                log.error("候选列表序列化失败，跳过该订单 orderId={}", orderId, e);
                continue;
            }

            // 写入初始搜索半径，供 RetryConsumer 候选耗尽后扩圈使用
            redisTemplate.opsForValue().set("order:dispatch:radius:" + orderId,
                    String.valueOf(INITIAL_RADIUS_KM), Duration.ofMinutes(CANDIDATES_TTL_MINUTES));

            candidatesMap.put(orderId, sortedDriverIds);
            candidateDetailsMap.put(orderId, candidateDetails);
        }

        if (candidatesMap.isEmpty()) {
            log.info("tick: 所有订单均无可用候选司机，等待下一 tick");
            return;
        }

        // 3.6 计算供需比
        // 取所有订单候选司机的并集去重，而非每订单平均候选数。
        // 原因：若 30 笔订单共享同一批 20 个司机，每订单平均候选数=20（看似充足），
        // 但实际司机/订单比=20/30≈0.67，属于严重供给紧张，应走 KM 路径。
        Set<Long> allCandidateDrivers = new HashSet<>();
        for (List<Long> driverIds : candidatesMap.values()) {
            allCandidateDrivers.addAll(driverIds);
        }
        int totalOrders = candidatesMap.size();
        double supplyDemandRatio = (double) allCandidateDrivers.size() / totalOrders;

        log.info("tick 供需比={} (司机并集={}, 订单数={})",
                String.format("%.2f", supplyDemandRatio), allCandidateDrivers.size(), totalOrders);

        // 3.7 / 3.8 根据供需比选择匹配策略
        Map<Long, Long> matchResult;
        if (supplyDemandRatio >= SUPPLY_DEMAND_THRESHOLD) {
            log.info("供给充足，走贪心派单");
            matchResult = greedyMatch(candidatesMap, now);
        } else {
            log.info("供给紧张，走 KM 全局匹配");
            matchResult = kmMatch(candidatesMap, candidateDetailsMap, allCandidateDrivers, now);
        }

        // 3.12 ~ 3.16 推送匹配结果
        for (Map.Entry<Long, Long> entry : matchResult.entrySet()) {
            pushMatchResult(entry.getKey(), entry.getValue(), candidatesMap.get(entry.getKey()));
        }

        // 3.11 未被匹配的订单留在等待池，参与下一个 tick（优先级权重随等待时间自动提升）
        int unmatchedCount = candidatesMap.size() - matchResult.size();
        if (unmatchedCount > 0) {
            log.info("tick: {} 个订单未被匹配（司机不足），留在等待池参与下一 tick", unmatchedCount);
        }
    }

    // ----------------------------------------------------------------
    // 贪心匹配（供给充足时使用）
    // ----------------------------------------------------------------

    /**
     * 贪心二部图匹配
     *
     * 按订单优先级降序遍历，对每个订单从其候选列表中取第一个尚未被分配的司机。
     * 已分配的司机跳过，保证每个司机最多匹配一个订单。
     *
     * 供给充足时（供需比 >= 1.5），贪心结果与全局最优差距极小，
     * 且实现简单、延迟低，是此场景下的最优选择。
     *
     * @param candidatesMap 订单 → 评分排序后的候选司机 ID 列表
     * @param now           当前时间戳（毫秒），用于计算优先级权重
     * @return 匹配结果：orderId → driverId
     */
    private Map<Long, Long> greedyMatch(Map<Long, List<Long>> candidatesMap, long now) {
        Map<Long, Long> result = new HashMap<>();
        Set<Long> assignedDrivers = new HashSet<>();

        // 预先计算所有订单的优先级权重，避免排序过程中反复访问 Redis。
        // 若在排序比较器内直接调用 getPriorityWeight（内含 Redis ZSCORE），
        // 每次比较都触发一次网络请求，O(N log N) 次调用性能差，
        // 且排序过程中 score 可能变化，导致比较器违反传递性，可能抛 IllegalArgumentException。
        Map<Long, Double> priorityWeights = new HashMap<>();
        for (Long orderId : candidatesMap.keySet()) {
            priorityWeights.put(orderId, getPriorityWeight(orderId, now));
        }

        List<Long> sortedOrderIds = new ArrayList<>(candidatesMap.keySet());
        sortedOrderIds.sort((a, b) ->
                Double.compare(priorityWeights.get(b), priorityWeights.get(a)));

        for (Long orderId : sortedOrderIds) {
            List<Long> candidates = candidatesMap.get(orderId);
            Set<String> alreadyDispatched = redisTemplate.opsForSet()
                    .members("order:dispatched:drivers:" + orderId);

            for (Long driverId : candidates) {
                boolean alreadySent = alreadyDispatched != null
                        && alreadyDispatched.contains(String.valueOf(driverId));
                if (!assignedDrivers.contains(driverId) && !alreadySent) {
                    result.put(orderId, driverId);
                    assignedDrivers.add(driverId);
                    break;
                }
            }
        }

        log.info("贪心匹配完成：{} 个订单成功匹配，{} 个未匹配",
                result.size(), candidatesMap.size() - result.size());
        return result;
    }

    // ----------------------------------------------------------------
    // KM 全局匹配（供给紧张时使用）
    // ----------------------------------------------------------------

    /**
     * KM 全局最优匹配
     *
     * 将所有待派订单和候选司机并集建模为加权二部图，
     * 边权 = DispatchScoreService 评分 × 订单优先级系数，
     * 调用 KuhnMunkresAlgorithm 求解最大权匹配，最大化总成交率。
     *
     * 收益矩阵构造规则：
     * - matrix[i][j] = score(order_i, driver_j) * priorityWeight(order_i)
     * - 若 driver_j 不在 order_i 的候选列表中（距离超出召回半径），收益 = 0
     * - 若 driver_j 已被推送过 order_i（order:dispatched:drivers），收益 = 0（防重推）
     *
     * @param candidatesMap       订单 → 评分排序后的候选司机 ID 列表
     * @param candidateDetailsMap 订单 → CandidateDriver 列表（含距离，用于计算评分）
     * @param allCandidateDrivers 所有订单候选司机的并集（KM 右侧节点集合）
     * @param now                 当前时间戳（毫秒），用于计算优先级权重
     * @return 匹配结果：orderId → driverId（未匹配的订单不在结果中）
     */
    private Map<Long, Long> kmMatch(
            Map<Long, List<Long>> candidatesMap,
            Map<Long, List<DispatchScoreService.CandidateDriver>> candidateDetailsMap,
            Set<Long> allCandidateDrivers,
            long now) {

        List<Long> orderIds = new ArrayList<>(candidatesMap.keySet());
        // allCandidateDrivers 是 HashSet，迭代顺序不稳定。排序后转 List，
        // 保证同样输入下矩阵列顺序一致，便于日志复现和调试。
        List<Long> driverIds = new ArrayList<>(allCandidateDrivers);
        Collections.sort(driverIds);
        int orderCount = orderIds.size();
        int driverCount = driverIds.size();

        // 构造收益矩阵 matrix[i][j] = 订单 i 分配给司机 j 的收益
        double[][] costMatrix = new double[orderCount][driverCount];

        for (int i = 0; i < orderCount; i++) {
            Long orderId = orderIds.get(i);
            double priorityWeight = getPriorityWeight(orderId, now);

            // 构建该订单的 driverId → CandidateDriver 映射，便于 O(1) 查找
            List<DispatchScoreService.CandidateDriver> details = candidateDetailsMap.get(orderId);
            Map<Long, DispatchScoreService.CandidateDriver> detailMap = new HashMap<>();
            if (details != null) {
                for (DispatchScoreService.CandidateDriver cd : details) {
                    detailMap.put(cd.driver().getId(), cd);
                }
            }

            // 读取该订单已推送过的司机集合，防止重复推送
            Set<String> alreadyDispatched = redisTemplate.opsForSet()
                    .members("order:dispatched:drivers:" + orderId);

            for (int j = 0; j < driverCount; j++) {
                Long driverId = driverIds.get(j);

                // 已推送过的司机收益置 0，KM 不会将其匹配给该订单
                boolean alreadySent = alreadyDispatched != null
                        && alreadyDispatched.contains(String.valueOf(driverId));
                if (alreadySent) {
                    costMatrix[i][j] = 0;
                    continue;
                }

                DispatchScoreService.CandidateDriver cd = detailMap.get(driverId);
                if (cd == null) {
                    // 该司机不在此订单的候选列表中（超出召回半径），收益 = 0
                    costMatrix[i][j] = 0;
                } else {
                    // 评分 × 优先级系数，高优先级订单在匹配中更容易获得优质司机
                    costMatrix[i][j] = scoreService.scoreOne(cd, INITIAL_RADIUS_KM) * priorityWeight;
                }
            }
        }

        // 调用 KM 算法求解最大权匹配
        // 复杂度 O(n³)，n = max(orderCount, driverCount)，本系统规模下毫秒级完成
        int[] matchArray = kmAlgorithm.solve(costMatrix, orderCount, driverCount);

        // 将 KM 结果转换为 orderId → driverId 映射
        // matchArray[i] = j 表示订单 i 匹配到司机 j；-1 表示未匹配（司机数不足）
        Map<Long, Long> result = new HashMap<>();
        for (int i = 0; i < orderCount; i++) {
            int driverIdx = matchArray[i];
            if (driverIdx >= 0) {
                result.put(orderIds.get(i), driverIds.get(driverIdx));
            }
        }

        log.info("KM 匹配完成：{} 个订单成功匹配，{} 个未匹配",
                result.size(), orderCount - result.size());
        return result;
    }

    // ----------------------------------------------------------------
    // 推送匹配结果
    // ----------------------------------------------------------------

    /**
     * 推送单个匹配对的派单通知，并发送延迟重试消息
     *
     * 执行步骤：
     * 1. 写 driver:pending:order:{driverId}（TTL=10s），司机端轮询此 key 感知新订单
     * 2. 写 order:dispatch:current:index:{orderId}，记录当前推送司机在候选列表中的索引
     *    供阶段 4 拒单接口和阶段 5 RetryConsumer 改造后使用
     * 3. 写 order:dispatch:version:{orderId}，推送版本号（初始为 0）
     *    阶段 4 拒单时递增，RetryConsumer 消费时校验版本号实现幂等
     * 4. 写 order:dispatch:batch:index:{orderId}=0，兼容现有 RetryConsumer 的幂等校验逻辑
     *    阶段 5 改造 RetryConsumer 后此 key 可废弃
     * 5. 将司机加入 order:dispatched:drivers:{orderId}，防止重复推送
     * 6. 发延迟消息到 dispatch.retry.queue（x-delay=10s），10s 后 RetryConsumer 检查接单状态
     * 7. 从等待池移除该订单（已进入派单流程，不再参与下一 tick）
     * 8. WS 推送派单通知（Redis key 作为兜底，WS 失败时司机端轮询仍可感知）
     * 9. 记录派单日志
     *
     * @param orderId    订单 ID
     * @param driverId   匹配到的司机 ID
     * @param candidates 该订单的完整候选列表（用于计算当前司机的索引）
     */
    private void pushMatchResult(Long orderId, Long driverId, List<Long> candidates) {
        // 3.12 写司机待接单通知（TTL=10s，与派单窗口一致）
        redisTemplate.opsForValue().set(
                "driver:pending:order:" + driverId,
                String.valueOf(orderId),
                Duration.ofSeconds(PENDING_TTL_SECONDS));

        // 3.13 记录当前推送司机在候选列表中的索引（供阶段 4/5 使用）
        int currentIndex = candidates != null ? candidates.indexOf(driverId) : 0;
        if (currentIndex < 0) currentIndex = 0;
        redisTemplate.opsForValue().set(
                "order:dispatch:current:index:" + orderId,
                String.valueOf(currentIndex),
                Duration.ofMinutes(CANDIDATES_TTL_MINUTES));

        // 3.13 写推送版本号（初始为 0），阶段 4 拒单时递增，用于延迟消息幂等校验
        redisTemplate.opsForValue().set(
                "order:dispatch:version:" + orderId,
                "0",
                Duration.ofMinutes(CANDIDATES_TTL_MINUTES));

        // 兼容现有 RetryConsumer：写 batch:index=0，使其幂等校验能正常工作
        // 阶段 5 改造 RetryConsumer 后此 key 可废弃
        redisTemplate.opsForValue().set(
                "order:dispatch:batch:index:" + orderId,
                "0",
                Duration.ofMinutes(CANDIDATES_TTL_MINUTES));

        // 3.15 将司机加入已推送集合，防止重复推送
        redisTemplate.opsForSet().add("order:dispatched:drivers:" + orderId, String.valueOf(driverId));
        redisTemplate.expire("order:dispatched:drivers:" + orderId, Duration.ofMinutes(CANDIDATES_TTL_MINUTES));

        // 3.14 发延迟消息（x-delay=10s），10s 后 RetryConsumer 检查接单状态
        // 消息体携带 batchIndex=0，兼容现有 RetryConsumer 的幂等校验逻辑
        Map<String, Object> retryMsg = new HashMap<>();
        retryMsg.put("orderId", orderId);
        retryMsg.put("batchIndex", 0);
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_RETRY,
                retryMsg,
                m -> {
                    m.getMessageProperties().setHeader("x-delay", DISPATCH_DELAY_MS);
                    return m;
                });

        // 3.16 从等待池移除已成功推送的订单
        waitingPool.remove(orderId);

        // WS 推送派单通知（Redis key 作为兜底，WS 失败时司机端轮询仍可感知）
        try {
            Driver driver = driverMapper.selectById(driverId);
            if (driver != null) {
                wsSessionManager.sendToUser(driver.getUserId(),
                        new WsMessage("DISPATCH_NOTIFY", Map.of("orderId", orderId)));
            }
        } catch (Exception e) {
            log.warn("WS 推送派单通知失败，依赖司机端轮询兜底 driverId={} orderId={}", driverId, orderId, e);
        }

        // 记录派单日志
        saveDispatchLog(orderId, driverId, DispatchAction.DISPATCHED,
                "GlobalDispatchScheduler 派单 index=" + currentIndex);

        log.info("派单推送成功 orderId={} driverId={} candidateIndex={}", orderId, driverId, currentIndex);
    }

    // ----------------------------------------------------------------
    // 辅助方法
    // ----------------------------------------------------------------

    /**
     * 构建候选司机列表（含距离信息）
     *
     * 通过 Redis GEO 的 GEOPOS 命令获取每个司机的坐标，
     * 再用 Haversine 公式计算与下单位置的距离，用于调度评分。
     *
     * 注意：Spring Data Redis 的 GeoOperations.distance() 只能计算 GEO 集合中两个成员之间的距离，
     * 无法直接计算成员与任意坐标的距离，因此改用 GEOPOS + Haversine 方式。
     */
    private List<DispatchScoreService.CandidateDriver> buildCandidates(
            List<Driver> drivers, double originLat, double originLng, String city) {
        String geoKey = "driver:online:" + city;
        List<DispatchScoreService.CandidateDriver> candidates = new ArrayList<>();

        for (Driver driver : drivers) {
            var positions = redisTemplate.opsForGeo().position(geoKey, String.valueOf(driver.getId()));
            double distKm = INITIAL_RADIUS_KM; // GEO 中无记录时的兜底值（最大距离）
            if (positions != null && !positions.isEmpty() && positions.get(0) != null) {
                Point driverPos = positions.get(0);
                // Point.getX() = 经度，Point.getY() = 纬度（Redis GEO 存储顺序）
                distKm = haversineKm(originLat, originLng, driverPos.getY(), driverPos.getX());
            }
            candidates.add(new DispatchScoreService.CandidateDriver(driver, distKm));
        }
        return candidates;
    }

    /**
     * 计算订单的优先级权重系数
     *
     * 等待时间越长，权重越高，防止订单饥饿（长时间未被匹配的订单在 KM 矩阵中更容易获得优质司机）。
     * 权重作用于边权（评分），不绕过匹配流程，保证全局最优性不被破坏。
     *
     * 权重上限 1.5x，避免权重膨胀影响其他订单的匹配质量。
     *
     * @param orderId 订单 ID
     * @param now     当前时间戳（毫秒）
     * @return 优先级权重系数（1.0 / 1.2 / 1.5）
     */
    private double getPriorityWeight(Long orderId, long now) {
        Double score = waitingPool.getScore(orderId);
        if (score == null) return PRIORITY_WEIGHT_NORMAL;
        long waitedMs = now - score.longValue();
        if (waitedMs >= PRIORITY_WAIT_60S_MS) return PRIORITY_WEIGHT_60S;
        if (waitedMs >= PRIORITY_WAIT_30S_MS) return PRIORITY_WEIGHT_30S;
        return PRIORITY_WEIGHT_NORMAL;
    }

    /** 记录派单日志 */
    private void saveDispatchLog(Long orderId, Long driverId, DispatchAction action, String remark) {
        OrderDispatchLog logEntry = new OrderDispatchLog();
        logEntry.setOrderId(orderId);
        logEntry.setDriverId(driverId);
        logEntry.setAction(action);
        logEntry.setRemark(remark);
        logEntry.setCreatedAt(LocalDateTime.now());
        dispatchLogMapper.insert(logEntry);
    }

    /** Haversine 公式计算两点距离（公里） */
    private double haversineKm(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}