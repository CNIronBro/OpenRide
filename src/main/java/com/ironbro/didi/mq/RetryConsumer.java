package com.ironbro.didi.mq;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.core.type.TypeReference;
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
import com.ironbro.didi.service.DriverLocationService;
import com.ironbro.didi.service.OrderService;
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 派单超时重试消费者
 *
 * 消费 dispatch.retry.queue 中的超时重试消息（由 dispatch.exchange x-delay=10s 延迟到期后路由而来）。
 *
 * 核心逻辑：
 * 1. 全局超时检查：now - order.createdAt > MAX_WAIT_SECONDS（120s）→ 取消订单
 * 2. 订单状态校验：非 DISPATCHING → 忽略（已接单或已取消）
 * 3. 幂等校验：消息携带的 version 与 Redis 中 order:dispatch:version:{orderId} 不一致 → 忽略（过期消息）
 *    特殊值 version=-1：跳过幂等校验，直接走 handleExhausted（候选耗尽/无司机扩圈逻辑）
 * 4. 候选列表还有剩余 → 取下一个候选司机推送，递增版本号，发新延迟消息
 * 5. 候选列表耗尽 → handleExhausted：重新 GEO 召回 + 扩圈
 *
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryConsumer {

    private final OrderMapper orderMapper;
    private final DriverMapper driverMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;
    private final RabbitTemplate rabbitTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;
    private final DriverLocationService locationService;
    private final OrderService orderService;

    /**
     * 每批派单窗口时长
     * 10s 后统一判断是否推下一个候选司机
     */
    private static final int BATCH_DELAY_MS = 10_000;

    /**
     * 从下单时刻起算的最大等待时间
     * 超过此值无论处于哪个阶段（正常轮转/扩圈/等待新司机）均取消订单
     */
    private static final int MAX_WAIT_SECONDS = 120;

    /** 最多召回候选司机数量 */
    private static final int MAX_CANDIDATES = 20;

    /**
     * 动态扩圈半径步进表（公里）
     *
     * 候选列表耗尽且当前半径无新司机时，按此表依次扩大搜索半径。
     * 步进设计原则：前几档小步快跑（5→8→12），最后一档拉到合理上限（15km）。
     * 超过 15km 的派单在实际场景中司机接单意愿极低，继续扩圈意义不大。
     */
    private static final double[] RADIUS_STEPS = {5.0, 8.0, 12.0, 15.0};

    /** 搜索半径上限（公里），超过此值不再扩大 */
    private static final double MAX_RADIUS_KM = 15.0;

    /**
     * 消费超时重试消息
     *
     * 消息体格式（Map）：
     * {
     *   "orderId":  Long,
     *   "version":  Integer  // 发送此延迟消息时的版本号；缺失时默认 0（兼容 GlobalDispatchScheduler 初始消息）
     *                        // 特殊值 -1 表示无候选司机等待扩圈，跳过幂等校验直接走 handleExhausted
     * }
     */
    @RabbitListener(queues = RabbitMqConfig.DISPATCH_RETRY_QUEUE)
    public void onRetry(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Map<String, Object> body;
        try {
            body = objectMapper.readValue(message.getBody(), new TypeReference<>() {});
        } catch (Exception e) {
            log.error("重试消息解析失败，NACK 进死信", e);
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        Long orderId = ((Number) body.get("orderId")).longValue();

        // 解析版本号：所有发送方（GlobalDispatchScheduler / OrderService.pushNextDriver / 本类）
        // 均携带 version 字段，缺失时默认 0 仅作防御性兜底
        Object versionRaw = body.get("version");
        int msgVersion = versionRaw != null ? ((Number) versionRaw).intValue() : 0;

        // 补偿幂等锁：防止同一超时消息被多个消费者实例并发处理
        String lockKey = "lock:compensate:" + orderId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!locked) {
                log.info("补偿幂等锁未获取，跳过重复消费 orderId={}", orderId);
                channel.basicAck(deliveryTag, false);
                return;
            }
            doRetry(orderId, msgVersion);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("重试处理失败 orderId={}", orderId, e);
            channel.basicNack(deliveryTag, false, false);
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 核心重试逻辑
     *
     * 处理顺序：
     * 1. 全局超时检查（从下单时刻起算 120s）
     * 2. 订单状态校验
     * 3. version=-1 特殊值处理（直接走候选耗尽逻辑）
     * 4. 幂等校验（比较 msgVersion 与 Redis 当前版本号）
     * 5. 取下一个候选司机推送，或候选耗尽触发 handleExhausted
     *
     * @param orderId    订单 ID
     * @param msgVersion 消息中携带的版本号（-1 表示无候选司机等待扩圈）
     */
    private void doRetry(Long orderId, int msgVersion) {
        // 1. 读取订单（后续多处使用）
        Order order = orderMapper.selectById(orderId);

        // 2. 全局超时检查：从下单时刻起算，超过 MAX_WAIT_SECONDS 则取消
        // 使用 order.createdAt 而非消息中的时间戳，避免消息延迟导致误差
        if (order != null) {
            long waitedSeconds = Duration.between(order.getCreatedAt(), LocalDateTime.now()).getSeconds();
            if (waitedSeconds > MAX_WAIT_SECONDS) {
                log.info("派单超时（{}s > {}s），取消订单 orderId={}", waitedSeconds, MAX_WAIT_SECONDS, orderId);
                saveDispatchLog(orderId, null, DispatchAction.TIMEOUT,
                        "派单总等待超时 " + waitedSeconds + "s");
                sendCancelMessage(orderId, "派单超时，无司机接单");
                return;
            }
        }

        // 3. 订单状态校验
        if (order == null || order.getStatus() != OrderStatus.DISPATCHING) {
            log.info("订单不在派单中状态，忽略重试 orderId={} status={}",
                    orderId, order != null ? order.getStatus() : "null");
            return;
        }

        // 4. version=-1 是特殊值，表示候选列表已耗尽且当前半径无新司机，直接走扩圈逻辑
        // 跳过幂等校验，因为此时 Redis 中没有有效的版本号可供比较
        if (msgVersion == -1) {
            handleExhausted(orderId, order);
            return;
        }

        // 5. 幂等校验：比较消息中的版本号与 Redis 当前版本号
        // 版本号不一致说明已有新一轮推送（拒单重推或 RetryConsumer 重推），此消息是过期的超时通知
        String versionKey = "order:dispatch:version:" + orderId;
        String currentVersionStr = redisTemplate.opsForValue().get(versionKey);
        if (currentVersionStr == null) {
            log.info("版本号 key 不存在，忽略重试 orderId={}", orderId);
            return;
        }
        int currentVersion = Integer.parseInt(currentVersionStr);
        if (msgVersion != currentVersion) {
            log.info("版本号不一致，忽略过期重试 orderId={} msgVersion={} currentVersion={}",
                    orderId, msgVersion, currentVersion);
            return;
        }

        // 6. 读候选列表，取下一个未被推送过的候选司机
        String candidatesKey = "order:candidates:" + orderId;
        String candidatesJson = redisTemplate.opsForValue().get(candidatesKey);
        if (candidatesJson == null) {
            log.warn("候选列表已过期，直接取消 orderId={}", orderId);
            sendCancelMessage(orderId, "派单异常，候选列表已过期");
            return;
        }

        List<Long> candidates;
        try {
            candidates = objectMapper.readValue(candidatesJson, new TypeReference<>() {});
        } catch (Exception e) {
            log.error("候选列表解析失败 orderId={}", orderId, e);
            sendCancelMessage(orderId, "派单异常");
            return;
        }

        // 读取已推送过的司机集合，跳过已推送的候选
        Set<String> alreadyDispatched = redisTemplate.opsForSet()
                .members("order:dispatched:drivers:" + orderId);

        Long nextDriverId = null;
        int nextIndex = -1;
        for (int i = 0; i < candidates.size(); i++) {
            Long candidateId = candidates.get(i);
            boolean alreadySent = alreadyDispatched != null
                    && alreadyDispatched.contains(String.valueOf(candidateId));
            if (!alreadySent) {
                nextDriverId = candidateId;
                nextIndex = i;
                break;
            }
        }

        if (nextDriverId == null) {
            // 候选列表耗尽，重新 GEO 召回
            log.info("候选列表耗尽，触发重新召回 orderId={} totalCandidates={}", orderId, candidates.size());
            handleExhausted(orderId, order);
            return;
        }

        // 7. 候选列表还有剩余，推下一个候选司机
        // 先原子递增版本号，使当前已发出的延迟消息（若有并发）失效
        // 再调用公共推送方法（OrderService.pushNextDriver），该方法会发新的延迟消息携带新版本号
        Long newVersion = redisTemplate.opsForValue().increment(versionKey);
        if (newVersion != null && newVersion == 1) {
            // key 不存在时 INCR 从 0 开始，首次创建时设置 TTL（正常不会走到这里，防御性兜底）
            redisTemplate.expire(versionKey, Duration.ofMinutes(10));
        }
        int version = newVersion != null ? newVersion.intValue() : currentVersion + 1;

        orderService.pushNextDriver(orderId, nextDriverId, nextIndex, version);
        log.info("超时重试推送下一候选司机 orderId={} driverId={} index={} version={}",
                orderId, nextDriverId, nextIndex, version);
    }

    /**
     * 候选列表耗尽后的处理逻辑
     *
     * 先在当前半径重新 GEO 召回（过滤已派过的司机），有新司机则继续派单；
     * 无新司机则按步进表扩圈，扩圈后仍无司机则发 version=-1 的延迟消息等待下一轮；
     * 已达最大半径且无司机则取消订单。
     *
     * 扩圈半径存储在 Redis（order:dispatch:radius:{orderId}），避免消息体携带状态。
     * 已派过司机通过 order:dispatched:drivers:{orderId} Set 过滤，防止重复推送。
     *
     * @param orderId 订单 ID
     * @param order   订单实体（含下单坐标）
     */
    private void handleExhausted(Long orderId, Order order) {
        // 读当前搜索半径（由 GlobalDispatchScheduler 初始写入，扩圈时更新）
        String radiusKey = "order:dispatch:radius:" + orderId;
        String radiusStr = redisTemplate.opsForValue().get(radiusKey);
        double currentRadius = radiusStr != null ? Double.parseDouble(radiusStr) : 5.0;

        double originLat = order.getOriginLat().doubleValue();
        double originLng = order.getOriginLng().doubleValue();
        String city = "default";

        // 读已派过的司机集合，用于过滤重复推送
        String dispatchedKey = "order:dispatched:drivers:" + orderId;
        Set<String> alreadyDispatched = redisTemplate.opsForSet().members(dispatchedKey);

        // 先在当前半径重新召回，过滤已派过的司机
        List<Long> newDrivers = recallFiltered(originLat, originLng, currentRadius, city, alreadyDispatched);

        if (!newDrivers.isEmpty()) {
            // 当前半径内有新司机，重置候选列表并推第一个
            log.info("重新召回发现新司机 orderId={} radius={}km count={}", orderId, currentRadius, newDrivers.size());
            dispatchNewCandidates(orderId, newDrivers, currentRadius);
            return;
        }

        // 当前半径无新司机，尝试扩圈
        double nextRadius = nextRadius(currentRadius);
        if (nextRadius <= currentRadius + 0.01) {
            // 已达最大半径（15km），无司机可派，取消订单
            log.info("已达最大搜索半径（{}km）且无新司机，取消订单 orderId={}", currentRadius, orderId);
            saveDispatchLog(orderId, null, DispatchAction.TIMEOUT, "扩圈至最大半径仍无司机");
            sendCancelMessage(orderId, "附近暂无可用司机");
            return;
        }

        // 更新搜索半径（无论扩圈后是否找到司机，都记录本轮已扩到的半径，避免下次重复扩同一档）
        redisTemplate.opsForValue().set(radiusKey, String.valueOf(nextRadius), Duration.ofMinutes(10));

        // 扩圈后重新召回，过滤已派过的司机
        List<Long> expandedDrivers = recallFiltered(originLat, originLng, nextRadius, city, alreadyDispatched);

        if (!expandedDrivers.isEmpty()) {
            log.info("扩圈至 {}km 发现新司机，开始派单 orderId={} count={}", nextRadius, orderId, expandedDrivers.size());
            dispatchNewCandidates(orderId, expandedDrivers, nextRadius);
        } else {
            // 扩圈后仍无司机，发 version=-1 延迟消息，等待下一轮再次尝试
            // 下一轮收到后直接再次走 handleExhausted，继续扩圈或等待新司机进入范围
            log.info("扩圈至 {}km 仍无司机，等待下一轮 orderId={}", nextRadius, orderId);
            sendDelayMessage(orderId, -1);
        }
    }

    /**
     * GEO 召回并过滤已派过的司机，同时校验司机状态
     *
     * 扩圈后，之前在小半径内被推送但未接单的司机可能再次出现在搜索结果中。
     * 通过过滤 order:dispatched:drivers:{orderId} Set，避免对同一司机重复推送同一订单。
     *
     * 状态过滤说明：
     * GEO 集合中的司机可能因状态变更（接单、下线、封禁）而不再可用，
     * 但 GEO 集合不会实时同步这些变更（仅在司机主动下线或 FakeOnlineCleanJob 清理时移除）。
     * 因此必须查库二次校验，只保留 ONLINE + APPROVED 的司机，与 GlobalDispatchScheduler 保持一致。
     *
     * @param alreadyDispatched 已派过的司机 ID 字符串集合（可为 null）
     * @return 过滤后的新司机 ID 列表
     */
    private List<Long> recallFiltered(double lat, double lng, double radiusKm,
                                       String city, Set<String> alreadyDispatched) {
        List<Long> nearby = locationService.nearbyDrivers(lat, lng, radiusKm, city, MAX_CANDIDATES);
        if (nearby.isEmpty()) return nearby;

        // 查库过滤状态异常的司机：GEO 集合不实时同步状态变更，必须二次校验
        // 只保留 ONLINE + APPROVED 的司机，防止将订单推送给行程中/封禁/未审核的司机
        List<Driver> validDrivers = driverMapper.selectList(
                new LambdaQueryWrapper<Driver>()
                        .in(Driver::getId, nearby)
                        .eq(Driver::getStatus, DriverStatus.ONLINE)
                        .eq(Driver::getAuditStatus, AuditStatus.APPROVED));
        Set<Long> validIds = validDrivers.stream()
                .map(Driver::getId)
                .collect(Collectors.toSet());

        return nearby.stream()
                .filter(validIds::contains)
                .filter(id -> alreadyDispatched == null || !alreadyDispatched.contains(String.valueOf(id)))
                .collect(Collectors.toList());
    }

    /**
     * 用新的候选列表重置派单状态并推第一个候选司机
     *
     * 更新 order:candidates，重置版本号为 0，推第一个候选司机，发延迟消息。
     * 供 handleExhausted 在找到新司机后调用。
     *
     * 版本号重置为 0 的原因：
     * 新候选列表是全新一轮派单的起点，版本号从 0 开始，
     * 后续 pushNextDriver 会将版本号递增为 1 并写入延迟消息，
     * 确保新一轮的幂等校验链路正确。
     *
     * @param orderId    订单 ID
     * @param candidates 过滤后的新候选司机列表
     * @param radius     本次召回使用的搜索半径（仅用于日志）
     */
    private void dispatchNewCandidates(Long orderId, List<Long> candidates, double radius) {
        String candidatesKey = "order:candidates:" + orderId;
        try {
            redisTemplate.opsForValue().set(candidatesKey,
                    objectMapper.writeValueAsString(candidates), Duration.ofMinutes(10));
        } catch (Exception e) {
            log.error("候选列表序列化失败 orderId={}", orderId, e);
            sendCancelMessage(orderId, "派单异常");
            return;
        }

        // 重置版本号为 0（新一轮候选列表从头开始）
        // pushNextDriver 会将版本号递增为 1 并写入延迟消息，确保幂等校验链路正确
        String versionKey = "order:dispatch:version:" + orderId;
        redisTemplate.opsForValue().set(versionKey, "0", Duration.ofMinutes(10));

        // 取第一个候选司机推送
        Long firstDriverId = candidates.get(0);

        // 原子递增版本号（0 → 1），pushNextDriver 发出的延迟消息携带版本号 1
        Long newVersion = redisTemplate.opsForValue().increment(versionKey);
        int version = newVersion != null ? newVersion.intValue() : 1;

        orderService.pushNextDriver(orderId, firstDriverId, 0, version);
        log.info("重新召回后推送第一个候选司机 orderId={} radius={}km driverId={}", orderId, radius, firstDriverId);
    }

    /**
     * 发送延迟消息（x-delay=10s）
     *
     * 消息体携带 version，用于 RetryConsumer 下次消费时的幂等校验。
     * 特殊值 version=-1 表示无候选司机，下次消费直接走 handleExhausted。
     */
    private void sendDelayMessage(Long orderId, int version) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("version", version);
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_RETRY,
                msg,
                m -> {
                    m.getMessageProperties().setHeader("x-delay", BATCH_DELAY_MS);
                    return m;
                }
        );
    }

    /** 发送取消消息 */
    private void sendCancelMessage(Long orderId, String reason) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("reason", reason);
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_CANCEL,
                msg
        );
    }

    /** 记录派单日志 */
    private void saveDispatchLog(Long orderId, Long driverId, DispatchAction action, String remark) {
        OrderDispatchLog log = new OrderDispatchLog();
        log.setOrderId(orderId);
        log.setDriverId(driverId);
        log.setAction(action);
        log.setRemark(remark);
        log.setCreatedAt(LocalDateTime.now());
        dispatchLogMapper.insert(log);
    }

    /**
     * 根据当前半径返回下一档扩圈半径
     *
     * 按 RADIUS_STEPS 步进表依次扩大，若已达最大档则返回 MAX_RADIUS_KM 不再扩大。
     * +0.01 容差用于规避浮点比较误差（如 5.0000000001 > 5.0 的情况）。
     */
    private double nextRadius(double currentRadius) {
        for (double step : RADIUS_STEPS) {
            if (step > currentRadius + 0.01) return step;
        }
        return MAX_RADIUS_KM;
    }
}