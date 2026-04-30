package com.ironbro.didi.mq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ironbro.didi.config.RabbitMqConfig;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.OrderDispatchLog;
import com.ironbro.didi.enums.DispatchAction;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.service.DriverLocationService;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.websocket.WebSocketSessionManager;
import com.ironbro.didi.websocket.WsMessage;
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
 * 3. 特殊值 batchIndex=-1：跳过幂等校验，直接走 handleExhausted（候选耗尽/无司机扩圈逻辑）
 * 4. 幂等校验：msgBatchIndex != currentBatchIndex → 忽略（过期消息）
 * 5. 候选列表还有剩余 → 推下一批（切片），更新 batchIndex，发新延迟消息
 * 6. 候选列表耗尽 → handleExhausted：重新 GEO 召回 + 扩圈
 *
 * 幂等设计说明：
 * batchIndex 是关键的幂等控制字段。每批派单时，Redis 中存储当前批次号。
 * 延迟消息携带发送时的 batchIndex，10s 后到期时：
 * - 若司机已接单，订单状态已变更，步骤 2 会过滤
 * - 若已进行了新一轮批次（batchIndex 已更新），步骤 4 会过滤
 * 两层保护确保同一批次的超时消息不会触发重复重试。
 *
 * 补偿幂等锁（阶段 7）：
 * 消费前用 Redisson tryLock（key=lock:compensate:{orderId}），防止同一超时消息
 * 被多个消费者实例并发处理（多实例部署或 MQ 重投场景）。
 * leaseTime=30s，超时自动释放防死锁。
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
    private final WebSocketSessionManager wsSessionManager;

    /** 每批同时推送的司机数量（与 DispatchConsumer 保持一致） */
    private static final int BATCH_SIZE = 3;

    /**
     * 每批派单窗口时长（毫秒）
     * 与 DispatchConsumer 保持一致，10s 后统一判断是否推下一批
     */
    private static final int BATCH_DELAY_MS = 10_000;

    /** driver:pending:order TTL（秒），略大于批次窗口 */
    private static final int PENDING_TTL_SECONDS = 12;

    /**
     * 从下单时刻起算的最大等待时间（秒）
     * 超过此值无论处于哪个阶段（正常轮转/扩圈/等待新司机）均取消订单
     */
    private static final int MAX_WAIT_SECONDS = 120;

    /** 最多召回候选司机数量（与 DispatchConsumer 保持一致） */
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
     *   "orderId":    Long,
     *   "batchIndex": Integer  // 发送此延迟消息时的批次号，-1 表示无候选司机等待扩圈
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
        Object batchIdxRaw = body.get("batchIndex");
        if (batchIdxRaw == null) {
            // 消息格式不合法（缺少 batchIndex 字段），NACK 进死信
            log.error("重试消息缺少 batchIndex 字段，NACK 进死信 orderId={}", orderId);
            channel.basicNack(deliveryTag, false, false);
            return;
        }
        int msgBatchIndex = ((Number) batchIdxRaw).intValue();

        // 7.2 补偿幂等锁：防止同一超时消息被多个消费者实例并发处理
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
            doRetry(orderId, msgBatchIndex);
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
     * 核心重试逻辑（统一入口）
     *
     * 处理顺序：
     * 1. 全局超时检查（从下单时刻起算 120s）
     * 2. 订单状态校验
     * 3. batchIndex=-1 特殊值处理（直接走候选耗尽逻辑）
     * 4. 幂等校验（比较 msgBatchIndex 与 Redis 当前批次号）
     * 5. 候选列表切片推下一批，或候选耗尽触发 handleExhausted
     *
     * @param orderId       订单 ID
     * @param msgBatchIndex 消息中携带的批次号（-1 表示无候选司机等待扩圈）
     */
    private void doRetry(Long orderId, int msgBatchIndex) {
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

        // 4. batchIndex=-1 是特殊值，表示候选列表已耗尽且当前半径无新司机，直接走扩圈逻辑
        // 跳过幂等校验，因为此时 Redis 中没有有效的 batchIndex 可供比较
        if (msgBatchIndex == -1) {
            handleExhausted(orderId, order);
            return;
        }

        // 5. 幂等校验：比较消息中的 batchIndex 与 Redis 当前批次号
        String batchIndexKey = "order:dispatch:batch:index:" + orderId;
        String currentBatchStr = redisTemplate.opsForValue().get(batchIndexKey);
        if (currentBatchStr == null) {
            log.info("批次索引 key 不存在，忽略重试 orderId={}", orderId);
            return;
        }
        int currentBatchIndex = Integer.parseInt(currentBatchStr);
        if (msgBatchIndex != currentBatchIndex) {
            // 批次号不一致：说明已有新一轮批次在处理（司机接单后 batchIndex 不变，但订单状态已变，
            // 或已推了下一批），此消息是过期的超时通知，忽略
            log.info("batchIndex 不一致，忽略过期重试 orderId={} msgBatch={} currentBatch={}",
                    orderId, msgBatchIndex, currentBatchIndex);
            return;
        }

        // 6. 读候选列表，判断是否还有剩余
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

        int nextBatchIndex = currentBatchIndex + 1;
        int fromIdx = nextBatchIndex * BATCH_SIZE;

        if (fromIdx >= candidates.size()) {
            // 候选列表耗尽，重新 GEO 召回
            log.info("候选列表耗尽，触发重新召回 orderId={} totalCandidates={}", orderId, candidates.size());
            handleExhausted(orderId, order);
            return;
        }

        // 7. 候选列表还有剩余，推下一批
        int toIdx = Math.min(fromIdx + BATCH_SIZE, candidates.size());
        List<Long> nextBatch = candidates.subList(fromIdx, toIdx);

        // 推送下一批前，先通知乘客"正在重新匹配司机"，改善等待体验
        // passengerId 即乘客的 user_id，与 WS session 的 userId 一致
        try {
            wsSessionManager.sendToUser(order.getPassengerId(),
                    new WsMessage("DISPATCH_RETRYING", Map.of("orderId", orderId)));
        } catch (Exception e) {
            log.warn("WS 推送重试通知失败 orderId={}", orderId, e);
        }

        // 写新批次 SET：key=order:dispatch:batch:{orderId}:{nextBatchIndex}，TTL=PENDING_TTL_SECONDS
        String batchSetKey = "order:dispatch:batch:" + orderId + ":" + nextBatchIndex;
        String[] members = nextBatch.stream().map(String::valueOf).toArray(String[]::new);
        redisTemplate.opsForSet().add(batchSetKey, members);
        redisTemplate.expire(batchSetKey, Duration.ofSeconds(PENDING_TTL_SECONDS));

        // 更新批次号
        redisTemplate.opsForValue().set(batchIndexKey, String.valueOf(nextBatchIndex),
                Duration.ofMinutes(10));

        // 批量推送
        for (Long driverId : nextBatch) {
            pushOrderToDriver(orderId, driverId);
            updateDriverDispatchStats(driverId);
            saveDispatchLog(orderId, driverId, DispatchAction.DISPATCHED,
                    "重试批量派单 batch=" + nextBatchIndex);
        }

        sendDelayMessage(orderId, nextBatchIndex);
        log.info("重试批量派单成功 orderId={} batch={} drivers={}", orderId, nextBatchIndex, nextBatch);
    }

    /**
     * 候选列表耗尽后的处理逻辑
     *
     * 先在当前半径重新 GEO 召回（过滤已派过的司机），有新司机则继续派单；
     * 无新司机则按步进表扩圈，扩圈后仍无司机则发 batchIndex=-1 的延迟消息等待下一轮；
     * 已达最大半径且无司机则取消订单。
     *
     * 扩圈半径存储在 Redis（order:dispatch:radius:{orderId}），避免消息体携带状态。
     * 已派过司机通过 order:dispatched:drivers:{orderId} Set 过滤，防止重复推送。
     *
     * @param orderId 订单 ID
     * @param order   订单实体（含下单坐标）
     */
    private void handleExhausted(Long orderId, Order order) {
        // 读当前搜索半径（由 DispatchConsumer 初始写入，扩圈时更新）
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
            // 当前半径内有新司机，重置候选列表并推第一批
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
            // 扩圈后仍无司机，发 batchIndex=-1 延迟消息，等待下一轮再次尝试
            // 下一轮收到后直接再次走 handleExhausted，继续扩圈或等待新司机进入范围
            log.info("扩圈至 {}km 仍无司机，等待下一轮 orderId={}", nextRadius, orderId);
            sendDelayMessage(orderId, -1);
        }
    }

    /**
     * GEO 召回并过滤已派过的司机
     *
     * 扩圈后，之前在小半径内被推送但未接单的司机可能再次出现在搜索结果中。
     * 通过过滤 order:dispatched:drivers:{orderId} Set，避免对同一司机重复推送同一订单。
     *
     * @param alreadyDispatched 已派过的司机 ID 字符串集合（可为 null）
     * @return 过滤后的新司机 ID 列表
     */
    private List<Long> recallFiltered(double lat, double lng, double radiusKm,
                                       String city, Set<String> alreadyDispatched) {
        List<Long> nearby = locationService.nearbyDrivers(lat, lng, radiusKm, city, MAX_CANDIDATES);
        if (nearby.isEmpty()) return nearby;
        return nearby.stream()
                .filter(id -> alreadyDispatched == null || !alreadyDispatched.contains(String.valueOf(id)))
                .collect(Collectors.toList());
    }

    /**
     * 用新的候选列表重置派单状态并推第一批
     *
     * 更新 order:candidates，重置 batchIndex=0，写批次 SET，推第一批，发延迟消息。
     * 供 handleExhausted 在找到新司机后调用。
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

        // 重置批次号为 0（新一轮候选列表从头开始）
        String batchIndexKey = "order:dispatch:batch:index:" + orderId;
        redisTemplate.opsForValue().set(batchIndexKey, "0", Duration.ofMinutes(10));

        // 写批次 SET
        int batchEnd = Math.min(BATCH_SIZE, candidates.size());
        List<Long> firstBatch = candidates.subList(0, batchEnd);
        String batchSetKey = "order:dispatch:batch:" + orderId + ":0";
        String[] members = firstBatch.stream().map(String::valueOf).toArray(String[]::new);
        redisTemplate.opsForSet().add(batchSetKey, members);
        redisTemplate.expire(batchSetKey, Duration.ofSeconds(PENDING_TTL_SECONDS));

        // 批量推送
        for (Long driverId : firstBatch) {
            pushOrderToDriver(orderId, driverId);
            updateDriverDispatchStats(driverId);
            saveDispatchLog(orderId, driverId, DispatchAction.DISPATCHED,
                    "重新召回后派单 radius=" + radius + "km batch=0");
        }

        sendDelayMessage(orderId, 0);
        log.info("重新召回后批量派单 orderId={} radius={}km drivers={}", orderId, radius, firstBatch);
    }

    /**
     * 推送新订单通知给司机（写 Redis，司机端轮询）
     *
     * 重复推送防护：SADD 原子操作检查司机是否已被推送过此订单，
     * 防止重试链路中同一司机被重复推送（如候选列表循环或消息重投场景）。
     */
    private void pushOrderToDriver(Long orderId, Long driverId) {
        // 7.3 重复推送防护：与 DispatchConsumer 共用同一 Redis Set
        String dispatchedKey = "order:dispatched:drivers:" + orderId;
        Long added = redisTemplate.opsForSet().add(dispatchedKey, String.valueOf(driverId));
        redisTemplate.expire(dispatchedKey, Duration.ofMinutes(10));

        if (added == null || added == 0) {
            log.info("司机已被推送过此订单（重试链路），跳过 driverId={} orderId={}", driverId, orderId);
            return;
        }

        String key = "driver:pending:order:" + driverId;
        // TTL=PENDING_TTL_SECONDS（12s），略大于 10s 批次窗口
        redisTemplate.opsForValue().set(key, String.valueOf(orderId), Duration.ofSeconds(PENDING_TTL_SECONDS));

        // WS 推送派单通知（Redis key 保留作为兜底，WS 失败时司机端轮询仍可感知）
        // driverId 是 driver.id（司机表主键），WS session 以 userId（user 表主键）索引，需转换
        try {
            var driver = driverMapper.selectById(driverId);
            if (driver != null) {
                wsSessionManager.sendToUser(driver.getUserId(),
                        new WsMessage("DISPATCH_NOTIFY", Map.of("orderId", orderId)));
            }
        } catch (Exception e) {
            log.warn("WS 推送派单通知失败（重试链路），依赖司机端轮询兜底 driverId={} orderId={}", driverId, orderId, e);
        }
    }

    /** 更新司机派单统计 */
    private void updateDriverDispatchStats(Long driverId) {
        var driver = driverMapper.selectById(driverId);
        if (driver != null) {
            driver.setLastDispatchAt(LocalDateTime.now());
            driver.setDispatchCountToday(
                    driver.getDispatchCountToday() != null ? driver.getDispatchCountToday() + 1 : 1);
            driverMapper.updateById(driver);
        }
    }

    /**
     * 发送延迟消息（x-delay=10s）
     *
     * 消息体携带 batchIndex，用于 RetryConsumer 下次消费时的幂等校验。
     * 特殊值 batchIndex=-1 表示无候选司机，下次消费直接走 handleExhausted。
     */
    private void sendDelayMessage(Long orderId, int batchIndex) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("batchIndex", batchIndex);
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