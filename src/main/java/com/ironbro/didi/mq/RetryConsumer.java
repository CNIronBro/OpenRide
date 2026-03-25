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
import java.util.concurrent.TimeUnit;

/**
 * 派单超时重试消费者
 *
 * 消费 dispatch.retry.queue 中的超时重试消息（由 dispatch.exchange x-delay=15s 延迟到期后路由而来）。
 *
 * 核心逻辑：
 * 1. 幂等校验：比较消息中的 dispatchIndex 与 Redis 中当前索引是否一致
 *    - 不一致：说明已有新一轮派单（司机接单或已重试），忽略此消息（ACK 即可）
 *    - 一致：继续处理
 * 2. 订单状态校验：若订单已不在 DISPATCHING 状态（已接单/已取消），忽略
 * 3. 取下一位候选司机（index+1），推送并更新 Redis 索引，发新延迟消息
 * 4. 候选列表耗尽（index 越界）：发消息到 cancel.queue，触发自动取消
 *
 * 幂等设计说明：
 * dispatchIndex 是关键的幂等控制字段。每次派单时，Redis 中存储当前索引值。
 * 延迟消息携带发送时的 dispatchIndex，15s 后到期时：
 * - 若司机已接单，订单状态已变更，步骤 2 会过滤
 * - 若已进行了新一轮重试（index 已更新），步骤 1 会过滤
 * 两层保护确保同一轮派单的超时消息不会触发重复重试。
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

    /** 无司机时最大等待时间（秒），超过后才真正取消订单 */
    private static final int MAX_NO_DRIVER_WAIT_SECONDS = 60;

    /** GEO 召回半径（与 DispatchConsumer 保持一致） */
    private static final double DISPATCH_RADIUS_KM = 5.0;

    /** 最多召回候选司机数量（与 DispatchConsumer 保持一致） */
    private static final int MAX_CANDIDATES = 10;

    /**
     * 消费超时重试消息
     *
     * 消息体格式（Map）：
     * {
     *   "orderId":       Long,
     *   "dispatchIndex": Integer  // 发送此延迟消息时的派单索引
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
        int msgDispatchIndex = ((Number) body.get("dispatchIndex")).intValue();

        // 7.2 补偿幂等锁：防止同一超时消息被多个消费者实例并发处理
        // 场景：多实例部署时 MQ 消息被重复投递，或网络抖动导致 ACK 未到达 Broker 后重投
        // tryLock(waitTime=0)：不等待，立即返回。若锁已被持有，说明另一实例正在处理，直接 ACK 跳过
        // leaseTime=30s：足够覆盖一次完整重试流程，超时自动释放防死锁
        // 注意：此锁与 dispatchIndex 幂等校验是两层独立保护，互为补充
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
            doRetry(orderId, msgDispatchIndex, body);
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
     * 两种消息类型：
     * 1. noDriverRetry=true：无司机等待重试，重新 GEO 召回，超过 60s 才取消
     * 2. 普通重试：候选司机未接单，轮换下一位候选司机
     *
     * @param orderId          订单 ID
     * @param msgDispatchIndex 消息中携带的派单索引（无司机重试时为 -1）
     * @param body             完整消息体（用于读取 noDriverRetry、waitedSeconds 等字段）
     */
    private void doRetry(Long orderId, int msgDispatchIndex, Map<String, Object> body) {
        // 无司机等待重试分支：重新 GEO 召回，超过最大等待时间才取消
        boolean noDriverRetry = Boolean.TRUE.equals(body.get("noDriverRetry"));
        if (noDriverRetry) {
            handleNoDriverRetry(orderId, body);
            return;
        }

        // 5.5.1 幂等校验：比较消息中的 dispatchIndex 与 Redis 当前索引
        String indexKey = "order:dispatch:index:" + orderId;
        String currentIndexStr = redisTemplate.opsForValue().get(indexKey);

        if (currentIndexStr == null) {
            // Redis 中无索引记录，说明候选列表已过期或订单已结束，忽略
            log.info("派单索引 key 不存在，忽略重试 orderId={}", orderId);
            return;
        }

        int currentIndex = Integer.parseInt(currentIndexStr);
        if (msgDispatchIndex != currentIndex) {
            // 索引不一致：说明已有新一轮派单（index 已更新），此消息是过期的超时通知，忽略
            // 这是幂等保证的核心：同一轮派单只处理一次超时
            log.info("dispatchIndex 不一致，忽略过期重试 orderId={} msgIndex={} currentIndex={}",
                    orderId, msgDispatchIndex, currentIndex);
            return;
        }

        // 5.5.2 订单状态校验
        Order order = orderMapper.selectById(orderId);
        if (order == null || order.getStatus() != OrderStatus.DISPATCHING) {
            log.info("订单不在派单中状态，忽略重试 orderId={} status={}",
                    orderId, order != null ? order.getStatus() : "null");
            return;
        }

        // 5.5.3 从 Redis 候选列表取下一位司机
        String candidatesKey = "order:candidates:" + orderId;
        String candidatesJson = redisTemplate.opsForValue().get(candidatesKey);

        if (candidatesJson == null) {
            // 候选列表已过期，直接取消
            log.warn("候选列表已过期，直接取消 orderId={}", orderId);
            sendCancelMessage(orderId, "派单超时，候选列表已过期");
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

        int nextIndex = currentIndex + 1;

        // 5.5.4 候选列表耗尽，发消息到 cancel.queue
        if (nextIndex >= candidates.size()) {
            log.info("候选列表耗尽，触发自动取消 orderId={} totalCandidates={}", orderId, candidates.size());
            saveDispatchLog(orderId, null, DispatchAction.TIMEOUT, "候选列表耗尽，自动取消");
            sendCancelMessage(orderId, "附近司机均未接单");
            return;
        }

        // 取下一位候选司机
        Long nextDriverId = candidates.get(nextIndex);

        // 更新 Redis 中的当前派单索引
        redisTemplate.opsForValue().set(indexKey, String.valueOf(nextIndex),
                Duration.ofMinutes(10));

        // 推送新订单通知给下一位司机
        pushOrderToDriver(orderId, nextDriverId);

        // 更新司机派单统计
        updateDriverDispatchStats(nextDriverId);

        // 记录派单日志
        saveDispatchLog(orderId, nextDriverId, DispatchAction.DISPATCHED,
                "重试派单，index=" + nextIndex);

        // 发送新的延迟消息（携带新的 dispatchIndex）
        sendDelayMessage(orderId, nextIndex);

        log.info("重试派单成功 orderId={} nextDriverId={} index={}", orderId, nextDriverId, nextIndex);
    }

    /**
     * 无司机等待重试处理
     *
     * 每 15s 重新 GEO 召回一次，累计等待超过 MAX_NO_DRIVER_WAIT_SECONDS（60s）才取消订单。
     * 若召回到司机，立即走正常派单流程（存候选列表、推送、发普通延迟消息）。
     *
     * @param orderId 订单 ID
     * @param body    消息体，含 waitedSeconds、originLat、originLng、city 等字段
     */
    private void handleNoDriverRetry(Long orderId, Map<String, Object> body) {
        // 订单状态校验，防止订单已被取消或接单
        Order order = orderMapper.selectById(orderId);
        if (order == null || order.getStatus() != OrderStatus.DISPATCHING) {
            log.info("无司机重试：订单不在派单中状态，忽略 orderId={}", orderId);
            return;
        }

        int waitedSeconds = body.containsKey("waitedSeconds")
                ? ((Number) body.get("waitedSeconds")).intValue() : 0;

        // 超过最大等待时间，取消订单
        if (waitedSeconds >= MAX_NO_DRIVER_WAIT_SECONDS) {
            log.info("无司机等待超时，取消订单 orderId={} waitedSeconds={}", orderId, waitedSeconds);
            sendCancelMessage(orderId, "附近暂无可用司机");
            return;
        }

        // 重新 GEO 召回，坐标从订单实体读取（无需消息体携带）
        double originLat = order.getOriginLat().doubleValue();
        double originLng = order.getOriginLng().doubleValue();
        String city = "default";

        List<Long> nearbyDriverIds = locationService.nearbyDrivers(
                originLat, originLng, DISPATCH_RADIUS_KM, city, MAX_CANDIDATES);

        int nextWaited = waitedSeconds + 15;

        if (nearbyDriverIds.isEmpty()) {
            // 仍无司机，继续等待
            log.info("无司机重试：仍无司机，继续等待 orderId={} waitedSeconds={}", orderId, nextWaited);
            sendNoDriverDelayMessage(orderId, nextWaited);
            return;
        }

        // 有司机了，走正常派单流程：存候选列表、推送第一位、发普通延迟消息
        log.info("无司机重试：发现司机，开始正常派单 orderId={} waitedSeconds={}", orderId, nextWaited);

        String candidatesKey = "order:candidates:" + orderId;
        String indexKey = "order:dispatch:index:" + orderId;

        String candidatesJson;
        try {
            candidatesJson = objectMapper.writeValueAsString(nearbyDriverIds);
        } catch (Exception e) {
            log.error("候选列表序列化失败 orderId={}", orderId, e);
            sendCancelMessage(orderId, "派单异常");
            return;
        }

        redisTemplate.opsForValue().set(candidatesKey, candidatesJson, Duration.ofMinutes(10));
        redisTemplate.opsForValue().set(indexKey, "0", Duration.ofMinutes(10));

        Long targetDriverId = nearbyDriverIds.get(0);
        pushOrderToDriver(orderId, targetDriverId);
        updateDriverDispatchStats(targetDriverId);
        saveDispatchLog(orderId, targetDriverId, DispatchAction.DISPATCHED,
                "无司机等待后找到司机，开始派单 waitedSeconds=" + nextWaited);
        sendDelayMessage(orderId, 0);
    }

    /**
     * 发送无司机等待延迟消息
     *
     * 通过 x-delay header 实现 15s 延迟，15s 后由 RetryConsumer 重新 GEO 召回。
     */
    private void sendNoDriverDelayMessage(Long orderId, int waitedSeconds) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("dispatchIndex", -1);   // 无司机重试不使用 dispatchIndex，填 -1 占位
        msg.put("noDriverRetry", true);
        msg.put("waitedSeconds", waitedSeconds);
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_RETRY,
                msg,
                m -> {
                    m.getMessageProperties().setHeader("x-delay", 15_000);
                    return m;
                }
        );
    }

    /** 推送新订单通知给司机（写 Redis，司机端轮询）
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
        redisTemplate.opsForValue().set(key, String.valueOf(orderId), Duration.ofSeconds(20));
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

    /** 发送延迟消息（x-delay=15s） */
    private void sendDelayMessage(Long orderId, int dispatchIndex) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("dispatchIndex", dispatchIndex);
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_RETRY,
                msg,
                m -> {
                    m.getMessageProperties().setHeader("x-delay", 15_000);
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
}