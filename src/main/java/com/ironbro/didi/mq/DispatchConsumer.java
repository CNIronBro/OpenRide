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
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.service.DispatchScoreService;
import com.ironbro.didi.service.DriverLocationService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 派单消费者
 *
 * 消费 dispatch.queue 中的新订单派单消息，执行：
 * 1. GEO 召回附近在线司机候选列表
 * 2. 调度评分排序，选出最优司机
 * 3. 将候选列表序列化存入 Redis（key=order:candidates:{orderId}，TTL=10min）
 * 4. 将当前派单索引（0）存入 Redis（key=order:dispatch:index:{orderId}）
 * 5. 推送新订单通知给最优司机（写 Redis，司机端轮询）
 * 6. 发送延迟消息到 dispatch.delay.queue（TTL=15s），消息体携带 orderId + dispatchIndex=0
 *
 * 幂等说明（阶段 7 实现）：
 * 消费前用 Redisson tryLock（key=lock:dispatch:{orderId}），防止同一订单的派单消息
 * 被多个消费者实例并发消费两次（MQ 消息重投或多实例部署场景）。
 * leaseTime=30s，足够覆盖一次完整派单流程，超时自动释放防止死锁。
 *
 * 重复推送防护（阶段 7 实现）：
 * 推送前检查 Redis Set（key=order:dispatched:drivers:{orderId}），
 * 若司机已在集合中则跳过，防止同一司机被重复推送同一订单。
 *
 * 消费失败处理：
 * 捕获异常后 NACK（requeue=false），消息路由到 dispatch.dlx → dispatch.dlq，
 * 避免消息无限重入队列导致死循环。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DispatchConsumer {

    /** GEO 召回半径（公里） */
    private static final double DISPATCH_RADIUS_KM = 5.0;

    /** 最多召回候选司机数量 */
    private static final int MAX_CANDIDATES = 10;

    /** 候选列表在 Redis 中的 TTL（分钟） */
    private static final long CANDIDATES_TTL_MINUTES = 10;

    private final OrderMapper orderMapper;
    private final DriverMapper driverMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;
    private final DriverLocationService locationService;
    private final DispatchScoreService scoreService;
    private final RabbitTemplate rabbitTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;

    /**
     * 消费派单消息
     *
     * 消息体格式（Map）：
     * {
     *   "orderId":   Long,
     *   "originLat": BigDecimal,
     *   "originLng": BigDecimal,
     *   "city":      String
     * }
     *
     * @param message RabbitMQ 原始消息（用于手动 ACK）
     * @param channel RabbitMQ Channel（用于手动 ACK/NACK）
     */
    @RabbitListener(queues = RabbitMqConfig.DISPATCH_QUEUE)
    public void onDispatch(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Map<String, Object> body;
        try {
            body = objectMapper.readValue(message.getBody(), new TypeReference<>() {});
        } catch (Exception e) {
            log.error("派单消息解析失败，NACK 进死信", e);
            // requeue=false：不重入队列，直接进死信队列
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        Long orderId = ((Number) body.get("orderId")).longValue();
        double originLat = ((Number) body.get("originLat")).doubleValue();
        double originLng = ((Number) body.get("originLng")).doubleValue();
        String city = (String) body.getOrDefault("city", "default");

        // 7.1 派单幂等锁：防止同一订单的派单消息被多个消费者实例并发消费两次
        // 场景：MQ 消息重投（网络抖动导致 ACK 未到达 Broker）或多实例部署时消息被重复投递
        // tryLock(waitTime=0)：不等待，立即返回。若锁已被持有，说明另一实例正在处理，直接 ACK 跳过
        // leaseTime=30s：足够覆盖一次完整派单流程（GEO 召回 + 评分 + Redis 写入 + MQ 发送），超时自动释放防死锁
        String lockKey = "lock:dispatch:" + orderId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!locked) {
                // 未获取到锁：说明另一实例正在处理此订单的派单，直接 ACK 跳过（幂等）
                log.info("派单幂等锁未获取，跳过重复消费 orderId={}", orderId);
                channel.basicAck(deliveryTag, false);
                return;
            }
            doDispatch(orderId, originLat, originLng, city);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("派单处理失败 orderId={}", orderId, e);
            // 消费失败 NACK，不重入队列，路由到死信队列
            channel.basicNack(deliveryTag, false, false);
        } finally {
            // 确保锁在持有时才释放，避免释放他人的锁
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 核心派单逻辑
     *
     * @param orderId   订单 ID
     * @param originLat 下单位置纬度
     * @param originLng 下单位置经度
     * @param city      城市标识
     */
    private void doDispatch(Long orderId, double originLat, double originLng, String city) {
        // 检查订单是否仍处于派单中状态（防止重复消费时订单已被取消或接单）
        Order order = orderMapper.selectById(orderId);
        if (order == null || order.getStatus() != OrderStatus.DISPATCHING) {
            log.info("订单不在派单中状态，跳过 orderId={} status={}", orderId,
                    order != null ? order.getStatus() : "null");
            return;
        }

        // 5.3.1 GEO 召回附近在线司机
        List<Long> nearbyDriverIds = locationService.nearbyDrivers(
                originLat, originLng, DISPATCH_RADIUS_KM, city, MAX_CANDIDATES);

        if (nearbyDriverIds.isEmpty()) {
            log.info("附近无在线司机，直接取消 orderId={}", orderId);
            sendCancelMessage(orderId, "附近无可用司机");
            return;
        }

        // 查询候选司机详情（含 acceptRate、idleSince、dispatchCountToday）
        List<Driver> drivers = driverMapper.selectList(
                new LambdaQueryWrapper<Driver>()
                        .in(Driver::getId, nearbyDriverIds)
                        .eq(Driver::getStatus, DriverStatus.ONLINE)
                        .eq(Driver::getAuditStatus, AuditStatus.APPROVED));

        if (drivers.isEmpty()) {
            log.info("候选司机均不可用，直接取消 orderId={}", orderId);
            sendCancelMessage(orderId, "附近无可用司机");
            return;
        }

        // 构建候选司机列表（含距离信息，用于评分）
        // 此处距离通过 GEO 位置重新计算，保证评分准确
        List<DispatchScoreService.CandidateDriver> candidates = buildCandidates(
                drivers, originLat, originLng, city);

        // 5.3.2 调度评分排序
        List<Long> sortedDriverIds = scoreService.score(candidates, DISPATCH_RADIUS_KM);

        // 5.3.3 将候选列表存入 Redis（TTL=10min）
        // key=order:candidates:{orderId}，value=JSON 数组（司机 ID 列表）
        String candidatesKey = "order:candidates:" + orderId;
        String candidatesJson = toJson(sortedDriverIds);
        redisTemplate.opsForValue().set(candidatesKey, candidatesJson,
                Duration.ofMinutes(CANDIDATES_TTL_MINUTES));

        // 5.3.3 将当前派单索引（0）存入 Redis
        // key=order:dispatch:index:{orderId}，value=当前正在派单的候选列表下标
        String indexKey = "order:dispatch:index:" + orderId;
        redisTemplate.opsForValue().set(indexKey, "0", Duration.ofMinutes(CANDIDATES_TTL_MINUTES));

        // 5.3.4 推送新订单通知给第一位司机（写 Redis，司机端轮询）
        Long targetDriverId = sortedDriverIds.get(0);
        pushOrderToDriver(orderId, targetDriverId);

        // 更新司机的最近派单时间和今日派单次数
        updateDriverDispatchStats(targetDriverId);

        // 记录派单日志
        saveDispatchLog(orderId, targetDriverId, DispatchAction.DISPATCHED, "初次派单，index=0");

        // 5.3.5 发送延迟消息（TTL=15s），消息体携带 orderId + dispatchIndex=0
        // 15s 后若司机未接单，RetryConsumer 将尝试派给下一位候选司机
        sendDelayMessage(orderId, 0);

        log.info("派单成功 orderId={} targetDriverId={} candidates={}", orderId, targetDriverId, sortedDriverIds.size());
    }

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
            // GEOPOS 获取司机在 GEO 集合中的坐标（经度在前，纬度在后）
            var positions = redisTemplate.opsForGeo().position(geoKey, String.valueOf(driver.getId()));
            double actualDistKm = DISPATCH_RADIUS_KM; // 默认最大距离（GEO 中无记录时的兜底值）
            if (positions != null && !positions.isEmpty() && positions.get(0) != null) {
                Point driverPos = positions.get(0);
                // Point.getX() = 经度，Point.getY() = 纬度（Redis GEO 存储顺序）
                actualDistKm = haversineKm(originLat, originLng, driverPos.getY(), driverPos.getX());
            }

            candidates.add(new DispatchScoreService.CandidateDriver(driver, actualDistKm));
        }

        return candidates;
    }

    /**
     * 推送新订单通知给司机
     *
     * 写入 Redis，司机端通过轮询 GET /driver/pending-order 读取。
     * key=driver:pending:order:{driverId}，value=orderId，TTL=20s（略大于 15s 派单超时）
     *
     * 重复推送防护（7.3）：
     * 推送前检查 Redis Set（key=order:dispatched:drivers:{orderId}），
     * 若司机已在集合中则跳过，防止同一司机被重复推送同一订单。
     * 使用 SADD 的原子性保证"检查+写入"不存在竞态。
     *
     * 设计意图：
     * 不使用 WebSocket 推送（阶段 6 约束），司机端每 2s 轮询一次此 key，
     * 有值则弹出新订单弹窗，15s 倒计时内接单或忽略。
     *
     * @return true=推送成功，false=该司机已被推送过（跳过）
     */
    private boolean pushOrderToDriver(Long orderId, Long driverId) {
        // 7.3 重复推送防护：SADD 返回 1 表示新增成功（未推送过），返回 0 表示已存在（已推送过）
        // SADD 是原子操作，天然防止并发下的重复写入，无需额外加锁
        String dispatchedKey = "order:dispatched:drivers:" + orderId;
        Long added = redisTemplate.opsForSet().add(dispatchedKey, String.valueOf(driverId));
        redisTemplate.expire(dispatchedKey, Duration.ofMinutes(10));

        if (added == null || added == 0) {
            // 该司机已被推送过此订单，跳过（防止重复弹窗）
            log.info("司机已被推送过此订单，跳过 driverId={} orderId={}", driverId, orderId);
            return false;
        }

        String key = "driver:pending:order:" + driverId;
        // TTL=20s，略大于 15s 派单超时，保证司机端有足够时间轮询到
        redisTemplate.opsForValue().set(key, String.valueOf(orderId), Duration.ofSeconds(20));
        log.debug("推送订单通知 driverId={} orderId={}", driverId, orderId);
        return true;
    }

    /**
     * 更新司机派单统计信息
     * 更新 last_dispatch_at 和 dispatch_count_today，用于下次调度评分
     */
    private void updateDriverDispatchStats(Long driverId) {
        Driver driver = driverMapper.selectById(driverId);
        if (driver != null) {
            driver.setLastDispatchAt(LocalDateTime.now());
            driver.setDispatchCountToday(
                    driver.getDispatchCountToday() != null ? driver.getDispatchCountToday() + 1 : 1);
            driverMapper.updateById(driver);
        }
    }

    /**
     * 发送延迟消息到 dispatch.delay.queue
     *
     * 消息体携带 orderId + dispatchIndex，用于 RetryConsumer 的幂等校验：
     * 若 15s 后消息到期时，Redis 中的当前索引已变更（说明已有新一轮派单），则忽略此消息。
     *
     * @param orderId       订单 ID
     * @param dispatchIndex 当前派单的候选列表下标
     */
    private void sendDelayMessage(Long orderId, int dispatchIndex) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("dispatchIndex", dispatchIndex);

        // 投递到 delay.exchange，routing key=dispatch.delay
        // dispatch.delay.queue 设置了 x-message-ttl=15000，15s 后自动转发到 dispatch.retry.queue
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DELAY_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_DELAY,
                msg
        );
    }

    /**
     * 发送取消消息到 cancel.queue
     */
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

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("JSON 序列化失败", e);
        }
    }
}