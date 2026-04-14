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
 * 6. 发送延迟消息到 dispatch.exchange（x-delay=15s），消息体携带 orderId + dispatchIndex=0
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
     * @param message Spring AMQP 对 RabbitMQ 原始消息的封装，包含两部分：
     *                - message.getBody()：消息体的原始字节数组，用 Jackson 反序列化成 Map
     *                - message.getMessageProperties().getDeliveryTag()：Broker 为这条消息分配的唯一序号
     *                  （在当前 Channel 内单调递增），ACK/NACK 时用它告诉 Broker "我确认的是哪条消息"
     * @param channel RabbitMQ 底层 TCP 通道，用于向 Broker 发送 ACK/NACK 信号：
     *                - basicAck(deliveryTag, false)：处理成功，Broker 删除该消息
     *                - basicNack(deliveryTag, false, requeue=false)：处理失败，不重入队列，路由到死信队列
     *                - 第二个参数 multiple=false 表示只确认这一条，不批量确认
     *                使用手动 ACK 而非自动 ACK 的原因：自动 ACK 在消息到达时立即确认，
     *                业务处理中途崩溃会导致消息丢失；手动 ACK 确保成功处理后才确认，
     *                失败时进死信队列便于排查，幂等重复时主动 ACK 丢弃避免重入队列。
     *                注意：Channel 操作会抛 IOException，因此方法签名需声明 throws IOException
     */
    @RabbitListener(queues = RabbitMqConfig.DISPATCH_QUEUE)
    public void onDispatch(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();//消息唯一序号。
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
        // QUESTION 此处为什么会出现多个消费者消费同一订单的情况？
        // QUESTION 代码做了消费者确认机制，消费者A执行doDispatch()后，如果在发送ACK前网络断开或服务崩溃，MQ会因为没有收到ACK而重新投递该消息。
        // QUESTION 如果此时A持有的分布式锁还未释放或未过期，那么消费者B再次消费时将拿不到锁，从而直接ACK跳过，避免短时间内的重复处理。
        // QUESTION 但如果锁已经释放或过期，那么B仍可能重新拿到锁并再次执行doDispatch()
        // QUESTION 第一次消费不受影响，会正常处理业务，因为已经执行了doDispatch()。
        String lockKey = "lock:dispatch:" + orderId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;
        try {
            // 0:不等待，一上来就抢锁，抢不到就return false。
            // 30：锁自动释放时间为30s，30秒后Redis会自动删掉这把锁，避免死锁
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
    // QUESTION 在之前只做redission锁仍然不够稳，因为如果消费者B消费前消费者A已经释放了锁，那么仍然会出现重复消费的情况，所以要在业务层面再加一层保险。
    // QUESTION 业务层面的这层保险就是“检查订单是否仍处于派单中状态”。
    // QUESTION 只有业务层面的保险可以吗？不加redission锁可以吗？
    // QUESTION 也不行！因为假设不加redission锁，同一个订单来了两条重复消息，被两个消费者A、B同时拿到，
    // QUESTION 此时查出来的订单状态都处于派单中，那么都可以继续执行，又重复消费了。
    // QUESTION redission锁的价值是限制同一个订单，同一时刻只允许一个消费者进doDispatch；
    // QUESTION 订单状态校验的价值是校验业务正确性，因为即使拿到了锁，也不代表订单应该派。
    // QUESTION 可能由于网络波动导致重复发了两条一样的订单到队列中，所以必须加上业务校验。
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
            // 附近暂无在线司机，不立即取消，发延迟消息等待司机上线
            // waitedSeconds=0 表示本次是第一次无司机，RetryConsumer 收到后会重新 GEO 召回
            log.info("附近无在线司机，等待重试 orderId={}", orderId);
            sendNoDriverDelayMessage(orderId, 0, DISPATCH_RADIUS_KM);
            return;
        }

        // 查询候选司机详情（含 acceptRate、idleSince、dispatchCountToday）
        List<Driver> drivers = driverMapper.selectList(
                new LambdaQueryWrapper<Driver>()
                        .in(Driver::getId, nearbyDriverIds)
                        .eq(Driver::getStatus, DriverStatus.ONLINE)
                        .eq(Driver::getAuditStatus, AuditStatus.APPROVED));

        if (drivers.isEmpty()) {
            // GEO 召回有结果但 DB 查询后均不可用（状态变更竞态），同样等待重试
            log.info("候选司机均不可用，等待重试 orderId={}", orderId);
            sendNoDriverDelayMessage(orderId, 0, DISPATCH_RADIUS_KM);
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

        // 5.3.5 发送延迟消息（x-delay=15s），消息体携带 orderId + dispatchIndex=0
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
    // QUESTION
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
    // QUESTION
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
     * 发送延迟消息
     *
     * 通过 rabbitmq-delayed-message-exchange 插件实现 15s 延迟：
     * 在消息 header 中设置 x-delay=15000（毫秒），Broker 持有消息直到延迟到期后路由到 dispatch.retry.queue。
     * 消息体携带 orderId + dispatchIndex，用于 RetryConsumer 的幂等校验。
     *
     * @param orderId       订单 ID
     * @param dispatchIndex 当前派单的候选列表下标
     */
    // QUESTION
    private void sendDelayMessage(Long orderId, int dispatchIndex) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("dispatchIndex", dispatchIndex);

        // 延迟15s
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

    /**
     * 无司机时发延迟消息，等待 15s 后由 RetryConsumer 重新 GEO 召回
     *
     * currentRadiusKm 携带本轮使用的搜索半径，RetryConsumer 收到后据此决定下一轮扩圈半径。
     * 初始调用时传入 DISPATCH_RADIUS_KM（5.0km），作为扩圈起点。
     *
     * @param orderId        订单 ID
     * @param waitedSeconds  已等待秒数（每轮 +15）
     * @param currentRadiusKm 本轮搜索半径（公里）
     */
    private void sendNoDriverDelayMessage(Long orderId, int waitedSeconds, double currentRadiusKm) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("dispatchIndex", -1);       // 无司机重试不使用 dispatchIndex，填 -1 占位
        msg.put("noDriverRetry", true);
        msg.put("waitedSeconds", waitedSeconds);
        msg.put("currentRadiusKm", currentRadiusKm);  // 本轮搜索半径，用于下一轮扩圈决策
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