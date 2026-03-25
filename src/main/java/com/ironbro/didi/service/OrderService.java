package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.config.RabbitMqConfig;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 订单服务
 *
 * 阶段 5 职责：
 * 1. 乘客下单：写库 + 发送派单消息到 dispatch.queue
 * 2. 查询订单状态（供乘客端轮询）
 *
 * 阶段 6 将在此类中补充：接单（CAS）、行程状态流转、取消等接口。
 *
 * 事务说明：
 * createOrder 使用 @Transactional，保证写库和发 MQ 消息的原子性。
 * 注意：Spring 的 @Transactional 无法保证 MQ 消息与数据库的强一致性（两阶段提交），
 * 但在本项目规模下，先写库再发 MQ，若 MQ 发送失败，xxl-job 补偿任务会兜底重新触发派单。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderMapper orderMapper;
    private final DriverMapper driverMapper;
    private final RabbitTemplate rabbitTemplate;
    private final StringRedisTemplate redisTemplate;
    private final PricingService pricingService;

    /**
     * 乘客下单
     *
     * 业务流程：
     * 1. 构建订单实体，初始状态为 DISPATCHING（直接进入派单，跳过 PENDING 中间态）
     * 2. 写入数据库
     * 3. 发送派单消息到 dispatch.queue，消息体携带 orderId 和下单位置
     *
     * 状态说明：
     * 直接设为 DISPATCHING 而非 PENDING，是因为下单和发 MQ 在同一事务中完成，
     * 不存在"已下单但未进入派单队列"的中间状态需要区分。
     *
     * @param passengerId 乘客 user_id
     * @param req         下单请求参数
     * @return 创建的订单
     */
    @Transactional
    public Order createOrder(Long passengerId, CreateOrderRequest req) {
        // 构建订单
        Order order = new Order();
        order.setPassengerId(passengerId);
        order.setStatus(OrderStatus.DISPATCHING);
        order.setOriginLat(req.originLat());
        order.setOriginLng(req.originLng());
        order.setOriginAddr(req.originAddr());
        order.setDestLat(req.destLat());
        order.setDestLng(req.destLng());
        order.setDestAddr(req.destAddr());
        order.setEstimatedPrice(req.estimatedPrice());
        order.setSurgeFactor(BigDecimal.ONE);
        order.setDispatchRetryCount(0);
        order.setVersion(0);
        order.setCreatedAt(LocalDateTime.now());

        orderMapper.insert(order);

        // 必须在事务提交后再发 MQ 消息，否则消费者可能在事务提交前查询订单，导致读不到数据
        // 场景：@Transactional 事务内直接发 MQ，消息极快被消费，但 INSERT 尚未提交，
        //       DispatchConsumer.selectById 返回 null，订单被误判为"不在派单中状态"而跳过
        // TransactionSynchronizationManager.registerSynchronization 注册事务提交后回调，
        // afterCommit() 在当前事务成功提交后才执行，保证消费者能读到已提交的订单数据
        final Long orderId = order.getId();
        final Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("originLat", req.originLat());
        msg.put("originLng", req.originLng());
        msg.put("city", req.city());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                // 投递到 dispatch.exchange，routing key=dispatch.new → dispatch.queue
                rabbitTemplate.convertAndSend(
                        RabbitMqConfig.DISPATCH_EXCHANGE,
                        RabbitMqConfig.ROUTING_DISPATCH_NEW,
                        msg
                );
            }
        });

        log.info("下单成功，orderId={} passengerId={}", order.getId(), passengerId);
        return order;
    }

    /**
     * 查询订单详情（乘客端轮询用）
     *
     * @param orderId     订单 ID
     * @param passengerId 乘客 ID（用于鉴权，防止越权查询他人订单）
     */
    public Order getOrder(Long orderId, Long passengerId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BizException("订单不存在");
        }
        if (!order.getPassengerId().equals(passengerId)) {
            throw new BizException(403, "无权查看此订单");
        }
        return order;
    }

    // ----------------------------------------------------------------
    // 阶段 6：行程状态流转
    // ----------------------------------------------------------------

    /**
     * 司机接单（CAS 乐观锁）
     *
     * 核心并发控制：
     * 使用 MyBatis-Plus @Version 乐观锁，底层 SQL 为：
     *   UPDATE `order` SET driver_id=?, status='ACCEPTED', version=version+1
     *   WHERE id=? AND status='DISPATCHING' AND version=?
     * 若 version 不匹配（已被其他司机接单），updateById 返回影响行数为 0，
     * MyBatis-Plus 会抛出 OptimisticLockerException，此处捕获后转为业务异常。
     *
     * 接单成功后副作用：
     * 1. 司机状态改为 IN_TRIP，从 GEO 在线集合移除（不再参与新派单）
     * 2. 清除司机待接单通知 key（driver:pending:order:{driverId}）
     *
     * @param orderId  订单 ID
     * @param driverId 司机的 driver.id（非 user_id）
     */
    @Transactional
    public Order acceptOrder(Long orderId, Long driverId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");
        if (order.getStatus() != OrderStatus.DISPATCHING) {
            throw new BizException(409, "订单已被接单或已取消");
        }

        // CAS 更新：@Version 注解会自动追加 AND version=? 并自增
        // 若并发接单导致 version 不匹配，updateById 影响行数为 0，抛 OptimisticLockerException
        order.setDriverId(driverId);
        order.setStatus(OrderStatus.ACCEPTED);
        order.setAcceptedAt(LocalDateTime.now());
        int rows = orderMapper.updateById(order);
        if (rows == 0) {
            // 乐观锁冲突：订单已被其他司机抢走
            throw new BizException(409, "订单已被其他司机接单");
        }

        // 接单成功：更新司机状态为 IN_TRIP，从 GEO 在线集合移除
        Driver driver = driverMapper.selectById(driverId);
        if (driver != null) {
            driver.setStatus(DriverStatus.IN_TRIP);
            driverMapper.updateById(driver);
            // 从 GEO 在线集合移除，行程中司机不参与新派单
            // 城市默认 "default"，阶段 7 可扩展为从订单中取 city
            redisTemplate.opsForZSet().remove("driver:online:default", String.valueOf(driverId));
        }

        // 清除司机待接单通知 key，避免司机端重复弹窗
        redisTemplate.delete("driver:pending:order:" + driverId);

        log.info("司机接单成功 orderId={} driverId={}", orderId, driverId);
        return orderMapper.selectById(orderId);
    }

    /**
     * 司机到达接客点（ACCEPTED → PICKING）
     *
     * @param orderId  订单 ID
     * @param driverId 司机 driver.id（用于鉴权，防止越权操作他人订单）
     */
    @Transactional
    public Order arrive(Long orderId, Long driverId) {
        Order order = getOrderForDriver(orderId, driverId);
        if (order.getStatus() != OrderStatus.ACCEPTED) {
            throw new BizException(400, "当前订单状态不允许此操作");
        }
        order.setStatus(OrderStatus.PICKING);
        orderMapper.updateById(order);
        return order;
    }

    /**
     * 开始行程（PICKING → IN_TRIP）
     *
     * @param orderId  订单 ID
     * @param driverId 司机 driver.id
     */
    @Transactional
    public Order startTrip(Long orderId, Long driverId) {
        Order order = getOrderForDriver(orderId, driverId);
        if (order.getStatus() != OrderStatus.PICKING) {
            throw new BizException(400, "当前订单状态不允许此操作");
        }
        order.setStatus(OrderStatus.IN_TRIP);
        order.setStartedAt(LocalDateTime.now());
        orderMapper.updateById(order);
        return order;
    }

    /**
     * 结束行程（IN_TRIP → FINISHED）
     *
     * 行程结束后调用 PricingService 计算真实价格：
     * - 距离：根据起终点坐标用 Haversine 公式估算（mock 场景下坐标固定，结果稳定）
     * - 时长：startedAt → finishedAt 的分钟数
     * - surge 系数：从 Redis 读取区域供需比实时计算
     *
     * 副作用：司机状态恢复为 ONLINE，等待下一单（不重新加入 GEO，
     * 司机需重新上报位置才会出现在 GEO 中，符合实际业务逻辑）。
     *
     * @param orderId  订单 ID
     * @param driverId 司机 driver.id
     */
    @Transactional
    public Order finishTrip(Long orderId, Long driverId) {
        Order order = getOrderForDriver(orderId, driverId);
        if (order.getStatus() != OrderStatus.IN_TRIP) {
            throw new BizException(400, "当前订单状态不允许此操作");
        }

        LocalDateTime finishedAt = LocalDateTime.now();
        order.setStatus(OrderStatus.FINISHED);
        order.setFinishedAt(finishedAt);

        // 计算行程时长（分钟），startedAt 为空时兜底用 10 分钟
        double durationMin = order.getStartedAt() != null
                ? Duration.between(order.getStartedAt(), finishedAt).toMinutes()
                : 10.0;

        // 计算行程距离（km），用 Haversine 公式估算起终点直线距离
        // mock 坐标场景下结果稳定，真实场景可替换为地图 API 返回的实际里程
        double distanceKm = haversineKm(
                order.getOriginLat().doubleValue(), order.getOriginLng().doubleValue(),
                order.getDestLat().doubleValue(),   order.getDestLng().doubleValue()
        );

        // 调用动态计价服务，计算含 surge 系数的最终价格
        BigDecimal actualPrice = pricingService.calculate("default", distanceKm, durationMin);
        BigDecimal surgeFactor = pricingService.getSurgeFactor("default");

        order.setActualPrice(actualPrice);
        order.setSurgeFactor(surgeFactor);
        orderMapper.updateById(order);

        // 司机行程结束，恢复 ONLINE 状态，等待下一单
        Driver driver = driverMapper.selectById(driverId);
        if (driver != null) {
            driver.setStatus(DriverStatus.ONLINE);
            driver.setIdleSince(LocalDateTime.now());
            driverMapper.updateById(driver);
        }

        log.info("行程结束 orderId={} driverId={} dist={}km dur={}min actualPrice={} surgeFactor={}",
                orderId, driverId, distanceKm, durationMin, actualPrice, surgeFactor);
        return order;
    }

    /**
     * 取消订单（乘客或司机主动取消）
     *
     * 允许取消的状态：DISPATCHING、ACCEPTED、PICKING
     * 行程中（IN_TRIP）不允许取消，需联系客服处理。
     *
     * 取消后副作用：
     * - 若司机已接单（status >= ACCEPTED），司机状态恢复为 ONLINE
     *
     * @param orderId   订单 ID
     * @param userId    操作者 user_id（乘客或司机的 user_id，用于鉴权）
     * @param cancelBy  取消方：PASSENGER / DRIVER
     * @param reason    取消原因
     */
    @Transactional
    public void cancelOrder(Long orderId, Long userId, String cancelBy, String reason) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");

        // 鉴权：乘客只能取消自己的订单；司机通过 driver.userId 关联
        if ("PASSENGER".equals(cancelBy) && !order.getPassengerId().equals(userId)) {
            throw new BizException(403, "无权操作此订单");
        }
        if ("DRIVER".equals(cancelBy)) {
            // 校验 userId 对应的司机是否是该订单的接单司机
            Driver driver = driverMapper.selectOne(
                    new LambdaQueryWrapper<Driver>().eq(Driver::getUserId, userId));
            if (driver == null || !driver.getId().equals(order.getDriverId())) {
                throw new BizException(403, "无权操作此订单");
            }
        }

        if (order.getStatus() == OrderStatus.IN_TRIP) {
            throw new BizException(400, "行程中无法取消，请联系客服");
        }
        if (order.getStatus() == OrderStatus.FINISHED || order.getStatus() == OrderStatus.CANCELLED) {
            throw new BizException(400, "订单已结束，无法取消");
        }

        order.setStatus(OrderStatus.CANCELLED);
        order.setCancelBy(cancelBy);
        order.setCancelReason(reason);
        order.setCancelledAt(LocalDateTime.now());
        orderMapper.updateById(order);

        // 若司机已接单，取消后恢复司机为 ONLINE
        if (order.getDriverId() != null) {
            Driver driver = driverMapper.selectById(order.getDriverId());
            if (driver != null && driver.getStatus() == DriverStatus.IN_TRIP) {
                driver.setStatus(DriverStatus.ONLINE);
                driver.setIdleSince(LocalDateTime.now());
                driverMapper.updateById(driver);
            }
        }

        log.info("订单取消 orderId={} cancelBy={} reason={}", orderId, cancelBy, reason);
    }

    /**
     * 司机端查询订单详情（用于新订单弹窗，不校验乘客身份）
     */
    public Order getOrderForDriverView(Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");
        return order;
    }

    /**
     * 查询订单（司机端鉴权）
     * 校验订单存在且 driverId 匹配，防止越权操作他人订单。
     */
    private Order getOrderForDriver(Long orderId, Long driverId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");
        if (!driverId.equals(order.getDriverId())) throw new BizException(403, "无权操作此订单");
        return order;
    }

    /**
     * 下单请求参数
     *
     * @param originLat      起点纬度
     * @param originLng      起点经度
     * @param originAddr     起点地址
     * @param destLat        终点纬度
     * @param destLng        终点经度
     * @param destAddr       终点地址
     * @param estimatedPrice 预估价格
     * @param city           城市标识（用于 GEO 召回分片）
     */
    public record CreateOrderRequest(
            BigDecimal originLat,
            BigDecimal originLng,
            String originAddr,
            BigDecimal destLat,
            BigDecimal destLng,
            String destAddr,
            BigDecimal estimatedPrice,
            String city
    ) {}

    /**
     * Haversine 公式计算两点间球面距离（公里）
     *
     * 用于行程结束时估算里程，精度满足计价需求（误差 < 0.5%）。
     * 真实场景可替换为地图 API 返回的实际行驶里程。
     */
    public static double haversineKm(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6371.0; // 地球半径（km）
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}