package com.ironbro.didi.service;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.config.RabbitMqConfig;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
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
    private final RabbitTemplate rabbitTemplate;

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

        // 发送派单消息
        // 消息体携带 orderId 和下单位置（供 DispatchConsumer 做 GEO 召回）
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", order.getId());
        msg.put("originLat", req.originLat());
        msg.put("originLng", req.originLng());
        msg.put("city", req.city());

        // 投递到 dispatch.exchange，routing key=dispatch.new → dispatch.queue
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_NEW,
                msg
        );

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
}