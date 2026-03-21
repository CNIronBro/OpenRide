package com.ironbro.didi.mq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ironbro.didi.config.RabbitMqConfig;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.OrderDispatchLog;
import com.ironbro.didi.enums.DispatchAction;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * 订单自动取消消费者
 *
 * 消费 cancel.queue 中的取消消息，将订单状态改为 CANCELLED（系统自动取消）。
 *
 * 触发场景：
 * 1. 候选司机列表耗尽（所有候选司机均未在 15s 内接单）
 * 2. 附近无可用司机（GEO 召回结果为空）
 *
 * 幂等说明：
 * 若订单已不在 DISPATCHING 状态（如乘客已主动取消），直接忽略，不重复取消。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CancelConsumer {

    private final OrderMapper orderMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;
    private final ObjectMapper objectMapper;

    /**
     * 消费取消消息
     *
     * 消息体格式（Map）：
     * {
     *   "orderId": Long,
     *   "reason":  String  // 取消原因
     * }
     */
    @RabbitListener(queues = RabbitMqConfig.CANCEL_QUEUE)
    public void onCancel(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Map<String, Object> body;
        try {
            body = objectMapper.readValue(message.getBody(), new TypeReference<>() {});
        } catch (Exception e) {
            log.error("取消消息解析失败，NACK 进死信", e);
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        Long orderId = ((Number) body.get("orderId")).longValue();
        String reason = (String) body.getOrDefault("reason", "系统自动取消");

        try {
            doCancel(orderId, reason);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("取消处理失败 orderId={}", orderId, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }

    /**
     * 执行订单自动取消
     *
     * 幂等：若订单已不在 DISPATCHING 状态，直接忽略，不重复操作。
     *
     * @param orderId 订单 ID
     * @param reason  取消原因
     */
    private void doCancel(Long orderId, String reason) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            log.warn("取消时订单不存在 orderId={}", orderId);
            return;
        }

        // 幂等：只有 DISPATCHING 状态的订单才能被系统自动取消
        if (order.getStatus() != OrderStatus.DISPATCHING) {
            log.info("订单已不在派单中状态，忽略取消 orderId={} status={}", orderId, order.getStatus());
            return;
        }

        // 更新订单状态为 CANCELLED
        order.setStatus(OrderStatus.CANCELLED);
        order.setCancelBy("SYSTEM");
        order.setCancelReason(reason);
        order.setCancelledAt(LocalDateTime.now());
        orderMapper.updateById(order);

        // 记录取消日志
        OrderDispatchLog dispatchLog = new OrderDispatchLog();
        dispatchLog.setOrderId(orderId);
        dispatchLog.setDriverId(null); // 系统取消，无司机
        dispatchLog.setAction(DispatchAction.CANCELLED);
        dispatchLog.setRemark("系统自动取消：" + reason);
        dispatchLog.setCreatedAt(LocalDateTime.now());
        dispatchLogMapper.insert(dispatchLog);

        log.info("订单已自动取消 orderId={} reason={}", orderId, reason);

        // TODO 阶段 5 暂不实现推送通知，乘客端通过轮询订单状态感知取消
        // 后续可接入 WebSocket 或消息推送服务通知乘客
    }
}