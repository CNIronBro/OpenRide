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
import com.ironbro.didi.service.dispatch.DispatchWaitingPool;
import com.ironbro.didi.websocket.WebSocketSessionManager;
import com.ironbro.didi.websocket.WsMessage;
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
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CancelConsumer {

    private final OrderMapper orderMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;
    private final ObjectMapper objectMapper;
    private final WebSocketSessionManager wsSessionManager;
    private final DispatchWaitingPool waitingPool;

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

        // 只有 DISPATCHING 状态的订单才能被系统自动取消
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

        // 从等待池移除该订单，防止 GlobalDispatchScheduler 在下一个 tick 再次尝试派单
        // 场景：订单刚进入等待池但尚未被调度器取出时，系统触发了取消（如全局超时）
        waitingPool.remove(orderId);

        // 记录取消日志
        OrderDispatchLog dispatchLog = new OrderDispatchLog();
        dispatchLog.setOrderId(orderId);
        dispatchLog.setDriverId(null); // 系统取消，无司机
        dispatchLog.setAction(DispatchAction.CANCELLED);
        dispatchLog.setRemark("系统自动取消：" + reason);
        dispatchLog.setCreatedAt(LocalDateTime.now());
        dispatchLogMapper.insert(dispatchLog);

        log.info("订单已自动取消 orderId={} reason={}", orderId, reason);

        // WS 推送取消通知给乘客，让乘客端立即感知，无需等待下次轮询
        // passengerId 即乘客的 user_id，与 WS session 的 userId 一致
        try {
            wsSessionManager.sendToUser(order.getPassengerId(),
                    new WsMessage("ORDER_CANCELLED", Map.of("orderId", orderId, "reason", reason)));
        } catch (Exception e) {
            // WS 推送失败不影响主流程，乘客端轮询兜底（最坏延迟 3s）
            log.warn("WS 推送取消通知失败，依赖乘客端轮询兜底 orderId={}", orderId, e);
        }
    }
}