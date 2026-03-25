package com.ironbro.didi.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ 交换机、队列、绑定配置
 *
 * 整体设计（见 technical-design.md 4.4 节）：
 *
 * 正常派单链路：
 *   下单 → dispatch.exchange(dispatch.new) → dispatch.queue
 *        → DispatchConsumer 处理后发延迟消息
 *        → dispatch.exchange(dispatch.retry, x-delay=15000) → [延迟 15s] → dispatch.retry.queue
 *        → RetryConsumer 检查是否已接单，未接单则重新派或发取消消息
 *        → dispatch.exchange(dispatch.cancel) → cancel.queue
 *        → CancelConsumer 执行自动取消
 *
 * 死信链路（消费失败兜底）：
 *   dispatch.queue / dispatch.retry.queue / cancel.queue 消费失败 NACK 后
 *   → dispatch.dlx → dispatch.dlq（人工处理或告警）
 *
 * 延迟队列实现原理：
 *   使用 rabbitmq-delayed-message-exchange 插件，dispatch.exchange 声明为 x-delayed-message 类型。
 *   发送延迟消息时在消息 header 中设置 x-delay（毫秒），Broker 持有消息直到延迟到期后再路由。
 *   相比 TTL+DLX 方案，无需额外的 delay 队列和交换机，拓扑更简洁。
 */
@Configuration
public class RabbitMqConfig {

    // ----------------------------------------------------------------
    // 常量：交换机名称
    // ----------------------------------------------------------------

    /**
     * 派单主交换机（x-delayed-message 类型）。
     * 使用 rabbitmq-delayed-message-exchange 插件，支持通过消息 header x-delay（毫秒）实现延迟投递。
     * 同时承担普通路由（dispatch.new / dispatch.cancel）和延迟路由（dispatch.retry）职责。
     */
    public static final String DISPATCH_EXCHANGE = "dispatch.exchange";

    /** 死信交换机（direct），消费失败的消息路由到此处 */
    public static final String DISPATCH_DLX = "dispatch.dlx";

    // ----------------------------------------------------------------
    // 常量：队列名称
    // ----------------------------------------------------------------

    /** 派单队列：DispatchConsumer 消费，执行 GEO 召回 + 评分 + 推送 */
    public static final String DISPATCH_QUEUE = "dispatch.queue";

    /** 重试队列：RetryConsumer 消费，检查接单状态，决定继续派或取消 */
    public static final String DISPATCH_RETRY_QUEUE = "dispatch.retry.queue";

    /** 取消队列：CancelConsumer 消费，将订单状态改为 CANCELLED */
    public static final String CANCEL_QUEUE = "cancel.queue";

    /** 死信队列：消费失败的消息落地，供人工处理或告警 */
    public static final String DISPATCH_DLQ = "dispatch.dlq";

    // ----------------------------------------------------------------
    // 常量：路由 key
    // ----------------------------------------------------------------

    /** 新订单派单路由 key */
    public static final String ROUTING_DISPATCH_NEW = "dispatch.new";

    /** 延迟重试路由 key（发送时携带 x-delay header，到期后路由到 dispatch.retry.queue） */
    public static final String ROUTING_DISPATCH_RETRY = "dispatch.retry";

    /** 取消路由 key */
    public static final String ROUTING_DISPATCH_CANCEL = "dispatch.cancel";

    // ----------------------------------------------------------------
    // 交换机声明
    // ----------------------------------------------------------------

    /**
     * 派单主交换机，类型为 x-delayed-message（插件提供）。
     *
     * x-delayed-message 是插件注册的自定义交换机类型，内部实际路由类型通过 x-delayed-type 参数指定。
     * 发送消息时在 header 中设置 x-delay（毫秒），Broker 会持有消息直到延迟到期后再按 routing key 路由。
     * 非延迟消息（不带 x-delay header）会立即路由，与普通 direct 交换机行为一致。
     */
    @Bean
    public CustomExchange dispatchExchange() {
        Map<String, Object> args = new HashMap<>();
        // 指定底层路由类型为 direct，插件在延迟到期后按 direct 规则路由消息
        args.put("x-delayed-type", "direct");
        return new CustomExchange(DISPATCH_EXCHANGE, "x-delayed-message", true, false, args);
    }

    @Bean
    public DirectExchange dispatchDlx() {
        return ExchangeBuilder.directExchange(DISPATCH_DLX).durable(true).build();
    }

    // ----------------------------------------------------------------
    // 队列声明
    // ----------------------------------------------------------------

    /**
     * 派单队列
     * 消费失败（NACK 且 requeue=false）时，消息路由到 dispatch.dlx → dispatch.dlq
     */
    @Bean
    public Queue dispatchQueue() {
        return QueueBuilder.durable(DISPATCH_QUEUE)
                .withArgument("x-dead-letter-exchange", DISPATCH_DLX)
                .withArgument("x-dead-letter-routing-key", "dlq")
                .build();
    }

    /**
     * 重试队列
     * 消费失败时路由到死信队列
     */
    @Bean
    public Queue dispatchRetryQueue() {
        return QueueBuilder.durable(DISPATCH_RETRY_QUEUE)
                .withArgument("x-dead-letter-exchange", DISPATCH_DLX)
                .withArgument("x-dead-letter-routing-key", "dlq")
                .build();
    }

    /**
     * 取消队列
     * 消费失败时路由到死信队列
     */
    @Bean
    public Queue cancelQueue() {
        return QueueBuilder.durable(CANCEL_QUEUE)
                .withArgument("x-dead-letter-exchange", DISPATCH_DLX)
                .withArgument("x-dead-letter-routing-key", "dlq")
                .build();
    }

    /**
     * 死信队列（DLQ）
     * 接收所有消费失败的消息，不设置 DLX（终点队列，人工处理）
     */
    @Bean
    public Queue dispatchDlq() {
        return QueueBuilder.durable(DISPATCH_DLQ).build();
    }

    // ----------------------------------------------------------------
    // 绑定关系
    // ----------------------------------------------------------------

    /** dispatch.exchange + dispatch.new → dispatch.queue */
    @Bean
    public Binding bindingDispatchQueue(Queue dispatchQueue, CustomExchange dispatchExchange) {
        return BindingBuilder.bind(dispatchQueue).to(dispatchExchange).with(ROUTING_DISPATCH_NEW).noargs();
    }

    /** dispatch.exchange + dispatch.retry → dispatch.retry.queue（延迟消息到期后路由到此） */
    @Bean
    public Binding bindingRetryQueue(Queue dispatchRetryQueue, CustomExchange dispatchExchange) {
        return BindingBuilder.bind(dispatchRetryQueue).to(dispatchExchange).with(ROUTING_DISPATCH_RETRY).noargs();
    }

    /** dispatch.exchange + dispatch.cancel → cancel.queue */
    @Bean
    public Binding bindingCancelQueue(Queue cancelQueue, CustomExchange dispatchExchange) {
        return BindingBuilder.bind(cancelQueue).to(dispatchExchange).with(ROUTING_DISPATCH_CANCEL).noargs();
    }

    /** dispatch.dlx + dlq → dispatch.dlq（死信落地） */
    @Bean
    public Binding bindingDlq(Queue dispatchDlq, DirectExchange dispatchDlx) {
        return BindingBuilder.bind(dispatchDlq).to(dispatchDlx).with("dlq");
    }

    /**
     * 使用 Jackson JSON 序列化消息体。
     * 配置后 RabbitMQ 管理后台可直接读取消息内容，消费者也无需手动反序列化。
     * Spring AMQP 会自动检测此 Bean 并替换默认的 Java 序列化方式。
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}