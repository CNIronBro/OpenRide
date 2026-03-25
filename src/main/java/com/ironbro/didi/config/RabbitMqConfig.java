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
 *        → delay.exchange(dispatch.delay) → dispatch.delay.queue(TTL=15s)
 *        → 到期后 DLX 路由 → dispatch.exchange(dispatch.retry) → dispatch.retry.queue
 *        → RetryConsumer 检查是否已接单，未接单则重新派或发取消消息
 *        → dispatch.exchange(dispatch.cancel) → cancel.queue
 *        → CancelConsumer 执行自动取消
 *
 * 死信链路（消费失败兜底）：
 *   dispatch.queue / dispatch.retry.queue / cancel.queue 消费失败 NACK 后
 *   → dispatch.dlx → dispatch.dlq（人工处理或告警）
 *
 * 延迟队列实现原理：
 *   dispatch.delay.queue 本身不绑定消费者，设置 x-message-ttl=15000ms + x-dead-letter-exchange。
 *   消息在队列中等待 15s 后自动过期，RabbitMQ 将其路由到 DLX（dispatch.exchange），
 *   再通过 routing key=dispatch.retry 投递到 dispatch.retry.queue。
 *   这是 RabbitMQ 实现延迟队列的标准方式（TTL + DLX），无需额外插件。
 */
@Configuration
public class RabbitMqConfig {

    // ----------------------------------------------------------------
    // 常量：交换机名称
    // ----------------------------------------------------------------

    /** 派单主交换机（direct），负责 dispatch.new / dispatch.retry / dispatch.cancel 路由 */
    public static final String DISPATCH_EXCHANGE = "dispatch.exchange";

    /** 死信交换机（direct），消费失败的消息路由到此处 */
    public static final String DISPATCH_DLX = "dispatch.dlx";

    /**
     * 延迟交换机（direct），用于接收延迟消息的投递。
     * dispatch.delay.queue 的消息 TTL 到期后，DLX 指向 dispatch.exchange，
     * 所以延迟消息最终还是由 dispatch.exchange 路由到 dispatch.retry.queue。
     * 此处 delay.exchange 作为延迟消息的入口交换机（与 dispatch.exchange 分开，职责更清晰）。
     */
    public static final String DELAY_EXCHANGE = "delay.exchange";

    // ----------------------------------------------------------------
    // 常量：队列名称
    // ----------------------------------------------------------------

    /** 派单队列：DispatchConsumer 消费，执行 GEO 召回 + 评分 + 推送 */
    public static final String DISPATCH_QUEUE = "dispatch.queue";

    /**
     * 延迟队列：不绑定消费者，消息在此等待 15s 后自动过期转发到 dispatch.retry.queue。
     * 通过 x-message-ttl + x-dead-letter-exchange 实现延迟效果。
     */
    public static final String DISPATCH_DELAY_QUEUE = "dispatch.delay.queue";

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

    /** 延迟消息路由 key（投递到 dispatch.delay.queue） */
    public static final String ROUTING_DISPATCH_DELAY = "dispatch.delay";

    /** 延迟到期后重试路由 key（从 DLX 路由到 dispatch.retry.queue） */
    public static final String ROUTING_DISPATCH_RETRY = "dispatch.retry";

    /** 取消路由 key */
    public static final String ROUTING_DISPATCH_CANCEL = "dispatch.cancel";

    // ----------------------------------------------------------------
    // 交换机声明
    // ----------------------------------------------------------------

    @Bean
    public DirectExchange dispatchExchange() {
        // durable=true：RabbitMQ 重启后交换机不丢失
        return ExchangeBuilder.directExchange(DISPATCH_EXCHANGE).durable(true).build();
    }

    @Bean
    public DirectExchange dispatchDlx() {
        return ExchangeBuilder.directExchange(DISPATCH_DLX).durable(true).build();
    }

    @Bean
    public DirectExchange delayExchange() {
        return ExchangeBuilder.directExchange(DELAY_EXCHANGE).durable(true).build();
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
     * 延迟队列（核心）
     *
     * 关键参数说明：
     * - x-message-ttl=15000：队列中所有消息的统一 TTL（15s），到期后自动转发到 DLX
     * - x-dead-letter-exchange=dispatch.exchange：TTL 到期后路由到派单主交换机
     * - x-dead-letter-routing-key=dispatch.retry：到期消息用此 routing key 路由到 dispatch.retry.queue
     *
     * 注意：此队列不绑定任何消费者，消息只在此"等待"，不会被消费。
     */
    @Bean
    public Queue dispatchDelayQueue() {
        return QueueBuilder.durable(DISPATCH_DELAY_QUEUE)
                .withArgument("x-message-ttl", 15_000)                    // 15s 延迟
                .withArgument("x-dead-letter-exchange", DISPATCH_EXCHANGE) // 到期路由到派单主交换机
                .withArgument("x-dead-letter-routing-key", ROUTING_DISPATCH_RETRY) // 到期 routing key
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
    public Binding bindingDispatchQueue(Queue dispatchQueue, DirectExchange dispatchExchange) {
        return BindingBuilder.bind(dispatchQueue).to(dispatchExchange).with(ROUTING_DISPATCH_NEW);
    }

    /**
     * delay.exchange + dispatch.delay → dispatch.delay.queue
     * 下单后发延迟消息时，投递到 delay.exchange，routing key=dispatch.delay
     */
    @Bean
    public Binding bindingDelayQueue(Queue dispatchDelayQueue, DirectExchange delayExchange) {
        return BindingBuilder.bind(dispatchDelayQueue).to(delayExchange).with(ROUTING_DISPATCH_DELAY);
    }

    /**
     * dispatch.exchange + dispatch.retry → dispatch.retry.queue
     * dispatch.delay.queue 中消息 TTL 到期后，DLX 将消息路由到此处
     */
    @Bean
    public Binding bindingRetryQueue(Queue dispatchRetryQueue, DirectExchange dispatchExchange) {
        return BindingBuilder.bind(dispatchRetryQueue).to(dispatchExchange).with(ROUTING_DISPATCH_RETRY);
    }

    /** dispatch.exchange + dispatch.cancel → cancel.queue */
    @Bean
    public Binding bindingCancelQueue(Queue cancelQueue, DirectExchange dispatchExchange) {
        return BindingBuilder.bind(cancelQueue).to(dispatchExchange).with(ROUTING_DISPATCH_CANCEL);
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