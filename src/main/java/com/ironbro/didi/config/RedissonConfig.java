package com.ironbro.didi.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 分布式锁配置
 *
 * - 派单幂等：防止同一订单的派单消息被多个 MQ 消费者实例并发消费两次
 * - 补偿幂等：防止 xxl-job 多节点并发执行同一订单的补偿任务
 * - 重复推送防护：防止同一司机被重复推送同一订单
 *
 *
 * 连接配置复用 spring.data.redis 中的 host/port，保持单一数据源。
 */
@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    /**
     *
     * Redisson tryLock 的 leaseTime 设置为锁的最大持有时间，超时后自动释放，防止死锁。
     * 各业务场景的 leaseTime 在调用处单独指定，此处只做连接配置。
     *
     */
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + redisHost + ":" + redisPort)
                // 连接池最小空闲连接数
                .setConnectionMinimumIdleSize(2)
                // 连接池最大连接数
                .setConnectionPoolSize(10)
                // 连接超时（ms）
                .setConnectTimeout(3000)
                // 命令等待超时（ms）
                .setTimeout(3000);
        return Redisson.create(config);
    }
}