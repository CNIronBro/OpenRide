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
 * 本项目使用 Redisson 解决"协调型并发"问题，即防止同一任务被多个实例重复执行：
 * - 派单幂等：防止同一订单的派单消息被多个 MQ 消费者实例并发消费两次
 * - 补偿幂等：防止 xxl-job 多节点并发执行同一订单的补偿任务
 * - 重复推送防护：防止同一司机被重复推送同一订单
 *
 * 注意：Redisson 不用于接单一致性（那是 MySQL CAS 的职责），
 * 避免滥用分布式锁带来的性能损耗和死锁风险。
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
     * 创建 RedissonClient（单节点模式）
     *
     * leaseTime 说明：
     * Redisson tryLock 的 leaseTime 设置为锁的最大持有时间，超时后自动释放，防止死锁。
     * 各业务场景的 leaseTime 在调用处单独指定，此处只做连接配置。
     *
     * 注意：redisson-spring-boot-starter 会自动尝试读取 redisson.yaml/redisson.json，
     * 若不存在则需手动声明此 Bean。本项目选择手动声明以便统一管理配置。
     */
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        // 单节点模式，地址格式为 redis://host:port
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