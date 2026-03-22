package com.ironbro.didi.config;

import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * xxl-job 执行器配置
 *
 * xxl-job 是一个分布式任务调度平台，由 xxl-job-admin（调度中心）和执行器（本应用）两部分组成：
 * - 调度中心：负责任务的触发、调度、监控，独立部署
 * - 执行器：嵌入业务应用，注册到调度中心，接收并执行任务
 *
 * 本配置类通过 XxlJobSpringExecutor 将本应用注册为执行器，
 * 调度中心通过 HTTP 回调执行器的 JobHandler 方法来触发任务。
 *
 * 配置项说明（见 application.yml xxl.job 节）：
 * - admin.addresses：调度中心地址，执行器启动时向此地址注册
 * - executor.appname：执行器名称，需与调度中心配置的执行器 AppName 一致
 * - executor.port：执行器监听端口，调度中心通过此端口回调
 * - executor.logpath：任务执行日志存储路径
 * - executor.logretentiondays：日志保留天数
 * - accessToken：调度中心与执行器通信的鉴权 token，需保持一致
 */
@Slf4j
@Configuration
public class XxlJobConfig {

    @Value("${xxl.job.admin.addresses}")
    private String adminAddresses;

    @Value("${xxl.job.executor.appname}")
    private String appname;

    @Value("${xxl.job.executor.port}")
    private int port;

    @Value("${xxl.job.executor.logpath}")
    private String logpath;

    @Value("${xxl.job.executor.logretentiondays}")
    private int logRetentionDays;

    @Value("${xxl.job.accessToken}")
    private String accessToken;

    /**
     * 注册 xxl-job 执行器 Bean
     *
     * XxlJobSpringExecutor 会在 Spring 容器启动后自动：
     * 1. 扫描所有标注了 @XxlJob 的方法，注册为 JobHandler
     * 2. 启动内嵌 HTTP 服务器（监听 executor.port），等待调度中心回调
     * 3. 向调度中心注册本执行器（appname + ip + port）
     *
     * 注意：执行器端口（9999）与 Spring Boot 端口（8080）不同，两者独立监听。
     */
    @Bean
    public XxlJobSpringExecutor xxlJobSpringExecutor() {
        XxlJobSpringExecutor executor = new XxlJobSpringExecutor();
        executor.setAdminAddresses(adminAddresses);
        executor.setAppname(appname);
        executor.setPort(port);
        executor.setLogPath(logpath);
        executor.setLogRetentionDays(logRetentionDays);
        executor.setAccessToken(accessToken);

        log.info("xxl-job 执行器已配置 appname={} port={} adminAddresses={}", appname, port, adminAddresses);
        return executor;
    }
}