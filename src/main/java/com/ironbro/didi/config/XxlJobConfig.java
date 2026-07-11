package com.ironbro.didi.config;

import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * xxl-job 执行器配置
 * - 调度中心：负责任务的触发、调度、监控，独立部署
 * - 执行器：嵌入业务应用，注册到调度中心，接收并执行任务
 *
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