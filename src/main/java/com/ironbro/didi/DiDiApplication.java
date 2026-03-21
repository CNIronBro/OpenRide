package com.ironbro.didi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling  // 开启 @Scheduled，支持假在线检测定时任务（阶段 8 替换为 xxl-job）
public class DiDiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DiDiApplication.class, args);
    }

}
