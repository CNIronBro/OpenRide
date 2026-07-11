package com.ironbro.didi.job;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.service.DriverLocationService;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 假在线司机清理任务（xxl-job JobHandler）
 *
 * 业务逻辑：
 * 扫描数据库中 status=ONLINE 的司机，检查其 Redis 心跳 key 是否存在。
 * 心跳 key TTL=30s，若已过期说明司机 30s 内未上报位置（断网/假在线），强制下线。
 *
 * 注意：
 * - 此任务不需要 Redisson 锁，因为 xxl-job 路由策略本身保证单实例执行
 * - 若需要多实例并行分片执行（大量司机场景），可改用分片广播路由策略
 * - 当前 city 固定为 "default"，生产环境应按城市分片扫描
 *
 * xxl-job 配置说明：
 * - JobHandler 名称：fakeOnlineCleanJob（需在调度中心新建任务时填写此名称）
 * - 建议执行频率：每 30s 一次（Cron: 0/30 * * * * ?）
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FakeOnlineCleanJob {

    private final DriverMapper driverMapper;
    private final DriverLocationService driverLocationService;
    private final StringRedisTemplate redisTemplate;

    /**
     * 扫描并清理假在线司机
     *
     * @XxlJob("fakeOnlineCleanJob") 将此方法注册为名为 "fakeOnlineCleanJob" 的 JobHandler，
     * 调度中心通过此名称找到并回调执行。
     */
    @XxlJob("fakeOnlineCleanJob")
    public void cleanFakeOnlineDrivers() {
        log.info("[xxl-job] 开始扫描假在线司机");

        // 查询所有 ONLINE 状态的司机
        List<Driver> onlineDrivers = driverMapper.selectList(
                new LambdaQueryWrapper<Driver>().eq(Driver::getStatus, DriverStatus.ONLINE));

        int cleanedCount = 0;
        for (Driver driver : onlineDrivers) {
            String heartbeatKey = "driver:heartbeat:" + driver.getId();
            Boolean alive = redisTemplate.hasKey(heartbeatKey);

            if (!Boolean.TRUE.equals(alive)) {
                // 心跳 key 已过期（30s 内未上报位置），视为假在线，强制下线
                // removeFromOnline 会从 GEO 集合移除并清理相关 Redis key
                driverLocationService.removeFromOnline(driver.getId(), "default");
                driver.setStatus(DriverStatus.OFFLINE);
                driverMapper.updateById(driver);
                cleanedCount++;
                log.info("[xxl-job] 假在线司机已清理 driverId={}", driver.getId());
            }
        }

        log.info("[xxl-job] 假在线清理完成，共清理 {} 名司机，扫描总数 {}", cleanedCount, onlineDrivers.size());
    }
}