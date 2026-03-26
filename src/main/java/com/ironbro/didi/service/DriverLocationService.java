package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.mapper.DriverMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 司机位置服务
 *
 * 职责：
 * 1. 位置上报：写入 Redis GEO + 刷新心跳 TTL
 * 2. 治理：频率控制（5s）、乱序过滤、漂移过滤（500m/5s）
 * 3. 附近司机召回：GEORADIUS
 * 4. 假在线检测：由 xxl-job FakeOnlineCleanJob 定期调用 removeFromOnline 完成清理
 *
 * Redis Key 设计（见 technical-design.md 4.5 节）：
 *   driver:online:{city}            ZSET(GEO)  在线司机地理位置
 *   driver:heartbeat:{driverId}     STRING     心跳，TTL=30s
 *   driver:location:last:{driverId} STRING     上次上报时间戳，TTL=10s（频率控制）
 *   driver:location:pos:{driverId}  STRING     上次上报坐标 "lat,lng"，用于漂移过滤
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DriverLocationService {

    /** 同一司机两次上报的最小间隔（秒） */
    private static final int THROTTLE_SECONDS = 5;

    /**
     * 漂移过滤阈值：5s 内移动超过 500m 视为漂移
     * 500m / 5s ≈ 360km/h，超过此速度的位移认为是 GPS 漂移
     */
    private static final double MAX_DISTANCE_PER_INTERVAL_METERS = 500.0;

    /** 心跳 TTL（秒），超过此时间未上报视为假在线 */
    private static final int HEARTBEAT_TTL_SECONDS = 30;

    private final StringRedisTemplate redisTemplate;
    private final DriverMapper driverMapper;

    // ----------------------------------------------------------------
    // 4.1~4.3 位置上报
    // ----------------------------------------------------------------

    /**
     * 司机上报位置
     *
     * @param driverId  司机 ID
     * @param lat       纬度
     * @param lng       经度
     * @param timestamp 客户端上报时间戳（毫秒），用于乱序过滤
     * @param city      城市标识，用于 GEO key 分片
     * @return false 表示被过滤（频率/乱序/漂移），true 表示写入成功
     */
    public boolean reportLocation(Long driverId, double lat, double lng, long timestamp, String city) {
        String throttleKey   = "driver:location:last:" + driverId;
        String heartbeatKey  = "driver:heartbeat:" + driverId;
        String lastPosKey    = "driver:location:pos:" + driverId;
        String geoKey        = "driver:online:" + city;

        // 4.1 频率控制：5s 内只处理一次
        // SET NX + TTL：若 key 已存在（上次上报在 5s 内），直接丢弃
        Boolean allowed = redisTemplate.opsForValue()
                .setIfAbsent(throttleKey, String.valueOf(timestamp), Duration.ofSeconds(THROTTLE_SECONDS));
        if (!Boolean.TRUE.equals(allowed)) {
            return false;
        }

        // 4.7 乱序过滤：时间戳早于上次处理时间，丢弃
        String lastTsStr = redisTemplate.opsForValue().get("driver:location:ts:" + driverId);
        if (lastTsStr != null && timestamp <= Long.parseLong(lastTsStr)) {
            log.debug("位置乱序，丢弃 driverId={} ts={}", driverId, timestamp);
            return false;
        }
        redisTemplate.opsForValue().set("driver:location:ts:" + driverId,
                String.valueOf(timestamp), Duration.ofMinutes(5));

        // 4.8 漂移过滤：与上次位置距离超过 500m/5s，丢弃
        // TODO 当前容易出现锁死风险，假如后续的位置都与上次位置发生了漂移，那么位置一直不能被更新。
        //  并且还要考虑到即使发生位置漂移，心跳ttl也要被刷新。
        String lastPos = redisTemplate.opsForValue().get(lastPosKey);
        if (lastPos != null) {
            String[] parts = lastPos.split(",");
            double lastLat = Double.parseDouble(parts[0]);
            double lastLng = Double.parseDouble(parts[1]);
            double distMeters = haversineMeters(lastLat, lastLng, lat, lng);
            if (distMeters > MAX_DISTANCE_PER_INTERVAL_METERS) {
                log.warn("位置漂移，丢弃 driverId={} dist={}m，lastLat={}, lastLng={}",
                        driverId, (int) distMeters, lastLat, lastLng);
                return false;
            }
        }
        // 更新上次坐标
        redisTemplate.opsForValue().set(lastPosKey, lat + "," + lng, Duration.ofMinutes(5));

        // 4.2 写入 Redis GEO
        // GEOADD driver:online:{city} lng lat driverId
        // 注意：Redis GEO 参数顺序是 lng, lat（经度在前）
        redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), String.valueOf(driverId));

        // 4.3 刷新心跳 TTL=30s
        redisTemplate.opsForValue().set(heartbeatKey, "1", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));

        return true;
    }

    // ----------------------------------------------------------------
    // 4.4 附近司机召回
    // ----------------------------------------------------------------

    /**
     * 召回指定坐标附近的在线司机列表
     *
     * 使用 GEORADIUS（Spring Data Redis 封装为 radius）
     * 返回按距离升序排列的司机 ID 列表
     *
     * @param lat        中心点纬度
     * @param lng        中心点经度
     * @param radiusKm   搜索半径（公里）
     * @param city       城市标识
     * @param maxResults 最多返回数量
     */
    public List<Long> nearbyDrivers(double lat, double lng, double radiusKm, String city, int maxResults) {
        String geoKey = "driver:online:" + city;

        // GEORADIUS 按距离升序，最多返回 maxResults 条
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = redisTemplate.opsForGeo().radius(
                geoKey,
                new Circle(new Point(lng, lat), new Distance(radiusKm, Metrics.KILOMETERS)),
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                        .sortAscending()
                        .limit(maxResults)
        );

        List<Long> driverIds = new ArrayList<>();
        if (results != null) {
            results.forEach(r -> driverIds.add(Long.parseLong(r.getContent().getName())));
        }
        return driverIds;
    }

    // ----------------------------------------------------------------
    // 4.5 司机主动下线：从 GEO 集合移除，清除心跳
    // ----------------------------------------------------------------

    /**
     * 司机下线时清理 Redis 中的在线状态
     * 由 DriverController.updateStatus 在司机下线时调用
     */
    public void removeFromOnline(Long driverId, String city) {
        redisTemplate.opsForGeo().remove("driver:online:" + city, String.valueOf(driverId));
        redisTemplate.delete("driver:heartbeat:" + driverId);
        redisTemplate.delete("driver:location:last:" + driverId);
        redisTemplate.delete("driver:location:pos:" + driverId);
        redisTemplate.delete("driver:location:ts:" + driverId);
    }

    // ----------------------------------------------------------------
    // 工具方法：Haversine 公式计算两点距离（米）
    // ----------------------------------------------------------------

    /**
     * 使用 Haversine 公式计算两个经纬度坐标之间的距离（米）
     * 用于漂移过滤中判断相邻两次上报的位移是否合理
     */
    private double haversineMeters(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6371000; // 地球半径（米）
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}