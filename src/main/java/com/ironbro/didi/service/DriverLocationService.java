package com.ironbro.didi.service;

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
 * 2. 治理：乱序过滤、漂移过滤（500m/5s）
 * 3. 附近司机召回：GEORADIUS
 * 4. 假在线检测：由 xxl-job FakeOnlineCleanJob 定期调用 removeFromOnline 完成清理
 *
 * Redis Key 设计：
 *   driver:online:{city}                ZSET(GEO)  在线司机地理位置（空间检索用）
 *   driver:heartbeat:{driverId}         STRING     心跳，TTL=30s
 *   driver:location:ts:{driverId}       STRING     上次上报时间戳，用于乱序过滤
 *   driver:location:pos:{driverId}      STRING     最新原始坐标 "lat,lng"，每次上报都更新，漂移判断基准
 *   driver:location:trusted:{driverId}  STRING     最新可信坐标 "lat,lng"，仅连续合理点确认后更新，GEO 写入基准
 *   driver:location:ok_count:{driverId} STRING     漂移恢复计数；key 存在即代表处于漂移态，TTL=30s
 *
 * 漂移过滤状态机：
 *   NORMAL  态（ok_count key 不存在）：每个合理点直接更新 trusted + 写 GEO
 *   DRIFTING 态（ok_count key 存在）：需连续 2 个合理点才恢复到 NORMAL 并写 GEO
 *   任意漂移点：ok_count 重置为 0（进入/维持 DRIFTING 态），不写 GEO，但刷新心跳
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DriverLocationService {

    /**
     * 漂移过滤阈值：2s 内移动超过 200m 视为漂移（约 360km/h）
     * 上报间隔从 5s 缩短为 2s，阈值按比例同步调整（500m * 2/5 = 200m）
     */
    private static final double MAX_DISTANCE_PER_INTERVAL_METERS = 200.0;

    /** 漂移恢复所需的连续合理点数 */
    private static final int DRIFT_RECOVER_COUNT = 2;

    /** 心跳 TTL（秒），超过此时间未上报视为假在线 */
    private static final int HEARTBEAT_TTL_SECONDS = 30;

    private final StringRedisTemplate redisTemplate;

    /**
     * 司机上报位置
     *
     * 漂移过滤采用双坐标 + 状态机设计：
     * - pos（原始坐标）：每次上报都更新，作为下次漂移判断的基准
     * - trusted（可信坐标）：仅在连续合理点确认后更新，写入 GEO 的依据
     * - ok_count（恢复计数）：key 存在即代表处于漂移态；连续 DRIFT_RECOVER_COUNT 个合理点后删除该 key，恢复正常态
     *
     * 正常态：合理点 → 直接更新 trusted + 写 GEO
     * 漂移态：合理点 → ok_count+1，达到阈值才恢复；漂移点 → ok_count 重置为 0
     * 所有路径最终都刷新心跳，避免漂移期间被误判为假在线
     *
     * @param driverId  司机 ID
     * @param lat       纬度
     * @param lng       经度
     * @param timestamp 客户端上报时间戳（毫秒），用于乱序过滤
     * @param city      城市标识，用于 GEO key 分片
     * @return true 表示写入 GEO 成功，false 表示被过滤
     */
    public boolean reportLocation(Long driverId, double lat, double lng, long timestamp, String city) {
        String heartbeatKey  = "driver:heartbeat:" + driverId;
        String posKey        = "driver:location:pos:" + driverId;
        String trustedKey    = "driver:location:trusted:" + driverId;
        String okCountKey    = "driver:location:ok_count:" + driverId;
        String tsKey         = "driver:location:ts:" + driverId;
        String geoKey        = "driver:online:" + city;

        // 乱序过滤：时间戳早于上次处理时间，丢弃
        String lastTsStr = redisTemplate.opsForValue().get(tsKey);
        if (lastTsStr != null && timestamp <= Long.parseLong(lastTsStr)) {
            log.debug("位置乱序，丢弃 driverId={} ts={}", driverId, timestamp);
            return false;
        }
        redisTemplate.opsForValue().set(tsKey, String.valueOf(timestamp), Duration.ofMinutes(5));

        // pos 每次都更新，仅用于辅助判断（当前实际以 trusted 为主要基准）
        redisTemplate.opsForValue().set(posKey, lat + "," + lng, Duration.ofMinutes(5));

        // trusted 是漂移判断和 GEO 写入的基准
        // 用 trusted == null 而非 lastPos == null 判断首次上报：
        // pos 可能因 TTL 过期被清掉，但 trusted 仍存在，此时不应跳过漂移检测
        String trustedPos = redisTemplate.opsForValue().get(trustedKey);

        boolean geoUpdated = false;

        if (trustedPos == null) {
            // 首次上报（或 trusted 过期）：无可信基准，直接写 GEO 并建立 trusted
            redisTemplate.opsForValue().set(trustedKey, lat + "," + lng, Duration.ofMinutes(5));
            redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), String.valueOf(driverId));
            geoUpdated = true;
            log.info("位置上报（首次），lat={}, lng={}", lat, lng);
        } else {
            // 漂移判断：与 trusted（可信坐标）比较，trusted 不会被坏点污染
            double[] ref = parsePos(trustedPos);
            double distMeters = haversineMeters(ref[0], ref[1], lat, lng);

            if (distMeters > MAX_DISTANCE_PER_INTERVAL_METERS) {
                // 漂移：进入/维持漂移态，ok_count 重置为 0
                // TTL=30s：若 30s 内无任何上报，漂移态自动过期，下次上报重新评估
                redisTemplate.opsForValue().set(okCountKey, "0", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                log.warn("位置漂移，丢弃 driverId={} dist={}m，refLat={}, refLng={}, lat={}, lng={}",
                        driverId, (int) distMeters, ref[0], ref[1], lat, lng);
            } else {
                // 合理点：根据当前状态决定是否写 GEO
                String okCountStr = redisTemplate.opsForValue().get(okCountKey);

                if (okCountStr == null) {
                    // 正常态（ok_count key 不存在）：直接更新 trusted + 写 GEO
                    redisTemplate.opsForValue().set(trustedKey, lat + "," + lng, Duration.ofMinutes(5));
                    redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), String.valueOf(driverId));
                    geoUpdated = true;
                    log.info("位置上报，lat={}, lng={}", lat, lng);
                } else {
                    // 漂移恢复态：累积连续合理点计数
                    int okCount = Integer.parseInt(okCountStr) + 1;
                    if (okCount >= DRIFT_RECOVER_COUNT) {
                        // 连续 DRIFT_RECOVER_COUNT 个合理点，恢复正常态
                        redisTemplate.opsForValue().set(trustedKey, lat + "," + lng, Duration.ofMinutes(5));
                        redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), String.valueOf(driverId));
                        redisTemplate.delete(okCountKey); // 删除 key = 退出漂移态
                        geoUpdated = true;
                        log.info("漂移恢复，位置已更新 driverId={} lat={}, lng={}", driverId, lat, lng);
                    } else {
                        // 合理点不足，继续等待
                        redisTemplate.opsForValue().set(okCountKey, String.valueOf(okCount),
                                Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                        log.debug("漂移恢复中 driverId={} okCount={}/{}", driverId, okCount, DRIFT_RECOVER_COUNT);
                    }
                }
            }
        }

        // 所有路径都刷新心跳，避免漂移期间被 FakeOnlineCleanJob 误判为假在线
        redisTemplate.opsForValue().set(heartbeatKey, "1", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
        return geoUpdated;
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
        redisTemplate.delete("driver:location:pos:" + driverId);
        redisTemplate.delete("driver:location:trusted:" + driverId);
        redisTemplate.delete("driver:location:ok_count:" + driverId);
        redisTemplate.delete("driver:location:ts:" + driverId);
    }

    /**
     * 查询司机当前最新坐标（用于乘客端地图实时展示）
     *
     * 读取 driver:location:pos:{driverId}（每次上报都更新的原始坐标），
     * 而非 trusted 坐标，以保证实时性。漂移点虽不写 GEO，但 pos 仍会更新，
     * 前端地图 SDK 可自行做路线吸附修正。
     *
     * @return 坐标数组 [lat, lng]，若 key 不存在返回 null
     */
    public double[] getDriverPosition(Long driverId) {
        String pos = redisTemplate.opsForValue().get("driver:location:pos:" + driverId);
        if (pos == null) return null;
        String[] parts = pos.split(",");
        return new double[]{ Double.parseDouble(parts[0]), Double.parseDouble(parts[1]) };
    }

    // ----------------------------------------------------------------
    // 工具方法：Haversine 公式计算两点距离（米）
    // ----------------------------------------------------------------

    private double[] parsePos(String pos) {
        String[] p = pos.split(",");
        return new double[]{Double.parseDouble(p[0]), Double.parseDouble(p[1])};
    }

    /**
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