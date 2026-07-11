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
 * 2. 治理：乱序过滤、漂移过滤（200m/2s）
 * 3. 附近司机召回：GEORADIUS
 * 4. 假在线检测：由 xxl-job FakeOnlineCleanJob 定期调用 removeFromOnline 完成清理
 *
 * 漂移过滤状态机：
 *
 *   NORMAL 态（ok_count key 不存在）：
 *     合理点 → 直接更新 trusted + 写 GEO
 *     漂移点 → 进入 DRIFTING 态，ok_count=0，清除 candidate
 *
 *   DRIFTING 态（ok_count key 存在）：
 *     漂移点 → 维持 DRIFTING 态，ok_count=0，清除 candidate
 *     第一个合理点（ok_count=0，相对 trusted 合理）→ 存入 candidate，ok_count=1，不写 GEO
 *     后续合理点（ok_count>=1）→ 与 candidate 比较（而非 trusted）：
 *       - 与 candidate 距离合理 → 恢复 NORMAL，更新 trusted，写 GEO，删除 ok_count/candidate
 *       - 与 candidate 距离超阈值 → candidate 更新为当前点，ok_count 重置为 1（滑动窗口）
 *
 * 关键设计决策：
 *   恢复时用 candidate 而非 trusted 作为比较基准。
 *   原因：trusted 冻结在漂移前位置，司机持续移动后任何新点相对 trusted 都超阈值，
 *   会陷入永远无法恢复的死锁（V4 的根本缺陷）。candidate 随合理点滑动，不会锁死。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DriverLocationService {

    /**
     * 漂移过滤阈值：2s 内移动超过 200m 视为漂移（约 360km/h）
     * 上报间隔 2s，阈值 200m（500m * 2/5）
     */
    private static final double MAX_DISTANCE_PER_INTERVAL_METERS = 200.0;

    /** 心跳 TTL（秒），超过此时间未上报视为假在线 */
    private static final int HEARTBEAT_TTL_SECONDS = 30;

    private final StringRedisTemplate redisTemplate;

    /**
     * 司机上报位置
     *
     * 漂移过滤采用 trusted + candidate 双坐标状态机（V5）：
     * - trusted：可信坐标，GEO 写入基准，不会被漂移点污染
     * - candidate：漂移恢复候选坐标，DRIFTING 态下用于相邻两点互比，
     *              避免与冻结的 trusted 比较导致"司机移动后永远无法恢复"的死锁
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
        String trustedKey    = "driver:location:trusted:" + driverId;
        String candidateKey  = "driver:location:candidate:" + driverId;
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

        String trustedPos = redisTemplate.opsForValue().get(trustedKey);
        boolean geoUpdated = false;

        if (trustedPos == null) {
            // 首次上报（或 trusted 过期）：无可信基准，直接写 GEO 并建立 trusted
            redisTemplate.opsForValue().set(trustedKey, lat + "," + lng, Duration.ofMinutes(5));
            redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), String.valueOf(driverId));
            geoUpdated = true;
            log.info("位置上报（首次），lat={}, lng={}", lat, lng);
        } else {
            double[] trusted = parsePos(trustedPos);
            double distFromTrusted = haversineMeters(trusted[0], trusted[1], lat, lng);
            String okCountStr = redisTemplate.opsForValue().get(okCountKey);

            if (okCountStr == null) {
                // ── NORMAL 态 ──
                if (distFromTrusted > MAX_DISTANCE_PER_INTERVAL_METERS) {
                    // 漂移点：进入 DRIFTING 态
                    redisTemplate.opsForValue().set(okCountKey, "0", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                    redisTemplate.delete(candidateKey);
                    log.warn("位置漂移，丢弃 driverId={} dist={}m，refLat={}, refLng={}, lat={}, lng={}",
                            driverId, (int) distFromTrusted, trusted[0], trusted[1], lat, lng);
                } else {
                    // 合理点：直接更新 trusted + 写 GEO
                    redisTemplate.opsForValue().set(trustedKey, lat + "," + lng, Duration.ofMinutes(5));
                    redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), String.valueOf(driverId));
                    geoUpdated = true;
                    log.info("位置上报，lat={}, lng={}", lat, lng);
                }
            } else {
                // ── DRIFTING 态 ──
                // 注意：此处不再与 trusted 比较。trusted 冻结在漂移前位置，
                // 司机持续移动后任何新点相对 trusted 都超阈值，会导致永远无法恢复的死锁。
                // 恢复逻辑完全基于相邻点互比（candidate 滑动窗口）。
                int okCount = Integer.parseInt(okCountStr);

                if (okCount == 0) {
                    // 第一个点：无条件存入 candidate，不与 trusted 比较
                    redisTemplate.opsForValue().set(candidateKey, lat + "," + lng, Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                    redisTemplate.opsForValue().set(okCountKey, "1", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                    log.debug("漂移恢复中（candidate 建立）driverId={} lat={}, lng={}", driverId, lat, lng);
                } else {
                    // 后续合理点：与 candidate 比较（滑动窗口，避免与冻结的 trusted 比较导致死锁）
                    String candidatePos = redisTemplate.opsForValue().get(candidateKey);
                    if (candidatePos == null) {
                        // candidate 意外过期（极端情况），重置状态重新积累
                        redisTemplate.opsForValue().set(okCountKey, "0", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                        log.debug("candidate 过期，重置漂移恢复状态 driverId={}", driverId);
                    } else {
                        double[] candidate = parsePos(candidatePos);
                        double distFromCandidate = haversineMeters(candidate[0], candidate[1], lat, lng);

                        if (distFromCandidate > MAX_DISTANCE_PER_INTERVAL_METERS) {
                            // 与 candidate 距离超阈值：滑动窗口，candidate 更新为当前点，重新积累
                            redisTemplate.opsForValue().set(candidateKey, lat + "," + lng, Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                            redisTemplate.opsForValue().set(okCountKey, "1", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
                            log.warn("漂移恢复中跳变，滑动 candidate driverId={} dist={}m", driverId, (int) distFromCandidate);
                        } else {
                            // 连续两个相邻合理点，恢复 NORMAL 态
                            redisTemplate.opsForValue().set(trustedKey, lat + "," + lng, Duration.ofMinutes(5));
                            redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), String.valueOf(driverId));
                            redisTemplate.delete(okCountKey);
                            redisTemplate.delete(candidateKey);
                            geoUpdated = true;
                            log.info("漂移恢复，位置已更新 driverId={} lat={}, lng={}", driverId, lat, lng);
                        }
                    }
                }
            }
        }

        // 所有路径都刷新心跳，避免漂移期间被 FakeOnlineCleanJob 误判为假在线
        redisTemplate.opsForValue().set(heartbeatKey, "1", Duration.ofSeconds(HEARTBEAT_TTL_SECONDS));
        return geoUpdated;
    }

    /**
     * 召回指定坐标附近的在线司机列表
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

    /**
     * 司机下线时清理 Redis 中的在线状态
     * 由 DriverController.updateStatus 在司机下线时调用
     */
    public void removeFromOnline(Long driverId, String city) {
        redisTemplate.opsForGeo().remove("driver:online:" + city, String.valueOf(driverId));
        redisTemplate.delete("driver:heartbeat:" + driverId);
        redisTemplate.delete("driver:location:trusted:" + driverId);
        redisTemplate.delete("driver:location:candidate:" + driverId);
        redisTemplate.delete("driver:location:ok_count:" + driverId);
        redisTemplate.delete("driver:location:ts:" + driverId);
    }

    /**
     * 缓存路线规划结果到 Redis
     *
     * 由司机端前端在接单/开始行程时调用高德路线规划 API 后，将路线点 JSON 推送到后端缓存。
     * 乘客端 /driver/location/current 接口从此处读取 routePoints，用于贴路动画。
     *
     * @param orderId    订单 ID（路线以订单为粒度缓存，避免同一司机多订单互相覆盖）
     * @param segment    路线段标识："toPickup" 或 "toDestination"
     * @param pointsJson 路线点 JSON 字符串，格式 [{lat,lng},...]
     */
    public void cacheRoute(Long orderId, String segment, String pointsJson) {
        // TTL 2h，覆盖任何合理的行程时长
        redisTemplate.opsForValue().set(
                "route:" + segment + ":" + orderId, pointsJson, Duration.ofHours(2));
    }

    /**
     * 读取缓存的路线规划结果
     *
     * @param orderId 订单 ID
     * @param segment "toPickup" 或 "toDestination"
     * @return 路线点 JSON 字符串，若未缓存返回 null
     */
    public String getRoute(Long orderId, String segment) {
        return redisTemplate.opsForValue().get("route:" + segment + ":" + orderId);
    }

    /**
     * 查询司机当前最新可信坐标（用于乘客端地图实时展示）
     *
     * 读取 trusted 坐标，避免漂移点导致乘客端地图跳变。
     *
     * @return 坐标数组 [lat, lng]，若 key 不存在返回 null
     */
    public double[] getDriverPosition(Long driverId) {
        String pos = redisTemplate.opsForValue().get("driver:location:trusted:" + driverId);
        if (pos == null) return null;
        return parsePos(pos);
    }

    // ----------------------------------------------------------------
    // 工具方法
    // ----------------------------------------------------------------

    private double[] parsePos(String pos) {
        String[] p = pos.split(",");
        return new double[]{Double.parseDouble(p[0]), Double.parseDouble(p[1])};
    }

    /**
     * Haversine 公式计算两点球面距离（米），用于漂移过滤中判断相邻两次上报的位移是否合理
     */
    private double haversineMeters(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6371000;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}