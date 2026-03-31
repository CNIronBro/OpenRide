package com.ironbro.didi.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.service.DriverLocationService;
import com.ironbro.didi.service.DriverService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 司机位置接口
 */
@RestController
@RequiredArgsConstructor
public class DriverLocationController {

    private final DriverLocationService locationService;
    private final DriverService driverService;
    private final OrderMapper orderMapper;
    private final ObjectMapper objectMapper;

    /**
     * 4.1 司机上报位置
     *
     * 请求体：{ "lat": 39.99, "lng": 116.48, "timestamp": 1700000000000, "city": "default" }
     * timestamp 由客户端传入，用于乱序过滤；city 默认 "default"。
     */
    @PostMapping("/driver/location")
    public Result<Void> reportLocation(@RequestBody Map<String, Object> body, HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        // GEO 中存储 driver.id（非 user_id），与派单查询保持一致
        Long driverId = driverService.getDriverByUserId(userId).getId();

        double lat  = ((Number) body.get("lat")).doubleValue();
        double lng  = ((Number) body.get("lng")).doubleValue();
        long   ts   = body.containsKey("timestamp")
                ? ((Number) body.get("timestamp")).longValue()
                : System.currentTimeMillis();
        String city = (String) body.getOrDefault("city", "default");

        locationService.reportLocation(driverId, lat, lng, ts, city);
        return Result.ok();
    }

    /**
     * 缓存路线规划结果
     *
     * 司机端前端在接单/开始行程时调用高德路线规划 API，拿到路线点后推送到此接口缓存。
     * 乘客端 /driver/location/current 接口从 Redis 读取这些路线点，用于贴路动画。
     *
     * 请求体：{ "orderId": 123, "segment": "toPickup", "points": [{lat,lng},...] }
     */
    @PostMapping("/route/cache")
    public Result<Void> cacheRoute(@RequestBody Map<String, Object> body, HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");

        Long orderId = ((Number) body.get("orderId")).longValue();
        String segment = (String) body.get("segment");

        try {
            // 将 points 列表序列化为 JSON 字符串存入 Redis
            String pointsJson = objectMapper.writeValueAsString(body.get("points"));
            locationService.cacheRoute(orderId, segment, pointsJson);
        } catch (Exception e) {
            throw new BizException(500, "路线缓存失败");
        }
        return Result.ok();
    }

    /**
     * 乘客端查询当前司机实时位置（用于行程中页地图展示）
     *
     * 通过 orderId 找到对应司机，再从 Redis driver:location:trusted:{driverId} 读取最新坐标。
     * 同时从 Redis route:{segment}:{orderId} 读取路线规划结果（由司机端接单时缓存），
     * 供前端在两个离散坐标之间做贴路 RAF 插值动画，避免司机 marker 直线穿越建筑。
     *
     * @param orderId 订单 ID（乘客只能查自己的订单）
     * @return { lat, lng, routePoints:[{lat,lng},...] }，司机未上报时返回 null
     */
    @GetMapping("/driver/location/current")
    public Result<Map<String, Object>> currentLocation(
            @RequestParam Long orderId,
            HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");

        Order order = orderMapper.selectById(orderId);
        if (order == null || order.getDriverId() == null) {
            return Result.ok(null);
        }

        double[] pos = locationService.getDriverPosition(order.getDriverId());
        if (pos == null) return Result.ok(null);

        // 根据订单状态决定读取哪段路线：接客阶段读 toPickup，行程中读 toDestination
        String segment = order.getStatus() != OrderStatus.IN_TRIP ? "toPickup" : "toDestination";
        String routeJson = locationService.getRoute(orderId, segment);

        Map<String, Object> result = new HashMap<>();
        result.put("lat", pos[0]);
        result.put("lng", pos[1]);

        if (routeJson != null) {
            try {
                // 将 JSON 字符串反序列化为 List<Map>，直接返回给前端
                List<Map<String, Double>> points = objectMapper.readValue(
                        routeJson, new TypeReference<List<Map<String, Double>>>() {});
                result.put("routePoints", points);
            } catch (Exception e) {
                result.put("routePoints", null);
            }
        } else {
            result.put("routePoints", null);
        }

        return Result.ok(result);
    }

    /**
     * 4.4 附近司机召回（供派单服务和乘客端展示使用）
     *
     * 参数：lat, lng, radius（km，默认5），city（默认default）
     */
    @GetMapping("/dispatch/nearby")
    public Result<List<Long>> nearby(
            @RequestParam double lat,
            @RequestParam double lng,
            @RequestParam(defaultValue = "5") double radius,
            @RequestParam(defaultValue = "default") String city) {
        return Result.ok(locationService.nearbyDrivers(lat, lng, radius, city, 20));
    }
}