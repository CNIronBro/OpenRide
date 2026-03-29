package com.ironbro.didi.controller;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.service.DriverLocationService;
import com.ironbro.didi.service.DriverService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

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
     * 乘客端查询当前司机实时位置（用于行程中页地图展示）
     *
     * 通过 orderId 找到对应司机，再从 Redis driver:location:pos:{driverId} 读取最新坐标。
     * 乘客端行程中页每 5s 轮询一次，拿到坐标后更新高德地图 marker。
     *
     * @param orderId 订单 ID（乘客只能查自己的订单）
     * @return { lat, lng } 或 null（司机尚未上报位置）
     */
    @GetMapping("/driver/location/current")
    public Result<Map<String, Double>> currentLocation(
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

        return Result.ok(Map.of("lat", pos[0], "lng", pos[1]));
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