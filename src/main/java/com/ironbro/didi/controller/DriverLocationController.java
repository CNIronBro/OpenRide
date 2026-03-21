package com.ironbro.didi.controller;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.service.DriverLocationService;
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

    /**
     * 4.1 司机上报位置
     *
     * 请求体：{ "lat": 39.99, "lng": 116.48, "timestamp": 1700000000000, "city": "default" }
     * timestamp 由客户端传入，用于乱序过滤；city 默认 "default"。
     */
    @PostMapping("/driver/location")
    public Result<Void> reportLocation(@RequestBody Map<String, Object> body, HttpSession session) {
        Long driverId = SessionUtils.getUserId(session);
        if (driverId == null) throw new BizException(401, "未登录");

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