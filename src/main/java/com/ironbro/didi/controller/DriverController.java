package com.ironbro.didi.controller;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.service.DriverService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 司机接口（个人信息、上下线、待接单轮询）
 * 位置上报在阶段 4 实现（DriverLocationController）
 */
@RestController
@RequestMapping("/driver")
@RequiredArgsConstructor
public class DriverController {

    private final DriverService driverService;
    private final StringRedisTemplate redisTemplate;

    /** 3.3 获取司机个人信息 */
    @GetMapping("/profile")
    public Result<Driver> profile(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        return Result.ok(driverService.getDriverByUserId(userId));
    }

    /**
     * 3.4 司机上线 / 下线
     *
     * 请求体：{ "online": true/false }
     * 上线前校验审核状态，未通过审核不允许上线。
     * 下线时将状态置为 OFFLINE；位置从 GEO 集合移除在阶段 4 实现。
     */
    @PutMapping("/status")
    public Result<Void> updateStatus(@RequestBody Map<String, Boolean> body, HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        driverService.updateStatus(userId, Boolean.TRUE.equals(body.get("online")));
        return Result.ok();
    }

    /**
     * 6.12 司机端轮询待接单通知
     *
     * 司机端每 2s 轮询此接口，有待接单订单时返回 orderId，无则返回 null。
     *
     * Redis key：driver:pending:order:{driverId}，TTL=20s（由 DispatchConsumer 写入）
     * 司机接单或超时后 key 自动过期，无需主动删除。
     *
     * 返回：{ "orderId": 123 } 或 { "orderId": null }
     */
    @GetMapping("/pending-order")
    public Result<Map<String, Object>> pendingOrder(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        Long driverId = driverService.getDriverByUserId(userId).getId();

        String key = "driver:pending:order:" + driverId;
        String orderIdStr = redisTemplate.opsForValue().get(key);
        Long orderId = orderIdStr != null ? Long.parseLong(orderIdStr) : null;
        return Result.ok(Map.of("orderId", orderId != null ? orderId : ""));
    }

    /**
     * 9.9 司机收入统计
     *
     * 返回今日收入、本周收入、今日订单数、今日行程记录列表。
     * 数据来源：order 表中 driver_id=? AND status=FINISHED 的已完成订单。
     */
    @GetMapping("/income")
    public Result<DriverService.IncomeResult> income(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        Long driverId = driverService.getDriverByUserId(userId).getId();
        return Result.ok(driverService.getIncome(driverId));
    }
}