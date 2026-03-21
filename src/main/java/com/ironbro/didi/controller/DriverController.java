package com.ironbro.didi.controller;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.service.DriverService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 司机接口（个人信息、上下线）
 * 位置上报在阶段 4 实现（DriverLocationController）
 */
@RestController
@RequestMapping("/driver")
@RequiredArgsConstructor
public class DriverController {

    private final DriverService driverService;

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
}