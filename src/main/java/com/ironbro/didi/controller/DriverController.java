package com.ironbro.didi.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.enums.AuditStatus;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.service.DriverLocationService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 司机接口（个人信息、上下线）
 * 位置上报在阶段 4 实现（DriverLocationController）
 */
@RestController
@RequestMapping("/driver")
@RequiredArgsConstructor
public class DriverController {

    private final DriverMapper driverMapper;
    private final DriverLocationService locationService;

    /** 3.3 获取司机个人信息 */
    @GetMapping("/profile")
    public Result<Driver> profile(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");

        Driver driver = driverMapper.selectOne(
                new LambdaQueryWrapper<Driver>().eq(Driver::getUserId, userId));
        if (driver == null) throw new BizException(404, "司机信息不存在");
        return Result.ok(driver);
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

        boolean online = Boolean.TRUE.equals(body.get("online"));

        Driver driver = driverMapper.selectOne(
                new LambdaQueryWrapper<Driver>().eq(Driver::getUserId, userId));
        if (driver == null) throw new BizException(404, "司机信息不存在");

        if (online) {
            // 上线前必须通过资质审核
            if (driver.getAuditStatus() != AuditStatus.APPROVED) {
                throw new BizException(403, "资质审核未通过，无法上线");
            }
            if (driver.getStatus() == DriverStatus.BANNED) {
                throw new BizException(403, "账号已封禁");
            }
            driver.setStatus(DriverStatus.ONLINE);
            driver.setIdleSince(LocalDateTime.now());
        } else {
            // 行程中不允许主动下线
            if (driver.getStatus() == DriverStatus.IN_TRIP) {
                throw new BizException(400, "行程中无法下线");
            }
            driver.setStatus(DriverStatus.OFFLINE);
            // 4.5 主动下线：从 GEO 集合移除，清除心跳 key
            locationService.removeFromOnline(driver.getId(), "default");
        }

        driverMapper.updateById(driver);
        return Result.ok();
    }
}