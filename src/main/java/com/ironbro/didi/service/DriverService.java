package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.enums.AuditStatus;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.mapper.DriverMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * 司机服务
 *
 * 职责：
 * 1. 查询司机个人信息
 * 2. 司机上线 / 下线状态流转（含审核校验、封禁校验、行程中校验）
 */
@Service
@RequiredArgsConstructor
public class DriverService {

    private final DriverMapper driverMapper;
    private final DriverLocationService locationService;

    /**
     * 获取司机信息（按 userId 查询）
     *
     * @param userId session 中的 userId
     * @return 司机实体
     */
    public Driver getDriverByUserId(Long userId) {
        Driver driver = driverMapper.selectOne(
                new LambdaQueryWrapper<Driver>().eq(Driver::getUserId, userId));
        if (driver == null) throw new BizException(404, "司机信息不存在");
        return driver;
    }

    /**
     * 司机上线 / 下线
     *
     * 上线前置条件：
     * - 资质审核必须通过（AuditStatus.APPROVED）
     * - 账号未被封禁
     *
     * 下线前置条件：
     * - 行程中（IN_TRIP）不允许主动下线，防止乘客被抛单
     *
     * 下线副作用：
     * - 从 Redis GEO 集合移除，清除心跳 key（由 DriverLocationService 处理）
     *
     * @param userId session 中的 userId
     * @param online true=上线，false=下线
     */
    public void updateStatus(Long userId, boolean online) {
        Driver driver = getDriverByUserId(userId);

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
            // 行程中不允许主动下线，防止乘客被抛单
            if (driver.getStatus() == DriverStatus.IN_TRIP) {
                throw new BizException(400, "行程中无法下线");
            }
            driver.setStatus(DriverStatus.OFFLINE);
            // 主动下线：从 GEO 集合移除，清除心跳 key（阶段 4.5 实现）
            locationService.removeFromOnline(driver.getId(), "default");
        }

        driverMapper.updateById(driver);
    }
}