package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.enums.AuditStatus;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

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
    private final OrderMapper orderMapper;

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

    /**
     * 查询司机收入统计（今日 + 本周）
     *
     * 统计来源：order 表中 driver_id=? AND status=FINISHED 的已完成订单
     * - 今日收入：finishedAt >= 今日 00:00:00 的订单 actualPrice 之和
     * - 本周收入：finishedAt >= 本周一 00:00:00 的订单 actualPrice 之和
     * - 今日订单数：今日完成的订单数量
     * - 今日行程记录：今日完成的订单列表（用于前端展示）
     *
     * @param driverId driver.id
     * @return 收入统计结果
     */
    public IncomeResult getIncome(Long driverId) {
        LocalDateTime todayStart = LocalDate.now().atStartOfDay();
        // 本周一 00:00:00（ISO 周，周一为第一天）
        LocalDateTime weekStart = LocalDate.now()
                .with(java.time.DayOfWeek.MONDAY).atStartOfDay();

        // 查询今日完成的订单
        List<Order> todayOrders = orderMapper.selectList(
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getDriverId, driverId)
                        .eq(Order::getStatus, OrderStatus.FINISHED)
                        .ge(Order::getFinishedAt, todayStart));

        // 查询本周完成的订单（用于计算本周收入）
        BigDecimal weekIncome = orderMapper.selectList(
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getDriverId, driverId)
                        .eq(Order::getStatus, OrderStatus.FINISHED)
                        .ge(Order::getFinishedAt, weekStart))
                .stream()
                .map(o -> o.getActualPrice() != null ? o.getActualPrice() : BigDecimal.ZERO)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        BigDecimal todayIncome = todayOrders.stream()
                .map(o -> o.getActualPrice() != null ? o.getActualPrice() : BigDecimal.ZERO)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        return new IncomeResult(todayIncome, weekIncome, todayOrders.size(), todayOrders);
    }

    /**
     * 司机收入统计结果
     *
     * @param todayIncome  今日收入（元）
     * @param weekIncome   本周收入（元）
     * @param todayOrders  今日完成订单数
     * @param trips        今日行程记录列表
     */
    public record IncomeResult(
            BigDecimal todayIncome,
            BigDecimal weekIncome,
            int todayOrders,
            List<Order> trips
    ) {}
}