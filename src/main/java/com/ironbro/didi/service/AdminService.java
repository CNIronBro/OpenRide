package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.enums.*;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 管理员服务
 *
 * 职责：
 * 1. 司机/乘客/订单分页查询
 * 2. 司机资质审核（通过/拒绝）
 * 3. 司机/乘客封禁与解封
 * 4. 强制取消订单
 * 5. 统计看板数据（今日概览 + 近 7 日趋势）
 */
@Service
@RequiredArgsConstructor
public class AdminService {

    private final DriverMapper driverMapper;
    private final OrderMapper  orderMapper;
    private final UserMapper   userMapper;

    // ----------------------------------------------------------------
    // 分页查询
    // ----------------------------------------------------------------

    public Page<Driver> listDrivers(int page, int size, String status) {
        LambdaQueryWrapper<Driver> wrapper = new LambdaQueryWrapper<>();
        if (status != null && !status.isBlank()) {
            wrapper.eq(Driver::getStatus, DriverStatus.valueOf(status));
        }
        return driverMapper.selectPage(new Page<>(page, size), wrapper);
    }

    public Page<Order> listOrders(int page, int size, String status) {
        LambdaQueryWrapper<Order> wrapper = new LambdaQueryWrapper<Order>()
                .orderByDesc(Order::getCreatedAt);
        if (status != null && !status.isBlank()) {
            wrapper.eq(Order::getStatus, OrderStatus.valueOf(status));
        }
        return orderMapper.selectPage(new Page<>(page, size), wrapper);
    }

    public Page<User> listPassengers(int page, int size) {
        return userMapper.selectPage(
                new Page<>(page, size),
                new LambdaQueryWrapper<User>().eq(User::getRole, UserRole.PASSENGER));
    }

    // ----------------------------------------------------------------
    // 司机资质审核
    // ----------------------------------------------------------------

    /**
     * 审核司机资质
     *
     * 通过后司机可上线接单；拒绝后司机需重新提交材料。
     * action 只允许 APPROVED 或 REJECTED，其他值抛异常。
     *
     * @param driverId 司机 ID
     * @param action   审核结果（APPROVED / REJECTED）
     */
    public void auditDriver(Long driverId, String action) {
        Driver driver = driverMapper.selectById(driverId);
        if (driver == null) throw new BizException(404, "司机不存在");

        AuditStatus auditStatus = AuditStatus.valueOf(action);
        if (auditStatus != AuditStatus.APPROVED && auditStatus != AuditStatus.REJECTED) {
            throw new BizException("action 只能为 APPROVED 或 REJECTED");
        }
        driver.setAuditStatus(auditStatus);
        driverMapper.updateById(driver);
    }

    // ----------------------------------------------------------------
    // 司机封禁 / 解封
    // ----------------------------------------------------------------

    public void banDriver(Long driverId) {
        Driver driver = driverMapper.selectById(driverId);
        if (driver == null) throw new BizException(404, "司机不存在");
        driver.setStatus(DriverStatus.BANNED);
        driverMapper.updateById(driver);
    }

    public void unbanDriver(Long driverId) {
        Driver driver = driverMapper.selectById(driverId);
        if (driver == null) throw new BizException(404, "司机不存在");
        // 解封后置为 OFFLINE，司机需主动上线
        driver.setStatus(DriverStatus.OFFLINE);
        driverMapper.updateById(driver);
    }

    // ----------------------------------------------------------------
    // 乘客封禁 / 解封
    // ----------------------------------------------------------------

    public void banPassenger(Long userId) {
        User user = userMapper.selectById(userId);
        if (user == null) throw new BizException(404, "用户不存在");
        user.setStatus(UserStatus.BANNED);
        userMapper.updateById(user);
    }

    public void unbanPassenger(Long userId) {
        User user = userMapper.selectById(userId);
        if (user == null) throw new BizException(404, "用户不存在");
        user.setStatus(UserStatus.NORMAL);
        userMapper.updateById(user);
    }

    // ----------------------------------------------------------------
    // 强制取消订单
    // ----------------------------------------------------------------

    /**
     * 管理员强制取消订单
     *
     * 已完成或已取消的订单不允许再次取消。
     * 取消原因固定为"管理员强制取消"，cancelBy 标记为 "SYSTEM"。
     *
     * @param orderId 订单 ID
     */
    public void cancelOrder(Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException(404, "订单不存在");
        if (order.getStatus() == OrderStatus.FINISHED || order.getStatus() == OrderStatus.CANCELLED) {
            throw new BizException(400, "订单已结束，无法取消");
        }
        order.setStatus(OrderStatus.CANCELLED);
        order.setCancelBy("SYSTEM");
        order.setCancelReason("管理员强制取消");
        order.setCancelledAt(LocalDateTime.now());
        orderMapper.updateById(order);
    }

    // ----------------------------------------------------------------
    // 统计看板
    // ----------------------------------------------------------------

    /**
     * 统计看板数据
     *
     * 返回内容：
     * - todayOrders：今日订单量
     * - todayRevenue：今日营收（已完成订单 actual_price 之和）
     * - activeDrivers：当前活跃司机数（ONLINE + IN_TRIP）
     * - activePassengers：今日有订单的乘客去重数
     * - dailyOrders：近 7 日每日订单量（用于柱状图）
     * - dailyFinance：近 7 日每日财务明细（用于财务统计页）
     *
     * 注意：MyBatis-Plus 不直接支持 SUM 聚合，营收通过 selectList + stream 求和。
     * 数据量较小时可接受，后续可改为自定义 XML 查询优化。
     */
    public Map<String, Object> getStats() {
        LocalDateTime todayStart = LocalDate.now().atStartOfDay();
        LocalDateTime todayEnd   = todayStart.plusDays(1);

        // 今日订单量
        long todayOrders = orderMapper.selectCount(
                new LambdaQueryWrapper<Order>()
                        .ge(Order::getCreatedAt, todayStart)
                        .lt(Order::getCreatedAt, todayEnd));

        // 今日营收（已完成订单的 actual_price 之和）
        BigDecimal todayRevenue = sumRevenue(todayStart, todayEnd);

        // 活跃司机数（ONLINE + IN_TRIP）
        long activeDrivers = driverMapper.selectCount(
                new LambdaQueryWrapper<Driver>()
                        .in(Driver::getStatus, DriverStatus.ONLINE, DriverStatus.IN_TRIP));

        // 活跃乘客数（今日有订单的乘客去重）
        long activePassengers = orderMapper.selectList(
                        new LambdaQueryWrapper<Order>()
                                .ge(Order::getCreatedAt, todayStart)
                                .lt(Order::getCreatedAt, todayEnd)
                                .select(Order::getPassengerId))
                .stream().map(Order::getPassengerId).distinct().count();

        // 近 7 日每日订单量（用于柱状图）
        Map<String, Long> dailyOrders = new HashMap<>();
        // 近 7 日每日财务明细（用于财务统计页）
        Map<String, Map<String, Object>> dailyFinance = new HashMap<>();

        for (int i = 6; i >= 0; i--) {
            LocalDateTime dayStart = LocalDate.now().minusDays(i).atStartOfDay();
            LocalDateTime dayEnd   = dayStart.plusDays(1);
            String dateKey = LocalDate.now().minusDays(i).toString();

            long dayCnt = orderMapper.selectCount(
                    new LambdaQueryWrapper<Order>()
                            .ge(Order::getCreatedAt, dayStart)
                            .lt(Order::getCreatedAt, dayEnd));
            dailyOrders.put(dateKey, dayCnt);

            BigDecimal dayRevenue = sumRevenue(dayStart, dayEnd);
            // 平台抽成 20%，司机收入 80%
            BigDecimal platform = dayRevenue.multiply(new BigDecimal("0.20"));
            BigDecimal driver   = dayRevenue.multiply(new BigDecimal("0.80"));

            long cancelCnt = orderMapper.selectCount(
                    new LambdaQueryWrapper<Order>()
                            .eq(Order::getStatus, OrderStatus.CANCELLED)
                            .ge(Order::getCreatedAt, dayStart)
                            .lt(Order::getCreatedAt, dayEnd));

            Map<String, Object> dayData = new HashMap<>();
            dayData.put("orderCount",    dayCnt);
            dayData.put("revenue",       dayRevenue);
            dayData.put("platformShare", platform);
            dayData.put("driverShare",   driver);
            dayData.put("cancelRate",    dayCnt == 0 ? 0 : (double) cancelCnt / dayCnt);
            dailyFinance.put(dateKey, dayData);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("todayOrders",      todayOrders);
        result.put("todayRevenue",     todayRevenue);
        result.put("activeDrivers",    activeDrivers);
        result.put("activePassengers", activePassengers);
        result.put("dailyOrders",      dailyOrders);
        result.put("dailyFinance",     dailyFinance);
        return result;
    }

    /**
     * 查询指定时间段内已完成订单的营收总和
     *
     * MyBatis-Plus 无原生 SUM 支持，通过 selectList 只查 actual_price 字段后 stream 求和。
     * 仅查单列字段（.select(Order::getActualPrice)）可减少数据传输量。
     */
    private BigDecimal sumRevenue(LocalDateTime start, LocalDateTime end) {
        return orderMapper.selectList(
                        new LambdaQueryWrapper<Order>()
                                .eq(Order::getStatus, OrderStatus.FINISHED)
                                .ge(Order::getFinishedAt, start)
                                .lt(Order::getFinishedAt, end)
                                .select(Order::getActualPrice))
                .stream()
                .filter(o -> o.getActualPrice() != null)
                .map(Order::getActualPrice)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}