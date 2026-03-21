package com.ironbro.didi.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.enums.*;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.mapper.UserMapper;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 管理员接口
 *
 * 所有接口均校验 ADMIN 角色，非管理员返回 403。
 */
@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final DriverMapper driverMapper;
    private final OrderMapper  orderMapper;
    private final UserMapper   userMapper;

    // ----------------------------------------------------------------
    // 权限校验工具方法
    // ----------------------------------------------------------------

    private void checkAdmin(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        if (SessionUtils.getRole(session) != UserRole.ADMIN) throw new BizException(403, "无权限");
    }

    // ----------------------------------------------------------------
    // 3.5 司机列表（分页 + 状态筛选）
    // ----------------------------------------------------------------

    @GetMapping("/drivers")
    public Result<Page<Driver>> drivers(
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false)    String status,
            HttpSession session) {
        checkAdmin(session);

        LambdaQueryWrapper<Driver> wrapper = new LambdaQueryWrapper<>();
        if (status != null && !status.isBlank()) {
            wrapper.eq(Driver::getStatus, DriverStatus.valueOf(status));
        }
        return Result.ok(driverMapper.selectPage(new Page<>(page, size), wrapper));
    }

    // ----------------------------------------------------------------
    // 3.6 订单列表（分页 + 状态筛选）
    // ----------------------------------------------------------------

    @GetMapping("/orders")
    public Result<Page<Order>> orders(
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false)    String status,
            HttpSession session) {
        checkAdmin(session);

        LambdaQueryWrapper<Order> wrapper = new LambdaQueryWrapper<Order>()
                .orderByDesc(Order::getCreatedAt);
        if (status != null && !status.isBlank()) {
            wrapper.eq(Order::getStatus, OrderStatus.valueOf(status));
        }
        return Result.ok(orderMapper.selectPage(new Page<>(page, size), wrapper));
    }

    // ----------------------------------------------------------------
    // 3.7 资质审核（通过 / 拒绝）
    // ----------------------------------------------------------------

    /**
     * 审核司机资质
     *
     * 请求体：{ "action": "APPROVED" / "REJECTED" }
     * 通过后司机可上线接单；拒绝后司机需重新提交材料。
     */
    @PostMapping("/drivers/{id}/audit")
    public Result<Void> audit(@PathVariable Long id,
                              @RequestBody Map<String, String> body,
                              HttpSession session) {
        checkAdmin(session);

        Driver driver = driverMapper.selectById(id);
        if (driver == null) throw new BizException(404, "司机不存在");

        AuditStatus action = AuditStatus.valueOf(body.get("action"));
        if (action != AuditStatus.APPROVED && action != AuditStatus.REJECTED) {
            throw new BizException("action 只能为 APPROVED 或 REJECTED");
        }

        driver.setAuditStatus(action);
        driverMapper.updateById(driver);
        return Result.ok();
    }

    // ----------------------------------------------------------------
    // 封禁 / 解封司机
    // ----------------------------------------------------------------

    @PostMapping("/drivers/{id}/ban")
    public Result<Void> banDriver(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        Driver driver = driverMapper.selectById(id);
        if (driver == null) throw new BizException(404, "司机不存在");
        driver.setStatus(DriverStatus.BANNED);
        driverMapper.updateById(driver);
        return Result.ok();
    }

    @PostMapping("/drivers/{id}/unban")
    public Result<Void> unbanDriver(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        Driver driver = driverMapper.selectById(id);
        if (driver == null) throw new BizException(404, "司机不存在");
        driver.setStatus(DriverStatus.OFFLINE);
        driverMapper.updateById(driver);
        return Result.ok();
    }

    // ----------------------------------------------------------------
    // 封禁 / 解封乘客
    // ----------------------------------------------------------------

    @GetMapping("/passengers")
    public Result<Page<User>> passengers(
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "10") int size,
            HttpSession session) {
        checkAdmin(session);
        LambdaQueryWrapper<User> wrapper = new LambdaQueryWrapper<User>()
                .eq(User::getRole, UserRole.PASSENGER);
        return Result.ok(userMapper.selectPage(new Page<>(page, size), wrapper));
    }

    @PostMapping("/passengers/{id}/ban")
    public Result<Void> banPassenger(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        User user = userMapper.selectById(id);
        if (user == null) throw new BizException(404, "用户不存在");
        user.setStatus(UserStatus.BANNED);
        userMapper.updateById(user);
        return Result.ok();
    }

    @PostMapping("/passengers/{id}/unban")
    public Result<Void> unbanPassenger(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        User user = userMapper.selectById(id);
        if (user == null) throw new BizException(404, "用户不存在");
        user.setStatus(UserStatus.NORMAL);
        userMapper.updateById(user);
        return Result.ok();
    }

    // ----------------------------------------------------------------
    // 强制取消订单
    // ----------------------------------------------------------------

    @PostMapping("/orders/{id}/cancel")
    public Result<Void> cancelOrder(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        Order order = orderMapper.selectById(id);
        if (order == null) throw new BizException(404, "订单不存在");
        if (order.getStatus() == OrderStatus.FINISHED || order.getStatus() == OrderStatus.CANCELLED) {
            throw new BizException(400, "订单已结束，无法取消");
        }
        order.setStatus(OrderStatus.CANCELLED);
        order.setCancelBy("SYSTEM");
        order.setCancelReason("管理员强制取消");
        order.setCancelledAt(LocalDateTime.now());
        orderMapper.updateById(order);
        return Result.ok();
    }

    // ----------------------------------------------------------------
    // 3.8 统计看板数据
    // ----------------------------------------------------------------

    /**
     * 今日统计：订单量、营收、活跃司机数、活跃乘客数
     * 近 7 日订单量趋势（用于看板柱状图）
     * 近 7 日营收明细（用于财务统计页）
     */
    @GetMapping("/stats")
    public Result<Map<String, Object>> stats(HttpSession session) {
        checkAdmin(session);

        LocalDateTime todayStart = LocalDate.now().atStartOfDay();
        LocalDateTime todayEnd   = todayStart.plusDays(1);

        // 今日订单量
        long todayOrders = orderMapper.selectCount(
                new LambdaQueryWrapper<Order>()
                        .ge(Order::getCreatedAt, todayStart)
                        .lt(Order::getCreatedAt, todayEnd));

        // 今日营收（已完成订单的 actual_price 之和）
        // MyBatis-Plus 不直接支持 SUM，用 selectList 后 stream 求和
        BigDecimal todayRevenue = orderMapper.selectList(
                        new LambdaQueryWrapper<Order>()
                                .eq(Order::getStatus, OrderStatus.FINISHED)
                                .ge(Order::getFinishedAt, todayStart)
                                .lt(Order::getFinishedAt, todayEnd)
                                .select(Order::getActualPrice))
                .stream()
                .filter(o -> o.getActualPrice() != null)
                .map(Order::getActualPrice)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

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
                .stream()
                .map(Order::getPassengerId)
                .distinct()
                .count();

        // 近 7 日每日订单量（用于柱状图）
        Map<String, Long> dailyOrders = new HashMap<>();
        for (int i = 6; i >= 0; i--) {
            LocalDateTime dayStart = LocalDate.now().minusDays(i).atStartOfDay();
            LocalDateTime dayEnd   = dayStart.plusDays(1);
            long cnt = orderMapper.selectCount(
                    new LambdaQueryWrapper<Order>()
                            .ge(Order::getCreatedAt, dayStart)
                            .lt(Order::getCreatedAt, dayEnd));
            dailyOrders.put(LocalDate.now().minusDays(i).toString(), cnt);
        }

        // 近 7 日每日营收明细（用于财务统计页）
        Map<String, Map<String, Object>> dailyFinance = new HashMap<>();
        for (int i = 6; i >= 0; i--) {
            LocalDateTime dayStart = LocalDate.now().minusDays(i).atStartOfDay();
            LocalDateTime dayEnd   = dayStart.plusDays(1);
            String dateKey = LocalDate.now().minusDays(i).toString();

            long dayCnt = orderMapper.selectCount(
                    new LambdaQueryWrapper<Order>()
                            .ge(Order::getCreatedAt, dayStart)
                            .lt(Order::getCreatedAt, dayEnd));

            BigDecimal dayRevenue = orderMapper.selectList(
                            new LambdaQueryWrapper<Order>()
                                    .eq(Order::getStatus, OrderStatus.FINISHED)
                                    .ge(Order::getFinishedAt, dayStart)
                                    .lt(Order::getFinishedAt, dayEnd)
                                    .select(Order::getActualPrice))
                    .stream()
                    .filter(o -> o.getActualPrice() != null)
                    .map(Order::getActualPrice)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

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
        return Result.ok(result);
    }
}