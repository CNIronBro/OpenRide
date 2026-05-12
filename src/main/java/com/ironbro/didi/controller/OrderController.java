package com.ironbro.didi.controller;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.enums.UserRole;
import com.ironbro.didi.service.DriverService;
import com.ironbro.didi.service.OrderService;
import com.ironbro.didi.service.PricingService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 订单控制器
 *
 * 阶段 5 接口：
 * - POST /order/create  乘客下单
 * - GET  /order/{id}    查询订单状态（乘客端轮询）
 *
 * 阶段 6 新增：
 * - POST /order/{id}/accept  司机接单（CAS 乐观锁）
 * - POST /order/{id}/arrive  司机到达接客点
 * - POST /order/{id}/start   开始行程
 * - POST /order/{id}/finish  结束行程
 * - POST /order/{id}/cancel  取消订单（乘客/司机）
 */
@RestController
@RequestMapping("/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final DriverService driverService;
    private final PricingService pricingService;

    /**
     * 乘客下单
     *
     * 前置条件：乘客已登录（session 中有 userId）
     * 成功后：订单状态为 DISPATCHING，MQ 开始异步派单
     */
    @PostMapping("/create")
    public Result<Order> createOrder(@RequestBody CreateOrderReq req, HttpSession session) {
        Long passengerId = SessionUtils.getUserId(session);
        if (passengerId == null) throw new BizException(401, "请先登录");

        OrderService.CreateOrderRequest serviceReq = new OrderService.CreateOrderRequest(
                req.originLat(), req.originLng(), req.originAddr(),
                req.destLat(), req.destLng(), req.destAddr(),
                req.estimatedPrice(), req.city() != null ? req.city() : "default"
        );
        return Result.ok(orderService.createOrder(passengerId, serviceReq));
    }

    /**
     * 查询订单状态
     *
     * 乘客端等待接单页通过轮询此接口感知订单状态变化（DISPATCHING → ACCEPTED → ...）
     */
    @GetMapping("/{id}")
    public Result<Order> getOrder(@PathVariable Long id, HttpSession session) {
        Long passengerId = SessionUtils.getUserId(session);
        if (passengerId == null) throw new BizException(401, "请先登录");
        return Result.ok(orderService.getOrder(id, passengerId));
    }

    /**
     * 司机接单（CAS 乐观锁）
     *
     * 并发场景：多个司机同时接同一订单，只有一个成功，其余返回 409。
     * 司机身份通过 session userId → driver.id 解析。
     */
    @PostMapping("/{id}/accept")
    public Result<Order> accept(@PathVariable Long id,
                                @RequestBody(required = false) Map<String, String> body,
                                HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "请先登录");
        // 通过 userId 查出 driver.id（接单操作使用 driver.id 而非 user_id）
        Long driverId = driverService.getDriverByUserId(userId).getId();
        String routeKey = (body != null) ? body.get("routeKey") : null;
        return Result.ok(orderService.acceptOrder(id, driverId, routeKey));
    }

    /** 司机到达接客点（ACCEPTED → PICKING） */
    @PostMapping("/{id}/arrive")
    public Result<Order> arrive(@PathVariable Long id, HttpSession session) {
        Long driverId = getDriverId(session);
        return Result.ok(orderService.arrive(id, driverId));
    }

    /** 开始行程（PICKING → IN_TRIP） */
    @PostMapping("/{id}/start")
    public Result<Order> start(@PathVariable Long id, HttpSession session) {
        Long driverId = getDriverId(session);
        return Result.ok(orderService.startTrip(id, driverId));
    }

    /** 结束行程（IN_TRIP → FINISHED） */
    @PostMapping("/{id}/finish")
    public Result<Order> finish(@PathVariable Long id, HttpSession session) {
        Long driverId = getDriverId(session);
        return Result.ok(orderService.finishTrip(id, driverId));
    }

    /**
     * 司机端查询订单详情（用于新订单弹窗展示）
     *
     * 与乘客端 GET /order/{id} 不同，此接口不校验 passengerId，
     * 仅校验订单存在且状态为 DISPATCHING（防止查询已过期订单）。
     */
    @GetMapping("/{id}/driver-view")
    public Result<Order> driverView(@PathVariable Long id, HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "请先登录");
        Order order = orderService.getOrderForDriverView(id);
        return Result.ok(order);
    }

    /**
     * 取消订单（乘客或司机主动取消）
     *
     * 请求体：{ "reason": "临时有事" }
     * 取消方由 session 中的 role 决定（PASSENGER / DRIVER）。
     */
    @PostMapping("/{id}/cancel")
    public Result<Void> cancel(@PathVariable Long id,
                               @RequestBody Map<String, String> body,
                               HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "请先登录");
        UserRole role = SessionUtils.getRole(session);
        String cancelBy = role == UserRole.PASSENGER ? "PASSENGER" : "DRIVER";
        orderService.cancelOrder(id, userId, cancelBy, body.getOrDefault("reason", ""));
        return Result.ok();
    }

    /**
     * 司机主动拒单（阶段 4）
     *
     * 拒单后立即同步推送候选列表中下一个司机，不等延迟消息超时。
     * 原来前端"忽略"按钮只是静默关闭弹窗，现在改为调用此接口，
     * 确保拒单场景下乘客等待时间不因 10s 超时窗口而延长。
     *
     * 校验：当前司机必须持有该订单的 pending 通知（driver:pending:order:{driverId} 存在且值为 orderId）。
     * 若 pending key 已过期（10s TTL），接口静默成功（弹窗已关闭，无需报错）。
     */
    @PostMapping("/{id}/reject")
    public Result<Void> reject(@PathVariable Long id, HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "请先登录");
        Long driverId = driverService.getDriverByUserId(userId).getId();
        orderService.rejectOrder(id, driverId);
        return Result.ok();
    }

    /** 从 session 中解析司机 driver.id（行程操作公共方法） */
    private Long getDriverId(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "请先登录");
        return driverService.getDriverByUserId(userId).getId();
    }

    /** 下单请求体 */
    public record CreateOrderReq(
            BigDecimal originLat,
            BigDecimal originLng,
            String originAddr,
            BigDecimal destLat,
            BigDecimal destLng,
            String destAddr,
            BigDecimal estimatedPrice,
            String city
    ) {}

    /**
     * 预估价格（下单前调用）
     *
     * 根据起终点坐标计算直线距离，结合区域计价规则和当前供需比，
     * 返回预估价格和 surge 系数，供前端展示"高峰期价格上浮"提示。
     *
     * 参数：originLat, originLng, destLat, destLng, region（可选，默认 default）
     */
    @GetMapping("/estimate")
    public Result<PricingService.EstimateResult> estimate(
            @RequestParam double originLat,
            @RequestParam double originLng,
            @RequestParam double destLat,
            @RequestParam double destLng,
            @RequestParam(defaultValue = "default") String region,
            HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "请先登录");

        // 用 Haversine 公式估算直线距离（km），时长按 40km/h 平均速度估算
        double distanceKm = OrderService.haversineKm(originLat, originLng, destLat, destLng);
        double durationMin = distanceKm / 40.0 * 60.0; // 40km/h 估算

        return Result.ok(pricingService.estimate(region, distanceKm, durationMin));
    }
}