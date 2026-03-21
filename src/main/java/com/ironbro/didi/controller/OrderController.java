package com.ironbro.didi.controller;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.service.OrderService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

/**
 * 订单控制器
 *
 * 阶段 5 接口：
 * - POST /order/create  乘客下单
 * - GET  /order/{id}    查询订单状态（乘客端轮询）
 *
 * 阶段 6 将补充：接单、行程状态流转、取消等接口。
 */
@RestController
@RequestMapping("/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * 乘客下单
     *
     * 前置条件：乘客已登录（session 中有 userId）
     * 成功后：订单状态为 DISPATCHING，MQ 开始异步派单
     */
    @PostMapping("/create")
    public Result<Order> createOrder(@RequestBody CreateOrderReq req, HttpSession session) {
        Long passengerId = SessionUtils.getUserId(session);
        if (passengerId == null) {
            throw new BizException(401, "请先登录");
        }

        OrderService.CreateOrderRequest serviceReq = new OrderService.CreateOrderRequest(
                req.originLat(),
                req.originLng(),
                req.originAddr(),
                req.destLat(),
                req.destLng(),
                req.destAddr(),
                req.estimatedPrice(),
                req.city() != null ? req.city() : "default"
        );

        Order order = orderService.createOrder(passengerId, serviceReq);
        return Result.ok(order);
    }

    /**
     * 查询订单状态
     *
     * 乘客端等待接单页通过轮询此接口感知订单状态变化（DISPATCHING → ACCEPTED → ...）
     */
    @GetMapping("/{id}")
    public Result<Order> getOrder(@PathVariable Long id, HttpSession session) {
        Long passengerId = SessionUtils.getUserId(session);
        if (passengerId == null) {
            throw new BizException(401, "请先登录");
        }
        return Result.ok(orderService.getOrder(id, passengerId));
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
}