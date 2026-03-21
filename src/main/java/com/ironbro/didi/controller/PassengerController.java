package com.ironbro.didi.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.mapper.UserMapper;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 乘客接口
 */
@RestController
@RequestMapping("/passenger")
@RequiredArgsConstructor
public class PassengerController {

    private final UserMapper  userMapper;
    private final OrderMapper orderMapper;

    /** 获取乘客个人信息 */
    @GetMapping("/profile")
    public Result<User> profile(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        return Result.ok(userMapper.selectById(userId));
    }

    /** 历史行程列表（按下单时间倒序） */
    @GetMapping("/orders")
    public Result<Page<Order>> orders(
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "20") int size,
            HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");

        return Result.ok(orderMapper.selectPage(
                new Page<>(page, size),
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getPassengerId, userId)
                        .orderByDesc(Order::getCreatedAt)));
    }
}