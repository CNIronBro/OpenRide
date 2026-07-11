package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * 乘客服务类
 *
 * 职责：
 * 1. 查询乘客个人信息
 * 2. 查询乘客历史订单（分页）
 */
@Service
@RequiredArgsConstructor
public class PassengerService {

    private final UserMapper  userMapper;
    private final OrderMapper orderMapper;

    /**
     * 获取乘客个人信息
     *
     * @param userId session 中的 userId
     * @return 用户实体
     */
    public User getProfile(Long userId) {
        User user = userMapper.selectById(userId);
        if (user == null) throw new BizException(404, "用户不存在");
        return user;
    }

    /**
     * 查询乘客历史订单（按下单时间倒序分页）
     *
     * @param userId 乘客 userId
     * @param page   页码（从 1 开始）
     * @param size   每页条数
     * @return 分页订单列表
     */
    public Page<Order> getOrders(Long userId, int page, int size) {
        return orderMapper.selectPage(
                new Page<>(page, size),
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getPassengerId, userId)
                        .orderByDesc(Order::getCreatedAt));
    }
}