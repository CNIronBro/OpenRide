package com.ironbro.didi.enums;

/**
 * 订单状态枚举
 *
 * 合法流转路径：
 *   PENDING → DISPATCHING → ACCEPTED → PICKING → IN_TRIP → FINISHED
 *                                                         ↘ CANCELLED（任意阶段均可取消）
 *
 * 不引入状态机框架，流转逻辑在 OrderService 中用 if 判断实现。
 */
public enum OrderStatus {
    PENDING,      // 已下单，等待进入派单队列
    DISPATCHING,  // 派单中（MQ 正在处理，等待司机接单）
    ACCEPTED,     // 司机已接单，前往接客点途中
    PICKING,      // 司机已到达接客点，等待乘客上车
    IN_TRIP,      // 行程中
    FINISHED,     // 行程结束，已计价
    CANCELLED     // 已取消（乘客主动取消 / 司机取消 / 系统自动取消）
}