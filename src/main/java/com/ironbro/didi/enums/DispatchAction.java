package com.ironbro.didi.enums;

/**
 * 派单日志操作类型枚举
 */
public enum DispatchAction {
    DISPATCHED,   // 已派单给某司机
    ACCEPTED,     // 司机接单成功
    REJECTED,     // 司机拒绝接单
    TIMEOUT,      // 司机 15s 未响应，超时
    COMPENSATED,  // xxl-job 补偿任务触发重新派单
    CANCELLED     // 订单最终取消（候选耗尽或人工取消）
}