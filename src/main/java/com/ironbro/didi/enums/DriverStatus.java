package com.ironbro.didi.enums;

/**
 * 司机状态枚举
 */
public enum DriverStatus {
    OFFLINE,   // 下线（不接单）
    ONLINE,    // 在线（等待接单）
    IN_TRIP,   // 行程中
    BANNED     // 已封禁
}