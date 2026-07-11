package com.ironbro.didi.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import com.ironbro.didi.enums.OrderStatus;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单主表实体
 * 对应 order 表
 *
 */
@Data
@TableName("`order`")  // order 是 MySQL 保留字，需要反引号转义
public class Order {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 乘客 user_id */
    private Long passengerId;

    /** 接单司机 driver_id，接单前为 null */
    private Long driverId;

    /** 订单状态，流转逻辑见 OrderStatus 枚举注释 */
    private OrderStatus status;

    private BigDecimal originLat;
    private BigDecimal originLng;
    private String originAddr;

    private BigDecimal destLat;
    private BigDecimal destLng;
    private String destAddr;

    /** 下单时预估价格 */
    private BigDecimal estimatedPrice;

    /** 行程结束后实际价格，由 PricingService 计算写入 */
    private BigDecimal actualPrice;

    /**
     * 动态计价倍率（1.00 ~ 1.50）
     * 由供需比决定：surge_ratio < 1.5 → 1.0x，1.5~2.0 → 1.2x，>2.0 → 1.5x
     */
    private BigDecimal surgeFactor;

    /**
     * 已重试派单次数
     * MQ 延迟重试每轮 +1，超过候选列表长度后触发自动取消
     */
    private Integer dispatchRetryCount;

    /** 取消原因 */
    private String cancelReason;

    /** 取消方：PASSENGER / DRIVER / SYSTEM */
    private String cancelBy;

    /**
     * CAS 乐观锁版本号
     * MyBatis-Plus @Version 注解会在 update 时自动追加 AND version=? 条件并自增
     */
    @Version
    private Integer version;

    private String routeKey;

    private LocalDateTime createdAt;

    /** 最后更新时间，数据库 ON UPDATE CURRENT_TIMESTAMP 自动维护 */
    private LocalDateTime updatedAt;

    private LocalDateTime acceptedAt;
    private LocalDateTime startedAt;
    private LocalDateTime finishedAt;
    private LocalDateTime cancelledAt;
}