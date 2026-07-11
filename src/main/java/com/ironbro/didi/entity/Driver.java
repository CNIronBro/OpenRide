package com.ironbro.didi.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ironbro.didi.enums.AuditStatus;
import com.ironbro.didi.enums.DriverStatus;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 司机扩展信息实体，与 user 表 1:1 关联
 * 对应 driver 表
 */
@Data
@TableName("driver")
public class Driver {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 关联 user.id */
    private Long userId;

    private String name;
    private String plate;
    private String carModel;
    private String carColor;

    /** 司机状态：OFFLINE / ONLINE / IN_TRIP / BANNED */
    private DriverStatus status;

    /** 资质审核状态：PENDING / APPROVED / REJECTED */
    private AuditStatus auditStatus;

    /** 综合评分（1.00 ~ 5.00） */
    private BigDecimal rating;

    /** 接单率（0.0000 ~ 1.0000）*/
    private BigDecimal acceptRate;

    /** 最近一次变为空闲的时间 */
    private LocalDateTime idleSince;

    /** 最近一次被派单的时间 */
    private LocalDateTime lastDispatchAt;

    /** 今日被派单次数 */
    private Integer dispatchCountToday;
}