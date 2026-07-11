package com.ironbro.didi.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ironbro.didi.enums.DispatchAction;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 派单操作日志实体
 * 对应 order_dispatch_log 表
 *
 */
@Data
@TableName("order_dispatch_log")
public class OrderDispatchLog {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long orderId;

    /** 被派单的司机 ID，系统自动取消时为 null */
    private Long driverId;

    /** 操作类型，见 DispatchAction 枚举 */
    private DispatchAction action;

    /** 备注（如取消原因、补偿触发原因） */
    private String remark;

    private LocalDateTime createdAt;
}