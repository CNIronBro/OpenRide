package com.ironbro.didi.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 计价规则实体，按区域配置
 * 对应 price_rule 表
 *
 * 最终价格计算：
 *   base_price + per_km * distance + per_min * duration，再乘以 surgeFactor
 */
@Data
@TableName("price_rule")
public class PriceRule {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 区域标识，如 beijing / shanghai / default */
    private String region;

    /** 起步价（元） */
    private BigDecimal basePrice;

    /** 每公里单价（元/km） */
    private BigDecimal perKm;

    /** 每分钟单价（元/min） */
    private BigDecimal perMin;

    private LocalDateTime updatedAt;
}