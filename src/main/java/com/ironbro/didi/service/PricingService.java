package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.entity.PriceRule;
import com.ironbro.didi.mapper.PriceRuleMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 动态计价服务
 *
 * 计价公式（：
 *   基础价 = base_price + per_km * distance_km + per_min * duration_min
 *   surge 系数由供需比决定：
 *     surge_ratio = 区域订单量 / 区域空闲司机量
 *     ratio < 1.5  → factor = 1.0
 *     1.5 <= ratio < 2.0 → factor = 1.2
 *     ratio >= 2.0 → factor = 1.5（上限）
 *   最终价格 = 基础价 * factor，保留 2 位小数
 *
 * 供需数据来源：
 *   Redis key pricing:order:count:{region}  → 区域近期订单量（TTL=5min）
 *   Redis key pricing:driver:idle:{region}  → 区域空闲司机量（TTL=5min）
 *   若 Redis 中无数据，默认 surge factor = 1.0（不上浮）
 *
 * 计价规则来源：
 *   price_rule 表，按 region 查询；若无对应 region，回退到 "default" 规则。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PricingService {

    private final PriceRuleMapper priceRuleMapper;
    private final StringRedisTemplate redisTemplate;

    // Redis key 前缀（与技术设计文档 §4.5 保持一致）
    private static final String KEY_ORDER_COUNT  = "pricing:order:count:";
    private static final String KEY_DRIVER_IDLE  = "pricing:driver:idle:";

    /**
     * 计算行程最终价格
     *
     * @param region      区域标识（如 "default" / "beijing"），用于查询计价规则和供需数据
     * @param distanceKm  行程距离（公里）
     * @param durationMin 行程时长（分钟）
     * @return 最终价格（含 surge 系数），保留 2 位小数
     */
    public BigDecimal calculate(String region, double distanceKm, double durationMin) {
        PriceRule rule = getRule(region);
        BigDecimal base = calcBase(rule, distanceKm, durationMin);
        BigDecimal factor = getSurgeFactor(region);
        BigDecimal finalPrice = base.multiply(factor).setScale(2, RoundingMode.HALF_UP);

        log.info("计价 region={} dist={}km dur={}min base={} factor={} final={}",
                region, distanceKm, durationMin, base, factor, finalPrice);
        return finalPrice;
    }

    /**
     * 预估价格（下单前调用，不写库）
     *
     * 与 calculate 逻辑相同，但返回 EstimateResult 包含 surge 系数，
     * 供前端展示"高峰期价格上浮 x 倍"提示。
     */
    public EstimateResult estimate(String region, double distanceKm, double durationMin) {
        PriceRule rule = getRule(region);
        BigDecimal base = calcBase(rule, distanceKm, durationMin);
        BigDecimal factor = getSurgeFactor(region);
        BigDecimal price = base.multiply(factor).setScale(2, RoundingMode.HALF_UP);
        return new EstimateResult(price, factor, rule.getBasePrice(), rule.getPerKm(), rule.getPerMin());
    }

    /**
     * 获取当前区域的 surge 系数
     *
     * 从 Redis 读取区域订单量和空闲司机量，计算供需比，映射到系数。
     * 若 Redis 无数据（key 不存在），默认返回 1.0（不上浮），避免因数据缺失误伤乘客。
     *
     * 边界处理：
     * - 空闲司机量为 0 时，视为极度供不应求，直接返回最高系数 1.5
     * - 订单量为 0 时，供需比为 0，返回 1.0
     */
    public BigDecimal getSurgeFactor(String region) {
        String orderCountStr  = redisTemplate.opsForValue().get(KEY_ORDER_COUNT + region);
        String driverIdleStr  = redisTemplate.opsForValue().get(KEY_DRIVER_IDLE + region);

        if (orderCountStr == null || driverIdleStr == null) {
            // Redis 无供需数据，不上浮
            return BigDecimal.ONE;
        }

        int orderCount  = Integer.parseInt(orderCountStr);
        int driverIdle  = Integer.parseInt(driverIdleStr);

        if (orderCount == 0) return BigDecimal.ONE;
        if (driverIdle == 0) return new BigDecimal("1.5");

        double ratio = (double) orderCount / driverIdle;

        if (ratio < 1.5) {
            return BigDecimal.ONE;
        } else if (ratio < 2.0) {
            return new BigDecimal("1.2");
        } else {
            return new BigDecimal("1.5");
        }
    }

    // ----------------------------------------------------------------
    // 私有工具方法
    // ----------------------------------------------------------------

    /**
     * 基础计价：起步价 + 里程费 + 时长费
     * 不含 surge 系数，用于账单明细拆分展示
     */
    private BigDecimal calcBase(PriceRule rule, double distanceKm, double durationMin) {
        BigDecimal dist = BigDecimal.valueOf(distanceKm);
        BigDecimal dur  = BigDecimal.valueOf(durationMin);
        return rule.getBasePrice()
                .add(rule.getPerKm().multiply(dist))
                .add(rule.getPerMin().multiply(dur))
                .setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * 按 region 查询计价规则，找不到时回退到 "default"
     *
     * 回退逻辑：保证即使新区域未配置规则，系统也能正常计价，不抛异常。
     */
    private PriceRule getRule(String region) {
        PriceRule rule = priceRuleMapper.selectOne(
                new LambdaQueryWrapper<PriceRule>().eq(PriceRule::getRegion, region));
        if (rule == null && !"default".equals(region)) {
            rule = priceRuleMapper.selectOne(
                    new LambdaQueryWrapper<PriceRule>().eq(PriceRule::getRegion, "default"));
        }
        if (rule == null) {
            // 兜底：数据库无任何规则时使用硬编码默认值，避免 NPE
            rule = new PriceRule();
            rule.setRegion("default");
            rule.setBasePrice(new BigDecimal("10.00"));
            rule.setPerKm(new BigDecimal("2.50"));
            rule.setPerMin(new BigDecimal("0.50"));
        }
        return rule;
    }

    /**
     * 预估价格结果
     *
     * @param price     最终预估价格（含 surge）
     * @param surgeFactor surge 系数（1.0 / 1.2 / 1.5）
     * @param basePrice 起步价
     * @param perKm     每公里单价
     * @param perMin    每分钟单价
     */
    public record EstimateResult(
            BigDecimal price,
            BigDecimal surgeFactor,
            BigDecimal basePrice,
            BigDecimal perKm,
            BigDecimal perMin
    ) {}
}