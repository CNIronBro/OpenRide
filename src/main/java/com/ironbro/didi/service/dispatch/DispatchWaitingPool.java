package com.ironbro.didi.service.dispatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;

/**
 * 订单等待池
 *
 * 业务背景：
 * 自适应全局派单方案中，订单创建后不再立即触发 MQ 派单，而是先进入等待池。
 * GlobalDispatchScheduler 每 2s 触发一次 tick，从等待池取出所有待派订单，
 * 根据供需比决定走 KM 全局匹配还是贪心派单，统一执行后再推送给司机。
 *
 * Redis 数据结构：
 *   key:    order:waiting:pool
 *   type:   ZSET
 *   member: orderId（字符串）
 *   score:  订单进入等待池时的毫秒时间戳
 *
 * score 的两个用途：
 * 1. 按到达时间排序，保证先到先处理（FIFO 公平性）
 * 2. 调度器通过 score 计算等待时长，实现优先级加权（等待越久权重越高）
 *    以及兜底 TTL 清理（超过 STALE_THRESHOLD_MS 的订单视为异常，由调度器移除）
 *
 * 注意：ZSET 成员不支持单独设置 TTL，兜底清理由 GlobalDispatchScheduler 在每次 tick 时
 * 检查 score 是否超过 STALE_THRESHOLD_MS 来实现，而非依赖 Redis 原生 TTL 机制。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DispatchWaitingPool {

    /** 等待池 Redis key */
    public static final String WAITING_POOL_KEY = "order:waiting:pool";

    /**
     * 兜底 TTL（毫秒）：订单在等待池中超过此时间视为异常滞留。
     * 由 GlobalDispatchScheduler 在每次 tick 时检查并清理，防止等待池无限膨胀。
     * 设为 180s，覆盖正常派单链路的最大等待时间（120s 全局超时 + 60s 余量）。
     */
    public static final long STALE_THRESHOLD_MS = 180_000L;

    private final StringRedisTemplate redisTemplate;

    /**
     * 将订单加入等待池
     *
     * 使用当前时间戳作为 score，保证：
     * 1. 订单按到达顺序排列（score 越小越早到达）
     * 2. 调度器可通过 score 计算等待时长，用于优先级加权和兜底清理
     *
     * 调用时机：
     * - OrderService.createOrder 事务提交后（afterCommit 回调）
     * - ZombieOrderScanJob 发现僵尸订单后重新触发派单
     *
     * @param orderId 订单 ID
     */
    public void add(Long orderId) {
        double score = System.currentTimeMillis();
        redisTemplate.opsForZSet().add(WAITING_POOL_KEY, String.valueOf(orderId), score);
        log.info("订单加入等待池 orderId={} score={}", orderId, (long) score);
    }

    /**
     * 从等待池移除订单
     *
     * 调用时机：
     * 1. 订单被取消（CancelConsumer）—— 防止调度器再次尝试派单
     * 2. 调度器成功推送后（GlobalDispatchScheduler，阶段 3）—— 订单已进入派单流程
     * 3. 调度器发现订单状态异常时（GlobalDispatchScheduler 过滤逻辑）
     *
     * 幂等：若 orderId 不在池中，ZREM 返回 0，不报错。
     *
     * @param orderId 订单 ID
     */
    public void remove(Long orderId) {
        Long removed = redisTemplate.opsForZSet().remove(WAITING_POOL_KEY, String.valueOf(orderId));
        if (removed != null && removed > 0) {
            log.info("订单从等待池移除 orderId={}", orderId);
        }
    }

    /**
     * 获取等待池中所有订单（按 score 升序，即按进入等待池的时间排序）
     *
     * 供 GlobalDispatchScheduler（阶段 3）在每次 tick 时调用，
     * 取出所有待派订单统一参与全局匹配。
     *
     * 使用 rangeByScore(0, MAX_VALUE) 而非 range(0, -1)，
     * 语义更明确：取所有 score >= 0 的成员（即所有有效时间戳）。
     *
     * @return orderId 字符串集合（按进入等待池时间升序），池为空时返回空集合
     */
    public Set<String> getAllOrdered() {
        Set<String> result = redisTemplate.opsForZSet()
                .rangeByScore(WAITING_POOL_KEY, 0, Double.MAX_VALUE);
        return result != null ? result : Collections.emptySet();
    }

    /**
     * 获取订单在等待池中的 score（进入等待池时的毫秒时间戳）
     *
     * 供调度器计算订单等待时长，用于优先级加权：
     * 等待超过 30s 权重 1.2x，超过 60s 权重 1.5x（见设计文档 3.5 节）。
     *
     * @param orderId 订单 ID
     * @return 毫秒时间戳，若订单不在池中返回 null
     */
    public Double getScore(Long orderId) {
        return redisTemplate.opsForZSet().score(WAITING_POOL_KEY, String.valueOf(orderId));
    }
}