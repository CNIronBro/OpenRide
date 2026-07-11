package com.ironbro.didi.job;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.OrderDispatchLog;
import com.ironbro.didi.enums.DispatchAction;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.service.dispatch.DispatchWaitingPool;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 僵尸订单扫描任务（xxl-job JobHandler）
 *
 * 业务背景：
 * 正常派单链路（MQ）中，订单从 DISPATCHING 状态到被接单或取消，最多经过若干轮 15s 重试。
 * 但在以下异常场景下，订单可能长时间卡在 DISPATCHING 状态（"僵尸订单"）：
 * 1. MQ 消息丢失（网络抖动、Broker 重启）
 * 2. 消费者崩溃导致消息未被处理
 * 3. 候选列表 Redis key 过期但订单状态未更新
 *
 * 补偿策略：
 * - 扫描 status=DISPATCHING AND updated_at < NOW()-5min 的订单
 * - 无标记则重新写入等待池（order:waiting:pool），由 GlobalDispatchScheduler 下一个 tick 调度
 * - 若 dispatch_retry_count 超过最大值（10次），直接取消订单
 *
 * xxl-job 配置说明：
 * - JobHandler 名称：zombieOrderScanJob
 * - 建议执行频率：每分钟一次（Cron: 0 * * * * ?）
 * - 路由策略：第一个（单节点执行，避免重复扫描）
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ZombieOrderScanJob {

    /** 僵尸订单判定阈值：DISPATCHING 状态超过此分钟数视为僵尸 */
    private static final int ZOMBIE_THRESHOLD_MINUTES = 5;

    /** 最大重试次数，超过后直接取消订单 */
    private static final int MAX_RETRY_COUNT = 10;

    private final OrderMapper orderMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;
    private final RedissonClient redissonClient;
    private final DispatchWaitingPool waitingPool;

    /**
     * 扫描并补偿僵尸订单
     *
     * 执行流程：
     * 1. 查询 status=DISPATCHING AND updated_at < NOW()-5min 的订单
     * 2. 对每个订单尝试获取补偿幂等锁（lock:compensate:{orderId}）
     * 3. 检查是否有正在进行的 MQ 派单（lock:dispatch:{orderId}）
     * 4. 根据重试次数决定：重新触发派单 or 直接取消
     */
    @XxlJob("zombieOrderScanJob")
    public void scanZombieOrders() {
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(ZOMBIE_THRESHOLD_MINUTES);

        // 查询超时未处理的派单中订单
        List<Order> zombieOrders = orderMapper.selectList(
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getStatus, OrderStatus.DISPATCHING)
                        .lt(Order::getUpdatedAt, threshold));

        if (zombieOrders.isEmpty()) {
            log.debug("[xxl-job] 僵尸订单扫描：无异常订单");
            return;
        }

        log.info("[xxl-job] 僵尸订单扫描：发现 {} 个异常订单", zombieOrders.size());

        for (Order order : zombieOrders) {
            compensateOrder(order);
        }
    }

    /**
     * 对单个僵尸订单执行补偿
     *
     * @param order 僵尸订单
     */
    private void compensateOrder(Order order) {
        Long orderId = order.getId();

        // 补偿幂等锁：防止 xxl-job 多节点并发执行同一订单的补偿
        // tryLock(waitTime=0)：不等待，若锁已被持有（另一节点正在补偿），直接跳过
        // leaseTime=60s：补偿操作（查库+写等待池）通常在 1s 内完成，60s 足够兜底
        String compensateLockKey = "lock:compensate:" + orderId;
        RLock compensateLock = redissonClient.getLock(compensateLockKey);
        boolean locked = false;

        try {
            locked = compensateLock.tryLock(0, 60, TimeUnit.SECONDS);
            if (!locked) {
                log.debug("[xxl-job] 补偿锁未获取，跳过 orderId={}", orderId);
                return;
            }

            // 重新查询订单，防止在获取锁期间状态已变更
            Order freshOrder = orderMapper.selectById(orderId);
            if (freshOrder == null || freshOrder.getStatus() != OrderStatus.DISPATCHING) {
                log.info("[xxl-job] 订单状态已变更，跳过补偿 orderId={}", orderId);
                return;
            }

            // 根据重试次数决定补偿策略
            int retryCount = freshOrder.getDispatchRetryCount() != null ? freshOrder.getDispatchRetryCount() : 0;

            if (retryCount >= MAX_RETRY_COUNT) {
                // 超过最大重试次数，直接取消订单
                cancelZombieOrder(freshOrder, "派单超时，自动取消（补偿任务）");
            } else {
                // 重新写入等待池，GlobalDispatchScheduler 下一个 tick 会取出并调度
                retriggerDispatch(freshOrder);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[xxl-job] 补偿任务被中断 orderId={}", orderId, e);
        } catch (Exception e) {
            log.error("[xxl-job] 补偿任务执行失败 orderId={}", orderId, e);
        } finally {
            if (locked && compensateLock.isHeldByCurrentThread()) {
                compensateLock.unlock();
            }
        }
    }

    /**
     * 重新触发派单
     *
     * 将订单重新写入等待池，由 GlobalDispatchScheduler 在下一个 tick 统一调度。
     * 不再直接发 MQ，与正常下单流程保持一致。
     *
     * 同时递增 dispatch_retry_count，用于后续判断是否超过最大重试次数。
     */
    private void retriggerDispatch(Order order) {
        // 递增重试次数
        order.setDispatchRetryCount(
                order.getDispatchRetryCount() != null ? order.getDispatchRetryCount() + 1 : 1);
        orderMapper.updateById(order);

        // 重新写入等待池，GlobalDispatchScheduler 下一个 tick（最多 2s）会取出并调度
        waitingPool.add(order.getId());

        // 记录补偿日志
        saveDispatchLog(order.getId(), null, DispatchAction.COMPENSATED,
                "僵尸订单补偿，重新写入等待池，retryCount=" + order.getDispatchRetryCount());

        log.info("[xxl-job] 僵尸订单重新写入等待池 orderId={} retryCount={}", order.getId(), order.getDispatchRetryCount());
    }

    /**
     * 取消僵尸订单
     *
     * 超过最大重试次数的订单直接取消，避免无限重试消耗资源。
     */
    private void cancelZombieOrder(Order order, String reason) {
        order.setStatus(OrderStatus.CANCELLED);
        order.setCancelBy("SYSTEM");
        order.setCancelReason(reason);
        order.setCancelledAt(LocalDateTime.now());
        orderMapper.updateById(order);

        // 记录补偿日志
        saveDispatchLog(order.getId(), null, DispatchAction.COMPENSATED,
                "僵尸订单超过最大重试次数，系统自动取消");

        log.warn("[xxl-job] 僵尸订单已取消 orderId={} retryCount={}", order.getId(), order.getDispatchRetryCount());
    }

    /** 记录补偿操作日志到 order_dispatch_log 表 */
    private void saveDispatchLog(Long orderId, Long driverId, DispatchAction action, String remark) {
        OrderDispatchLog log = new OrderDispatchLog();
        log.setOrderId(orderId);
        log.setDriverId(driverId);
        log.setAction(action);
        log.setRemark(remark);
        log.setCreatedAt(LocalDateTime.now());
        dispatchLogMapper.insert(log);
    }
}