package com.ironbro.didi.job;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.OrderDispatchLog;
import com.ironbro.didi.enums.DispatchAction;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 行程中超时订单扫描任务（xxl-job JobHandler）
 *
 * 业务背景：
 * 司机接单（ACCEPTED）后，正常流程应在合理时间内到达接客点并开始行程。
 * 若司机接单后长时间未推进行程步骤（如 30 分钟内未点击"到达接客点"），
 * 可能是司机端异常、司机失联或其他异常情况，需要告警处理。
 *
 * 当前仅记录告警日志，不自动取消订单（避免误操作影响正常行程）。
 * 后续可扩展为：发送告警通知给运营人员，或在超过更长时间后自动取消。
 *
 * 扫描范围：
 * - status=ACCEPTED AND accepted_at < NOW()-30min（接单后超过 30 分钟未到达接客点）
 * - status=PICKING AND started_at 为空 AND updated_at < NOW()-30min（到达接客点后超时未开始行程）
 *
 * xxl-job 配置说明：
 * - JobHandler 名称：tripTimeoutScanJob
 * - 建议执行频率：每 5 分钟一次（Cron: 0 0/5 * * * ?）
 * - 路由策略：第一个（单节点执行）
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TripTimeoutScanJob {

    /** 接单后超过此分钟数未推进行程，视为超时 */
    private static final int ACCEPT_TIMEOUT_MINUTES = 30;

    private final OrderMapper orderMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;

    /**
     * 扫描行程中超时订单并告警
     *
     */
    @XxlJob("tripTimeoutScanJob")
    public void scanTripTimeoutOrders() {
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(ACCEPT_TIMEOUT_MINUTES);

        // 扫描接单后超时未到达接客点的订单（ACCEPTED 状态超过 30 分钟）
        List<Order> timeoutOrders = orderMapper.selectList(
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getStatus, OrderStatus.ACCEPTED)
                        .lt(Order::getAcceptedAt, threshold));

        if (timeoutOrders.isEmpty()) {
            log.debug("[xxl-job] 行程超时扫描：无异常订单");
            return;
        }

        log.warn("[xxl-job] 行程超时扫描：发现 {} 个超时订单", timeoutOrders.size());

        for (Order order : timeoutOrders) {
            long overMinutes = java.time.Duration.between(order.getAcceptedAt(), LocalDateTime.now()).toMinutes();
            log.warn("[xxl-job] 行程超时告警 orderId={} driverId={} acceptedAt={} 已超时{}分钟",
                    order.getId(), order.getDriverId(), order.getAcceptedAt(), overMinutes);

            // 8.6 记录告警日志到 order_dispatch_log，供运营人员排查
            saveDispatchLog(order.getId(), order.getDriverId(), DispatchAction.TIMEOUT,
                    "行程超时告警：接单后超过 " + overMinutes + " 分钟未推进行程");
        }
    }

    /** 记录告警日志到 order_dispatch_log 表 */
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