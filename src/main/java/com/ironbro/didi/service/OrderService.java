package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.config.RabbitMqConfig;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.OrderDispatchLog;
import com.ironbro.didi.enums.DispatchAction;
import com.ironbro.didi.enums.DriverStatus;
import com.ironbro.didi.enums.OrderStatus;
import com.ironbro.didi.mapper.DriverMapper;
import com.ironbro.didi.mapper.OrderDispatchLogMapper;
import com.ironbro.didi.mapper.OrderMapper;
import com.ironbro.didi.service.dispatch.DispatchWaitingPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ironbro.didi.websocket.WebSocketSessionManager;
import com.ironbro.didi.websocket.WsMessage;
import java.math.BigDecimal;
import java.time.Duration;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 订单服务
 *
 * 职责：
 * 1. 乘客下单：写库 + 事务提交后写入 Redis 等待池（由 GlobalDispatchScheduler 统一调度）
 * 2. 查询订单状态（供乘客端轮询）
 * 3. 接单（CAS 乐观锁）、行程状态流转、取消等接口
 *
 * 派单入口变更说明（自适应全局派单改造）：
 * 原方案：createOrder 事务提交后直接发 MQ 到 dispatch.queue，DispatchConsumer 立即处理。
 * 新方案：createOrder 事务提交后写入 order:waiting:pool（Redis ZSET），
 *         GlobalDispatchScheduler 每 2s 统一取出所有待派订单，根据供需比决定 KM 或贪心匹配。
 * 好处：同一 tick 内多个订单可参与全局最优匹配，避免先到订单抢走后到订单唯一合适司机。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderMapper orderMapper;
    private final DriverMapper driverMapper;
    private final StringRedisTemplate redisTemplate;
    private final PricingService pricingService;
    private final WebSocketSessionManager wsSessionManager;
    private final DispatchWaitingPool waitingPool;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final OrderDispatchLogMapper dispatchLogMapper;

    /**
     * 乘客下单
     *
     * 业务流程：
     * 1. 构建订单实体，初始状态为 DISPATCHING（直接进入派单，跳过 PENDING 中间态）
     * 2. 写入数据库
     * 3. 事务提交后将 orderId 写入 Redis 等待池（order:waiting:pool ZSET）
     *
     * 状态说明：
     * 直接设为 DISPATCHING 而非 PENDING，是因为下单和写等待池在同一事务提交后完成，
     * 不存在"已下单但未进入派单流程"的中间状态需要区分。
     *
     * 派单入口说明：
     * 不再直接发 MQ，改为写入等待池。GlobalDispatchScheduler 每 2s 触发一次 tick，
     * 统一取出等待池中所有订单参与全局匹配，最多延迟约 2s 开始派单。
     *
     * @param passengerId 乘客 user_id
     * @param req         下单请求参数
     * @return 创建的订单
     */
    @Transactional
    public Order createOrder(Long passengerId, CreateOrderRequest req) {
        // 构建订单
        Order order = new Order();
        order.setPassengerId(passengerId);
        order.setStatus(OrderStatus.DISPATCHING);
        order.setOriginLat(req.originLat());
        order.setOriginLng(req.originLng());
        order.setOriginAddr(req.originAddr());
        order.setDestLat(req.destLat());
        order.setDestLng(req.destLng());
        order.setDestAddr(req.destAddr());
        order.setEstimatedPrice(req.estimatedPrice());
        order.setSurgeFactor(BigDecimal.ONE);
        order.setDispatchRetryCount(0);
        order.setVersion(0);
        order.setCreatedAt(LocalDateTime.now());

        orderMapper.insert(order);

        // 必须在事务提交后再写入等待池，否则调度器可能在事务提交前读到 orderId，
        // 查询订单时返回 null，导致订单被误判为无效而从等待池移除。
        // afterCommit() 在当前事务成功提交后才执行，保证调度器能读到已持久化的订单数据。
        final Long orderId = order.getId();

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                // 写入等待池，GlobalDispatchScheduler 每 2s 取出所有待派订单统一调度
                // 不再直接发 MQ，派单入口从"订单到达立即触发"改为"tick 统一批量处理"
                waitingPool.add(orderId);
            }
        });

        log.info("下单成功，orderId={} passengerId={}", order.getId(), passengerId);
        return order;
    }

    /**
     * 查询订单详情（乘客端轮询用）
     *
     * @param orderId     订单 ID
     * @param passengerId 乘客 ID（用于鉴权，防止越权查询他人订单）
     */
    public Order getOrder(Long orderId, Long passengerId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BizException("订单不存在");
        }
        if (!order.getPassengerId().equals(passengerId)) {
            throw new BizException(403, "无权查看此订单");
        }
        return order;
    }

    // ----------------------------------------------------------------
    // 阶段 6：行程状态流转
    // ----------------------------------------------------------------

    /**
     * 司机接单（CAS 乐观锁）
     *
     * 核心并发控制：
     * 使用 MyBatis-Plus @Version 乐观锁，底层 SQL 为：
     *   UPDATE `order` SET driver_id=?, status='ACCEPTED', version=version+1
     *   WHERE id=? AND status='DISPATCHING' AND version=?
     * 若 version 不匹配（已被其他司机接单），updateById 返回影响行数为 0，
     * MyBatis-Plus 会抛出 OptimisticLockerException，此处捕获后转为业务异常。
     *
     * 批量派单场景下的并发处理：
     * 同批 N 个司机并发接单，CAS 保证只有一个成功，其余 N-1 个收到"订单已被他人接走"。
     * 接单成功后主动清除同批其他司机的 pending key，让其弹窗尽快关闭（约 12s TTL 内）。
     *
     * 接单成功后副作用：
     * 1. 司机状态改为 IN_TRIP，从 GEO 在线集合移除（不再参与新派单）
     * 2. 清除接单司机的待接单通知 key（driver:pending:order:{driverId}）
     * 3. 清除同批其他司机的待接单通知 key
     *
     * @param orderId  订单 ID
     * @param driverId 司机的 driver.id（非 user_id）
     */
    @Transactional
    public Order acceptOrder(Long orderId, Long driverId, String routeKey) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");
        if (order.getStatus() != OrderStatus.DISPATCHING) {
            throw new BizException(409, "订单已被接单或已取消");
        }

        // CAS 更新：@Version 注解会自动追加 AND version=? 并自增
        // 若并发接单导致 version 不匹配，updateById 影响行数为 0，抛 OptimisticLockerException
        order.setDriverId(driverId);
        order.setStatus(OrderStatus.ACCEPTED);
        order.setAcceptedAt(LocalDateTime.now());
        // 将司机选择的预设路线标识写入订单，供前端贴路插值动画使用
        order.setRouteKey(routeKey);
        int rows = orderMapper.updateById(order);
        if (rows == 0) {
            // 乐观锁冲突：重新查询订单状态，给出更精确的错误提示
            // 批量派单场景下，同批多个司机并发接单，N-1 个会走到这里
            Order current = orderMapper.selectById(orderId);
            if (current != null && current.getStatus() == OrderStatus.ACCEPTED) {
                throw new BizException(409, "订单已被他人接走");
            }
            throw new BizException(409, "订单已被接单或已取消");
        }

        // 接单成功：清除同批其他司机的 pending key，让其弹窗尽快关闭
        // 读取当前批次号，再读批次 SET，过滤掉接单司机自身，批量删除其余司机的 pending key
        // 注意：此操作允许最终一致——批次 SET 可能已 TTL 过期，此时依赖 pending key 自身 12s TTL 自然过期兜底
        String batchIndexKey = "order:dispatch:batch:index:" + orderId;
        String batchIndexStr = redisTemplate.opsForValue().get(batchIndexKey);
        if (batchIndexStr != null) {
            String batchSetKey = "order:dispatch:batch:" + orderId + ":" + batchIndexStr;
            Set<String> batchMembers = redisTemplate.opsForSet().members(batchSetKey);
            if (batchMembers != null && !batchMembers.isEmpty()) {
                List<String> keysToDelete = batchMembers.stream()
                        .filter(id -> !id.equals(String.valueOf(driverId)))
                        .map(id -> "driver:pending:order:" + id)
                        .collect(Collectors.toList());
                if (!keysToDelete.isEmpty()) {
                    // delete(Collection) 底层发送单条 DEL key1 key2 ... 命令，比逐个删除高效
                    redisTemplate.delete(keysToDelete);
                    log.info("清除同批其他司机 pending key orderId={} keys={}", orderId, keysToDelete);
                }

                // WS 推送：通知同批其他司机立即关闭弹窗，并显示"已被他人接走"
                // 在已有的批次成员遍历基础上追加，不重复查询 Redis
                // 每个 driverId 是 driver.id，需转换为 userId 才能找到对应的 WS session
                batchMembers.stream()
                        .filter(id -> !id.equals(String.valueOf(driverId)))
                        .forEach(id -> {
                            try {
                                Driver otherDriver = driverMapper.selectById(Long.parseLong(id));
                                if (otherDriver != null) {
                                    wsSessionManager.sendToUser(otherDriver.getUserId(),
                                            new WsMessage("DISPATCH_CANCELLED",
                                                    Map.of("orderId", orderId, "reason", "TAKEN")));
                                }
                            } catch (Exception e) {
                                log.warn("WS 推送同批司机关闭弹窗失败 driverId={} orderId={}", id, orderId, e);
                            }
                        });
            }
        }

        // 接单成功：更新司机状态为 IN_TRIP，从 GEO 在线集合移除
        Driver driver = driverMapper.selectById(driverId);
        if (driver != null) {
            driver.setStatus(DriverStatus.IN_TRIP);
            driverMapper.updateById(driver);
            // 从 GEO 在线集合移除，行程中司机不参与新派单
            redisTemplate.opsForZSet().remove("driver:online:default", String.valueOf(driverId));
        }

        // 清除接单司机的待接单通知 key，避免司机端重复弹窗
        redisTemplate.delete("driver:pending:order:" + driverId);

        // WS 推送：通知乘客司机已接单，让乘客端立即跳转行程中页
        // payload 只含 orderId/driverId，前端收到后需再发一次 GET /order/{id} 拉取司机姓名、车牌等展示字段
        // passengerId 即乘客的 user_id，与 WS session 的 userId 一致
        try {
            wsSessionManager.sendToUser(order.getPassengerId(),
                    new WsMessage("ORDER_ACCEPTED", Map.of("orderId", orderId, "driverId", driverId)));
        } catch (Exception e) {
            // WS 推送失败不影响接单主流程，乘客端轮询兜底（最坏延迟 3s）
            log.warn("WS 推送接单通知失败，依赖乘客端轮询兜底 orderId={}", orderId, e);
        }

        log.info("司机接单成功 orderId={} driverId={}", orderId, driverId);
        return orderMapper.selectById(orderId);
    }

    /**
     * 司机到达接客点（ACCEPTED → PICKING）
     *
     * @param orderId  订单 ID
     * @param driverId 司机 driver.id（用于鉴权，防止越权操作他人订单）
     */
    /**
     * 司机到达接客点（ACCEPTED → PICKING）
     *
     * 副作用：向乘客推送 TRIP_STEP_CHANGED(PICKING)，让步骤条立即更新。
     *
     * @param orderId  订单 ID
     * @param driverId 司机 driver.id（用于鉴权，防止越权操作他人订单）
     */
    @Transactional
    public Order arrive(Long orderId, Long driverId) {
        Order order = getOrderForDriver(orderId, driverId);
        if (order.getStatus() != OrderStatus.ACCEPTED) {
            throw new BizException(400, "当前订单状态不允许此操作");
        }
        order.setStatus(OrderStatus.PICKING);
        orderMapper.updateById(order);

        // WS 推送步骤变更，让乘客端步骤条立即更新，无需等待轮询
        try {
            wsSessionManager.sendToUser(order.getPassengerId(),
                    new WsMessage("TRIP_STEP_CHANGED", Map.of("orderId", orderId, "status", "PICKING")));
        } catch (Exception e) {
            log.warn("WS 推送 TRIP_STEP_CHANGED(PICKING) 失败，依赖乘客端轮询兜底 orderId={}", orderId, e);
        }

        return order;
    }

    /**
     * 开始行程（PICKING → IN_TRIP）
     *
     * @param orderId  订单 ID
     * @param driverId 司机 driver.id
     */
    @Transactional
    public Order startTrip(Long orderId, Long driverId) {
        Order order = getOrderForDriver(orderId, driverId);
        if (order.getStatus() != OrderStatus.PICKING) {
            throw new BizException(400, "当前订单状态不允许此操作");
        }
        order.setStatus(OrderStatus.IN_TRIP);
        order.setStartedAt(LocalDateTime.now());
        orderMapper.updateById(order);

        // WS 推送步骤变更
        try {
            wsSessionManager.sendToUser(order.getPassengerId(),
                    new WsMessage("TRIP_STEP_CHANGED", Map.of("orderId", orderId, "status", "IN_TRIP")));
        } catch (Exception e) {
            log.warn("WS 推送 TRIP_STEP_CHANGED(IN_TRIP) 失败，依赖乘客端轮询兜底 orderId={}", orderId, e);
        }

        return order;
    }

    /**
     * 结束行程（IN_TRIP → FINISHED）
     *
     * 行程结束后调用 PricingService 计算真实价格：
     * - 距离：根据起终点坐标用 Haversine 公式估算（mock 场景下坐标固定，结果稳定）
     * - 时长：startedAt → finishedAt 的分钟数
     * - surge 系数：从 Redis 读取区域供需比实时计算
     *
     * 副作用：司机状态恢复为 ONLINE，等待下一单（不重新加入 GEO，
     * 司机需重新上报位置才会出现在 GEO 中，符合实际业务逻辑）。
     *
     * @param orderId  订单 ID
     * @param driverId 司机 driver.id
     */
    @Transactional
    public Order finishTrip(Long orderId, Long driverId) {
        Order order = getOrderForDriver(orderId, driverId);
        if (order.getStatus() != OrderStatus.IN_TRIP) {
            throw new BizException(400, "当前订单状态不允许此操作");
        }

        LocalDateTime finishedAt = LocalDateTime.now();
        order.setStatus(OrderStatus.FINISHED);
        order.setFinishedAt(finishedAt);

        // 计算行程时长（分钟），startedAt 为空时兜底用 10 分钟
        double durationMin = order.getStartedAt() != null
                ? Duration.between(order.getStartedAt(), finishedAt).toMinutes()
                : 10.0;

        // 计算行程距离（km），用 Haversine 公式估算起终点直线距离
        // mock 坐标场景下结果稳定，真实场景可替换为地图 API 返回的实际里程
        double distanceKm = haversineKm(
                order.getOriginLat().doubleValue(), order.getOriginLng().doubleValue(),
                order.getDestLat().doubleValue(),   order.getDestLng().doubleValue()
        );

        // 调用动态计价服务，计算含 surge 系数的最终价格
        BigDecimal actualPrice = pricingService.calculate("default", distanceKm, durationMin);
        BigDecimal surgeFactor = pricingService.getSurgeFactor("default");

        order.setActualPrice(actualPrice);
        order.setSurgeFactor(surgeFactor);
        orderMapper.updateById(order);

        // 司机行程结束，恢复 ONLINE 状态，等待下一单
        Driver driver = driverMapper.selectById(driverId);
        if (driver != null) {
            driver.setStatus(DriverStatus.ONLINE);
            driver.setIdleSince(LocalDateTime.now());
            driverMapper.updateById(driver);
        }

        log.info("行程结束 orderId={} driverId={} dist={}km dur={}min actualPrice={} surgeFactor={}",
                orderId, driverId, distanceKm, durationMin, actualPrice, surgeFactor);

        // WS 推送步骤变更，让乘客端立即跳转支付页，无需等待轮询
        // payload 含 actualPrice，乘客端可直接展示金额，无需再发一次 GET /order/{id}
        try {
            wsSessionManager.sendToUser(order.getPassengerId(),
                    new WsMessage("TRIP_STEP_CHANGED", Map.of(
                            "orderId", orderId,
                            "status", "FINISHED",
                            "actualPrice", actualPrice
                    )));
        } catch (Exception e) {
            log.warn("WS 推送 TRIP_STEP_CHANGED(FINISHED) 失败，依赖乘客端轮询兜底 orderId={}", orderId, e);
        }

        return order;
    }

    /**
     * 司机主动拒单（阶段 4）
     *
     * 业务流程：
     * 1. 校验当前司机确实持有该订单的 pending 通知（driver:pending:order:{driverId} 存在且值为 orderId）
     * 2. 清除当前司机的 pending key，释放该司机
     * 3. 递增版本号（order:dispatch:version:{orderId}），使当前已发出的延迟消息失效
     *    RetryConsumer 消费时会校验版本号，版本号不一致则忽略（阶段 5 改造后生效）
     * 4. 从候选列表取下一个未被推送过的司机，立即推送
     * 5. 若候选耗尽，发取消消息
     *
     * 即时重推的必要性：
     * 新方案每次只推 1 个司机，若拒单后不即时处理，乘客需等待整个 10s 超时窗口才能推下一个司机。
     * 即时重推确保拒单场景下的等待时间与批量方案无实质差距。
     *
     * @param orderId  订单 ID
     * @param driverId 拒单司机的 driver.id（非 user_id）
     */
    public void rejectOrder(Long orderId, Long driverId) {
        // 4.2 校验：当前司机确实持有该订单的 pending 通知
        // driver:pending:order:{driverId} 的值应为 orderId 字符串
        String pendingKey = "driver:pending:order:" + driverId;
        String pendingOrderId = redisTemplate.opsForValue().get(pendingKey);
        if (pendingOrderId == null || !pendingOrderId.equals(String.valueOf(orderId))) {
            // pending key 不存在或已过期（10s TTL），或值不匹配（司机持有的是另一个订单的通知）
            // 此时拒单无意义，直接返回（不报错，前端弹窗已关闭即可）
            log.info("拒单校验失败：司机未持有该订单的 pending 通知 driverId={} orderId={} pendingOrderId={}",
                    driverId, orderId, pendingOrderId);
            return;
        }

        // 校验订单状态：只有 DISPATCHING 状态才需要处理拒单
        // 若订单已被其他司机接单（ACCEPTED）或已取消，直接返回，避免产生无效操作
        Order order = orderMapper.selectById(orderId);
        if (order == null || order.getStatus() != OrderStatus.DISPATCHING) {
            log.info("拒单时订单状态已变更，忽略 orderId={} status={}",
                    orderId, order != null ? order.getStatus() : "null");
            // 仍需清除 pending key，避免司机端轮询到已无效的通知
            redisTemplate.delete(pendingKey);
            return;
        }

        // 4.3 清除当前司机的 pending key，释放该司机（不再参与本订单的等待窗口）
        redisTemplate.delete(pendingKey);

        // 记录拒单日志
        saveDispatchLog(orderId, driverId, DispatchAction.REJECTED, "司机主动拒单，即时重推下一候选");

        // 4.6 原子递增版本号，使当前已发出的延迟消息失效
        // 使用 Redis INCR 命令（原子操作），避免 GET+SET 的 TOCTOU 竞态：
        // 若两个请求并发（如司机快速双击），GET+SET 会导致两次都读到旧值，版本号只递增 1 而非 2。
        // INCR 保证每次调用都严格递增，并发安全。
        // RetryConsumer 消费时会校验版本号，版本号不一致则忽略（阶段 5 改造后完全生效）
        String versionKey = "order:dispatch:version:" + orderId;
        Long newVersion = redisTemplate.opsForValue().increment(versionKey);
        // 确保版本号 key 有 TTL（INCR 在 key 不存在时会创建，但不设置 TTL）
        // 仅在首次创建时设置 TTL，避免每次拒单都重置过期时间
        if (newVersion != null && newVersion == 1) {
            redisTemplate.expire(versionKey, Duration.ofMinutes(10));
        }
        log.info("拒单版本号递增 orderId={} newVersion={}", orderId, newVersion);

        // 4.4 读取候选列表
        String candidatesKey = "order:candidates:" + orderId;
        String candidatesJson = redisTemplate.opsForValue().get(candidatesKey);
        if (candidatesJson == null) {
            // 候选列表已过期（TTL 10min），无法即时重推
            // 不主动取消订单，让已发出的延迟消息自然到期，由 RetryConsumer 走扩圈逻辑
            log.warn("拒单后候选列表已过期，等待 RetryConsumer 扩圈兜底 orderId={}", orderId);
            return;
        }

        List<Long> candidates;
        try {
            candidates = objectMapper.readValue(candidatesJson, new TypeReference<>() {});
        } catch (Exception e) {
            log.error("候选列表解析失败 orderId={}", orderId, e);
            return;
        }

        // 4.5 取下一个未被推送过的候选司机
        // 跳过 order:dispatched:drivers:{orderId} 中已有的司机（防重推）
        Set<String> alreadyDispatched = redisTemplate.opsForSet()
                .members("order:dispatched:drivers:" + orderId);

        Long nextDriverId = null;
        int nextIndex = -1;
        for (int i = 0; i < candidates.size(); i++) {
            Long candidateId = candidates.get(i);
            boolean alreadySent = alreadyDispatched != null
                    && alreadyDispatched.contains(String.valueOf(candidateId));
            if (!alreadySent) {
                nextDriverId = candidateId;
                nextIndex = i;
                break;
            }
        }

        if (nextDriverId == null) {
            // 候选列表内所有司机均已推送过，无法即时重推
            // 不主动取消订单：RetryConsumer 的延迟消息 10s 后到期，会走 handleExhausted 扩圈逻辑，
            // 扩圈后若仍无司机才最终取消。拒单即时重推只负责在现有候选列表内快速切换，
            // 扩圈和最终取消属于超时重试的职责，不在此处越权处理。
            log.info("拒单后当前候选列表已耗尽，等待 RetryConsumer 扩圈 orderId={}", orderId);
            return;
        }

        // 4.6 有下一个候选：立即推送
        if (newVersion == null) newVersion = 1L; // 防御性兜底，正常不会为 null
        pushNextDriver(orderId, nextDriverId, nextIndex, newVersion.intValue());
        log.info("拒单即时重推成功 orderId={} prevDriverId={} nextDriverId={} nextIndex={}",
                orderId, driverId, nextDriverId, nextIndex);
    }

    /**
     * 推送下一个候选司机（拒单即时重推的核心推送逻辑）
     *
     * 与 GlobalDispatchScheduler.pushMatchResult 类似，但不从等待池移除订单（订单已不在等待池中）。
     *
     * 执行步骤：
     * 1. 写 driver:pending:order:{driverId}（TTL=10s）
     * 2. 更新 order:dispatch:current:index:{orderId}
     * 3. 将司机加入 order:dispatched:drivers:{orderId}（防重推）
     * 4. 递增 order:dispatch:batch:index:{orderId}，使现有 RetryConsumer 的幂等校验感知到新推送
     *    （阶段 5 改造 RetryConsumer 后改为版本号校验，此 key 可废弃）
     * 5. 发延迟消息（x-delay=10s），携带新 batchIndex 和版本号
     * 6. WS 推送派单通知
     *
     * @param orderId    订单 ID
     * @param driverId   下一个候选司机 ID
     * @param index      该司机在候选列表中的索引
     * @param version    当前版本号（已递增后的值），写入延迟消息供阶段 5 改造后的 RetryConsumer 校验
     */
    private void pushNextDriver(Long orderId, Long driverId, int index, int version) {
        // 写司机待接单通知（TTL=10s，与派单窗口一致）
        redisTemplate.opsForValue().set(
                "driver:pending:order:" + driverId,
                String.valueOf(orderId),
                Duration.ofSeconds(10));

        // 更新当前推送司机在候选列表中的索引
        redisTemplate.opsForValue().set(
                "order:dispatch:current:index:" + orderId,
                String.valueOf(index),
                Duration.ofMinutes(10));

        // 将司机加入已推送集合，防止重复推送
        redisTemplate.opsForSet().add("order:dispatched:drivers:" + orderId, String.valueOf(driverId));
        redisTemplate.expire("order:dispatched:drivers:" + orderId, Duration.ofMinutes(10));

        // 递增 batch:index，使现有 RetryConsumer 的幂等校验感知到这是一次新推送。
        // 背景：RetryConsumer 当前用 msgBatchIndex == currentBatchIndex 做幂等校验。
        // GlobalDispatchScheduler 初始写 batch:index=0，若拒单后仍发 batchIndex=0 的延迟消息，
        // RetryConsumer 消费时校验通过，会再次触发重试推送，产生多余操作。
        // 递增 batch:index 后，新延迟消息携带 newBatchIndex，RetryConsumer 消费时校验一致，
        // 但此时已是新一轮推送的超时检查，行为正确。
        // 阶段 5 改造 RetryConsumer 后改为版本号校验，此 key 可废弃。
        String batchIndexKey = "order:dispatch:batch:index:" + orderId;
        Long newBatchIndex = redisTemplate.opsForValue().increment(batchIndexKey);
        if (newBatchIndex != null && newBatchIndex == 1) {
            // key 不存在时 INCR 从 0 开始，首次创建时设置 TTL
            redisTemplate.expire(batchIndexKey, Duration.ofMinutes(10));
        }
        int batchIndex = newBatchIndex != null ? newBatchIndex.intValue() : 1;

        // 发延迟消息（x-delay=10s），携带新 batchIndex 和版本号
        Map<String, Object> retryMsg = new HashMap<>();
        retryMsg.put("orderId", orderId);
        retryMsg.put("batchIndex", batchIndex);  // 与 Redis 中的 batch:index 一致，RetryConsumer 幂等校验用
        retryMsg.put("version", version);         // 阶段 5 改造后使用
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_RETRY,
                retryMsg,
                m -> {
                    m.getMessageProperties().setHeader("x-delay", 10_000);
                    return m;
                });

        // WS 推送派单通知（Redis key 作为兜底，WS 失败时司机端轮询仍可感知）
        try {
            Driver driver = driverMapper.selectById(driverId);
            if (driver != null) {
                wsSessionManager.sendToUser(driver.getUserId(),
                        new WsMessage("DISPATCH_NOTIFY", Map.of("orderId", orderId)));
            }
        } catch (Exception e) {
            log.warn("WS 推送拒单重推通知失败，依赖司机端轮询兜底 driverId={} orderId={}", driverId, orderId, e);
        }

        saveDispatchLog(orderId, driverId, DispatchAction.DISPATCHED,
                "拒单即时重推 index=" + index + " batchIndex=" + batchIndex + " version=" + version);
    }

    /** 发送取消消息到 cancel.queue */
    private void sendCancelMessage(Long orderId, String reason) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("reason", reason);
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DISPATCH_EXCHANGE,
                RabbitMqConfig.ROUTING_DISPATCH_CANCEL,
                msg);
    }

    /** 记录派单日志 */
    private void saveDispatchLog(Long orderId, Long driverId, DispatchAction action, String remark) {
        OrderDispatchLog logEntry = new OrderDispatchLog();
        logEntry.setOrderId(orderId);
        logEntry.setDriverId(driverId);
        logEntry.setAction(action);
        logEntry.setRemark(remark);
        logEntry.setCreatedAt(LocalDateTime.now());
        dispatchLogMapper.insert(logEntry);
    }

    /**
     * 取消订单（乘客或司机主动取消）
     *
     * 允许取消的状态：DISPATCHING、ACCEPTED、PICKING
     * 行程中（IN_TRIP）不允许取消，需联系客服处理。
     *
     * 取消后副作用：
     * - 若司机已接单（status >= ACCEPTED），司机状态恢复为 ONLINE
     *
     * @param orderId   订单 ID
     * @param userId    操作者 user_id（乘客或司机的 user_id，用于鉴权）
     * @param cancelBy  取消方：PASSENGER / DRIVER
     * @param reason    取消原因
     */
    @Transactional
    public void cancelOrder(Long orderId, Long userId, String cancelBy, String reason) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");

        // 鉴权：乘客只能取消自己的订单；司机通过 driver.userId 关联
        if ("PASSENGER".equals(cancelBy) && !order.getPassengerId().equals(userId)) {
            throw new BizException(403, "无权操作此订单");
        }
        if ("DRIVER".equals(cancelBy)) {
            // 校验 userId 对应的司机是否是该订单的接单司机
            Driver driver = driverMapper.selectOne(
                    new LambdaQueryWrapper<Driver>().eq(Driver::getUserId, userId));
            if (driver == null || !driver.getId().equals(order.getDriverId())) {
                throw new BizException(403, "无权操作此订单");
            }
        }

        if (order.getStatus() == OrderStatus.IN_TRIP) {
            throw new BizException(400, "行程中无法取消，请联系客服");
        }
        if (order.getStatus() == OrderStatus.FINISHED || order.getStatus() == OrderStatus.CANCELLED) {
            throw new BizException(400, "订单已结束，无法取消");
        }

        // 记录取消前的状态，用于后续判断是否需要从等待池移除
        OrderStatus prevStatus = order.getStatus();

        order.setStatus(OrderStatus.CANCELLED);
        order.setCancelBy(cancelBy);
        order.setCancelReason(reason);
        order.setCancelledAt(LocalDateTime.now());
        orderMapper.updateById(order);

        // 若订单处于派单中，从等待池移除，防止调度器在下一个 tick 再次尝试派单
        // CancelConsumer 处理系统自动取消时也会调用 waitingPool.remove，两处均需覆盖
        if (prevStatus == OrderStatus.DISPATCHING) {
            waitingPool.remove(orderId);
        }

        // 若司机已接单，取消后恢复司机为 ONLINE
        if (order.getDriverId() != null) {
            Driver driver = driverMapper.selectById(order.getDriverId());
            if (driver != null && driver.getStatus() == DriverStatus.IN_TRIP) {
                driver.setStatus(DriverStatus.ONLINE);
                driver.setIdleSince(LocalDateTime.now());
                driverMapper.updateById(driver);
            }
        }

        log.info("订单取消 orderId={} cancelBy={} reason={}", orderId, cancelBy, reason);
    }

    /**
     * 司机端查询订单详情（用于新订单弹窗，不校验乘客身份）
     */
    public Order getOrderForDriverView(Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");
        return order;
    }

    /**
     * 查询订单（司机端鉴权）
     * 校验订单存在且 driverId 匹配，防止越权操作他人订单。
     */
    private Order getOrderForDriver(Long orderId, Long driverId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) throw new BizException("订单不存在");
        if (!driverId.equals(order.getDriverId())) throw new BizException(403, "无权操作此订单");
        return order;
    }

    /**
     * 下单请求参数
     *
     * @param originLat      起点纬度
     * @param originLng      起点经度
     * @param originAddr     起点地址
     * @param destLat        终点纬度
     * @param destLng        终点经度
     * @param destAddr       终点地址
     * @param estimatedPrice 预估价格
     * @param city           城市标识（用于 GEO 召回分片）
     */
    public record CreateOrderRequest(
            BigDecimal originLat,
            BigDecimal originLng,
            String originAddr,
            BigDecimal destLat,
            BigDecimal destLng,
            String destAddr,
            BigDecimal estimatedPrice,
            String city
    ) {}

    /**
     * Haversine 公式计算两点间球面距离（公里）
     *
     * 用于行程结束时估算里程，精度满足计价需求（误差 < 0.5%）。
     * 真实场景可替换为地图 API 返回的实际行驶里程。
     */
    public static double haversineKm(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6371.0; // 地球半径（km）
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}