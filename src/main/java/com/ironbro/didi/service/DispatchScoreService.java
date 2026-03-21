package com.ironbro.didi.service;

import com.ironbro.didi.entity.Driver;
import lombok.AllArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 调度评分服务
 *
 * 职责：对 GEO 召回的候选司机列表进行多维度评分并排序，
 * 选出最优司机优先派单，保证派单公平性和服务质量。
 *
 * 评分公式（见 technical-design.md 4.1 节）：
 *   score = w1 * distance_score + w2 * idle_score + w3 * accept_rate_score + w4 * dispatch_penalty
 *
 *   distance_score    = 1 - (distance / max_distance)        // 越近分越高
 *   idle_score        = min(idle_minutes / 30, 1.0)          // 空闲越久分越高（上限 30 分钟）
 *   accept_rate_score = driver.acceptRate                    // 接单率（0~1）
 *   dispatch_penalty  = 1 - min(dispatchCountToday / 3, 1.0) // 今日被派次数越多分越低
 *
 *   权重：w1=0.4, w2=0.2, w3=0.3, w4=0.1
 *
 * 设计意图：
 * - 距离权重最高（0.4），保证乘客等待时间短
 * - 接单率权重次之（0.3），过滤掉频繁拒单的司机
 * - 空闲时长（0.2），让等待较久的司机优先获得订单，保证公平
 * - 派单惩罚（0.1），避免同一司机被反复派单，分散派单压力
 */
@Service
public class DispatchScoreService {

    /** 评分权重 */
    private static final double W_DISTANCE      = 0.4;    private static final double W_IDLE          = 0.2;
    private static final double W_ACCEPT_RATE   = 0.3;
    private static final double W_DISPATCH_PENALTY = 0.1;

    /** 空闲时长评分上限（分钟），超过此值 idle_score 固定为 1.0 */
    private static final double MAX_IDLE_MINUTES = 30.0;

    /**
     * 今日派单次数惩罚上限：达到 3 次后 dispatch_penalty 降为 0
     * 即今日已被派 3 次及以上的司机，派单惩罚维度得分为 0
     */
    private static final double MAX_DISPATCH_COUNT = 3.0;

    /**
     * 对候选司机列表进行评分并排序
     *
     * @param candidates  GEO 召回的候选司机列表（含距离信息）
     * @param maxDistance 召回半径（公里），用于归一化距离分
     * @return 按评分降序排列的司机 ID 列表（最优在前）
     */
    public List<Long> score(List<CandidateDriver> candidates, double maxDistance) {
        List<ScoredDriver> scored = new ArrayList<>();

        for (CandidateDriver candidate : candidates) {
            Driver driver = candidate.driver();
            double distKm = candidate.distanceKm();

            // 1. 距离分：越近越高，归一化到 [0, 1]
            double distanceScore = 1.0 - Math.min(distKm / maxDistance, 1.0);

            // 2. 空闲时长分：从 idleSince 到现在的分钟数，上限 30 分钟
            double idleMinutes = 0;
            if (driver.getIdleSince() != null) {
                idleMinutes = Duration.between(driver.getIdleSince(), LocalDateTime.now()).toMinutes();
            }
            double idleScore = Math.min(idleMinutes / MAX_IDLE_MINUTES, 1.0);

            // 3. 接单率分：直接使用 acceptRate（已是 0~1 范围）
            double acceptRateScore = driver.getAcceptRate() != null
                    ? driver.getAcceptRate().doubleValue() : 0.5; // 无数据时默认 0.5

            // 4. 派单惩罚分：今日被派次数越多，此维度得分越低
            // 设计意图：防止同一司机被反复派单，让更多司机有机会接单
            int dispatchCount = driver.getDispatchCountToday() != null ? driver.getDispatchCountToday() : 0;
            double dispatchPenalty = 1.0 - Math.min(dispatchCount / MAX_DISPATCH_COUNT, 1.0);

            // 加权求和
            double totalScore = W_DISTANCE * distanceScore
                    + W_IDLE * idleScore
                    + W_ACCEPT_RATE * acceptRateScore
                    + W_DISPATCH_PENALTY * dispatchPenalty;

            scored.add(new ScoredDriver(driver.getId(), totalScore));
        }

        // 按评分降序排列，最优司机排在最前
        scored.sort(Comparator.comparingDouble(ScoredDriver::score).reversed());

        return scored.stream().map(ScoredDriver::driverId).toList();
    }

    /**
     * 候选司机数据（GEO 召回结果 + 司机详情）
     *
     * @param driver     司机实体（含 acceptRate、idleSince、dispatchCountToday）
     * @param distanceKm 与下单位置的距离（公里）
     */
    public record CandidateDriver(Driver driver, double distanceKm) {}

    /** 评分结果（内部使用） */
    private record ScoredDriver(Long driverId, double score) {}
}