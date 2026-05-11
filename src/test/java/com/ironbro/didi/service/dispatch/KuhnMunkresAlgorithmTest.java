package com.ironbro.didi.service.dispatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KuhnMunkresAlgorithm 单元测试
 *
 * 验证 KM 算法在各种矩阵规模和边界场景下的正确性。
 * 核心验证点：算法输出全局最优匹配，而非贪心最优。
 */
class KuhnMunkresAlgorithmTest {

    private KuhnMunkresAlgorithm km;

    @BeforeEach
    void setUp() {
        km = new KuhnMunkresAlgorithm();
    }

    /**
     * 场景 1：2×2 矩阵，验证全局最优 vs 贪心最优
     *
     * 收益矩阵：
     *         司机X  司机Y
     * 订单A [  10,   6  ]
     * 订单B [   9,   2  ]
     *
     * 贪心：订单A → 司机X（10），订单B → 司机Y（2），总收益 = 12
     * 全局最优：订单A → 司机Y（6），订单B → 司机X（9），总收益 = 15
     *
     * 这是设计文档中的典型反例：两个订单都最优选同一个司机X，
     * 贪心让A抢走X，B只能拿到次优的Y；KM 全局分配总收益更高。
     */
    @Test
    void testGlobalOptimalVsGreedy() {
        // 经典反例：贪心选 A→X(10), B→Y(2)=12；全局最优 A→Y(6), B→X(9)=15
        double[][] matrix = {
                {10, 6},  // 订单A：司机X=10, 司机Y=6
                { 9, 2}   // 订单B：司机X=9,  司机Y=2
        };

        int[] result = km.solve(matrix, 2, 2);

        // 全局最优：订单A → 司机Y(1)，订单B → 司机X(0)，总收益 = 6 + 9 = 15
        assertThat(result[0]).isEqualTo(1); // 订单A → 司机Y
        assertThat(result[1]).isEqualTo(0); // 订单B → 司机X

        // 验证总收益确实是全局最优
        double totalScore = matrix[0][result[0]] + matrix[1][result[1]];
        assertThat(totalScore).isEqualTo(15.0);
    }

    /**
     * 场景 2：订单数 < 司机数（2 订单 × 4 司机）
     *
     * 所有订单都应被匹配，且匹配到最优司机。
     * 多余的司机不参与匹配（result 数组长度 = 订单数）。
     */
    @Test
    void testFewerOrdersThanDrivers() {
        double[][] matrix = {
                {3, 1, 4, 2},  // 订单0：最优司机2（4）
                {2, 5, 1, 3}   // 订单1：最优司机1（5）
        };

        int[] result = km.solve(matrix, 2, 4);

        // 结果数组长度应等于订单数
        assertThat(result).hasSize(2);

        // 所有订单都应被匹配（不为 -1）
        assertThat(result[0]).isNotEqualTo(-1);
        assertThat(result[1]).isNotEqualTo(-1);

        // 两个订单不能匹配同一个司机
        assertThat(result[0]).isNotEqualTo(result[1]);

        // 验证全局最优：订单0 → 司机2（4），订单1 → 司机1（5），总收益 = 9
        // 其他组合：订单0→司机0(3)+订单1→司机1(5)=8，订单0→司机3(2)+订单1→司机1(5)=7 等均 < 9
        double totalScore = matrix[0][result[0]] + matrix[1][result[1]];
        assertThat(totalScore).isEqualTo(9.0);
    }

    /**
     * 场景 3：订单数 > 司机数（4 订单 × 2 司机）
     *
     * 只有 2 个订单能被匹配，另外 2 个订单 result[i] = -1。
     * 算法应将司机分配给收益最高的订单组合。
     */
    @Test
    void testMoreOrdersThanDrivers() {
        double[][] matrix = {
                {1, 2},  // 订单0
                {3, 4},  // 订单1
                {5, 6},  // 订单2
                {7, 8}   // 订单3
        };

        int[] result = km.solve(matrix, 4, 2);

        // 结果数组长度应等于订单数
        assertThat(result).hasSize(4);

        // 恰好有 2 个订单被匹配（result[i] != -1），2 个未匹配（result[i] == -1）
        long matchedCount = java.util.Arrays.stream(result).filter(r -> r != -1).count();
        assertThat(matchedCount).isEqualTo(2);

        // 被匹配的订单不能使用同一个司机
        int[] matched = java.util.Arrays.stream(result).filter(r -> r != -1).toArray();
        assertThat(matched[0]).isNotEqualTo(matched[1]);

        // 全局最优：订单3 → 司机1（8），订单2 → 司机0（5），总收益 = 13
        // 验证总收益是所有可能的 2 订单组合中最大的
        double totalScore = 0;
        for (int i = 0; i < 4; i++) {
            if (result[i] != -1) {
                totalScore += matrix[i][result[i]];
            }
        }
        assertThat(totalScore).isEqualTo(13.0);
    }

    /**
     * 场景 4：1×1 单元素矩阵
     *
     * 最简单的情况：1 个订单，1 个司机，直接匹配。
     */
    @Test
    void testSingleElement() {
        double[][] matrix = {{7.5}};

        int[] result = km.solve(matrix, 1, 1);

        assertThat(result).hasSize(1);
        assertThat(result[0]).isEqualTo(0); // 订单0 → 司机0
    }

    /**
     * 场景 5：全零矩阵
     *
     * 所有收益为 0，算法仍应返回合法匹配（不崩溃，不返回 -1）。
     * 全零时任意完美匹配都是最优，只验证匹配合法性。
     */
    @Test
    void testAllZeroMatrix() {
        double[][] matrix = {
                {0, 0, 0},
                {0, 0, 0},
                {0, 0, 0}
        };

        int[] result = km.solve(matrix, 3, 3);

        assertThat(result).hasSize(3);

        // 所有订单都应被匹配
        for (int r : result) {
            assertThat(r).isNotEqualTo(-1);
            assertThat(r).isBetween(0, 2);
        }

        // 匹配结果应是一个排列（每个司机只被匹配一次）
        java.util.Set<Integer> usedDrivers = new java.util.HashSet<>();
        for (int r : result) {
            assertThat(usedDrivers.add(r)).isTrue(); // 不重复
        }
    }

    /**
     * 场景 6：性能验证 —— 30 订单 × 100 司机，执行时间 < 50ms
     *
     * 验证在本系统预期规模下算法不会成为性能瓶颈。
     */
    @Test
    void testPerformance() {
        int orderCount = 30;
        int driverCount = 100;
        double[][] matrix = new double[orderCount][driverCount];
        java.util.Random rng = new java.util.Random(42);
        for (int i = 0; i < orderCount; i++) {
            for (int j = 0; j < driverCount; j++) {
                matrix[i][j] = rng.nextDouble() * 100;
            }
        }

        long start = System.currentTimeMillis();
        int[] result = km.solve(matrix, orderCount, driverCount);
        long elapsed = System.currentTimeMillis() - start;

        // 所有订单都应被匹配（司机数 > 订单数）
        assertThat(result).hasSize(orderCount);
        for (int r : result) {
            assertThat(r).isNotEqualTo(-1);
        }

        // 执行时间应 < 50ms
        assertThat(elapsed).isLessThan(50L);
    }
}