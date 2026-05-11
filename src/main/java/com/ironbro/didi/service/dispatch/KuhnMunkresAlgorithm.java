package com.ironbro.didi.service.dispatch;

import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Kuhn-Munkres（匈牙利）算法 —— 加权二部图最大权完美匹配
 *
 * 业务背景：
 * 在自适应全局派单场景中，同一个 tick 内可能有多个待派订单和多个候选司机。
 * 若每个订单独立贪心选最优司机，会出现"先到订单抢走后到订单唯一合适司机"的问题。
 * KM 算法将所有订单和司机建模为加权二部图，求解总收益最大的全局匹配，
 * 避免局部最优导致全局较差的结果。
 *
 * 算法原理（顶标法）：
 * 1. 构造 size×size 方阵（size = max(订单数, 司机数)），不足部分补 0（虚拟节点）
 * 2. 维护顶标 lx[i]（左侧/订单）和 ly[j]（右侧/司机）
 *    初始化：lx[i] = max(row[i])，ly[j] = 0
 *    顶标约束：lx[i] + ly[j] >= w[i][j]，等号成立时边 (i,j) 属于相等子图
 * 3. 对每个左侧节点 i，在相等子图中 DFS 寻找增广路
 *    找不到时，计算松弛量 delta = min(lx[u]+ly[v]-w[u][v])（u 已访问，v 未访问）
 *    调整顶标：已访问左侧节点 lx -= delta，已访问右侧节点 ly += delta
 *    重复直到找到增广路
 * 4. 最终 matchX[i] = j 表示订单 i 分配给司机 j
 *
 * 复杂度：O(n³)，n = max(订单数, 司机数)
 * 在本系统规模下（单次 tick 通常 < 30 订单，< 100 司机），毫秒级完成。
 *
 * 使用方式：
 * <pre>
 *   double[][] matrix = { {10, 6}, {9, 2} };  // matrix[订单][司机] = 收益
 *   int[] result = algorithm.solve(matrix, 2, 2);
 *   // 全局最优：result[0]=1（订单0→司机1），result[1]=0（订单1→司机0），总收益=15
 *   // result[i] = -1 表示订单 i 未被匹配（司机数 < 订单数时）
 * </pre>
 */
@Component
public class KuhnMunkresAlgorithm {

    /**
     * 求解加权二部图最大权匹配
     *
     * @param costMatrix  costMatrix[i][j] 表示订单 i 分配给司机 j 的收益（非负）
     * @param orderCount  实际订单数（左侧节点数）
     * @param driverCount 实际司机数（右侧节点数）
     * @return match 数组，match[i] = j 表示订单 i 匹配到司机 j；
     *         match[i] = -1 表示订单 i 未被匹配（司机数不足时）
     */
    public int[] solve(double[][] costMatrix, int orderCount, int driverCount) {
        // 补方阵：size = max(订单数, 司机数)，虚拟节点收益为 0
        int size = Math.max(orderCount, driverCount);
        double[][] w = new double[size][size];
        for (int i = 0; i < orderCount; i++) {
            for (int j = 0; j < driverCount; j++) {
                w[i][j] = costMatrix[i][j];
            }
        }

        // 顶标初始化：lx[i] = 该行最大值，ly[j] = 0
        double[] lx = new double[size];
        double[] ly = new double[size];
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                lx[i] = Math.max(lx[i], w[i][j]);
            }
        }

        // matchX[i] = j：左侧节点 i 当前匹配的右侧节点（-1 表示未匹配）
        // matchY[j] = i：右侧节点 j 当前匹配的左侧节点（-1 表示未匹配）
        int[] matchX = new int[size];
        int[] matchY = new int[size];
        Arrays.fill(matchX, -1);
        Arrays.fill(matchY, -1);

        // 对每个左侧节点（订单）寻找增广路
        for (int i = 0; i < size; i++) {
            // 每次为节点 i 寻找增广路时，反复尝试直到成功
            while (true) {
                boolean[] visitX = new boolean[size];
                boolean[] visitY = new boolean[size];

                if (dfs(i, w, lx, ly, matchX, matchY, visitX, visitY)) {
                    break; // 找到增广路，处理下一个左侧节点
                }

                // 未找到增广路，计算松弛量 delta
                // delta = min(lx[u] + ly[v] - w[u][v])，u 已访问，v 未访问
                // 含义：使相等子图至少新增一条边所需的最小顶标调整量
                // 注意：delta 理论上 > 0（顶标约束保证 lx[u]+ly[v] >= w[u][v]），
                // 若因浮点误差导致 delta <= 0，强制取 EPS 防止顶标不变而死循环
                double delta = Double.MAX_VALUE;
                for (int u = 0; u < size; u++) {
                    if (!visitX[u]) continue;
                    for (int v = 0; v < size; v++) {
                        if (!visitY[v]) {
                            delta = Math.min(delta, lx[u] + ly[v] - w[u][v]);
                        }
                    }
                }
                if (delta < 1e-9) delta = 1e-9;

                // 调整顶标：已访问左侧节点 lx -= delta，已访问右侧节点 ly += delta
                // 效果：相等子图中已有的边保持不变（lx[u]-delta + ly[v]+delta = lx[u]+ly[v]），
                //       同时至少新增一条边（delta 最小的那条边满足 lx[u]+ly[v]-w[u][v] == 0）
                for (int u = 0; u < size; u++) {
                    if (visitX[u]) lx[u] -= delta;
                    if (visitY[u]) ly[u] += delta;
                }
                // 注意：不 break，继续 while 循环，用新的顶标重新 DFS
            }
        }

        // 构造结果：只返回实际订单的匹配结果
        // matchX[i] >= driverCount 表示匹配到虚拟司机，即未匹配
        int[] result = new int[orderCount];
        for (int i = 0; i < orderCount; i++) {
            int matched = matchX[i];
            result[i] = (matched >= 0 && matched < driverCount) ? matched : -1;
        }
        return result;
    }

    /**
     * DFS 寻找增广路
     *
     * 从左侧节点 u 出发，在相等子图（lx[u] + ly[v] ≈ w[u][v]）中寻找增广路。
     * 若找到，沿增广路翻转匹配关系。
     *
     * 关键：标记 visitX[u] = true，防止同一左侧节点在一次增广中被重复访问，
     * 避免死循环（当匹配链中存在环时）。
     *
     * @param u      当前左侧节点
     * @param visitX 本轮已访问的左侧节点（防止重复访问，避免死循环）
     * @param visitY 本轮已访问的右侧节点（防止重复访问）
     * @return true=找到增广路并完成匹配翻转；false=未找到
     */
    private boolean dfs(int u, double[][] w, double[] lx, double[] ly,
                        int[] matchX, int[] matchY,
                        boolean[] visitX, boolean[] visitY) {
        visitX[u] = true;
        int size = w.length;

        for (int v = 0; v < size; v++) {
            if (visitY[v]) continue;

            // 只走相等子图中的边（顶标之和等于边权，允许浮点误差 1e-6）
            double gap = lx[u] + ly[v] - w[u][v];
            if (gap > 1e-6) continue;

            visitY[v] = true;

            // 若右侧节点 v 未匹配，或其当前匹配的左侧节点能找到其他增广路
            int prevMatch = matchY[v];
            if (prevMatch == -1 || (!visitX[prevMatch] &&
                    dfs(prevMatch, w, lx, ly, matchX, matchY, visitX, visitY))) {
                matchX[u] = v;
                matchY[v] = u;
                return true;
            }
        }
        return false;
    }
}