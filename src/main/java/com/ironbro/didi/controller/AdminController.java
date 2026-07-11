package com.ironbro.didi.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.Driver;
import com.ironbro.didi.entity.Order;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.enums.UserRole;
import com.ironbro.didi.service.AdminService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 管理员接口，不做重点
 *
 */
@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final AdminService adminService;

    /** 权限校验工具方法 */

    private void checkAdmin(HttpSession session) {
        Long userId = SessionUtils.getUserId(session);
        if (userId == null) throw new BizException(401, "未登录");
        if (SessionUtils.getRole(session) != UserRole.ADMIN) throw new BizException(403, "无权限");
    }

    /** 司机列表（分页 + 状态筛选） */

    @GetMapping("/drivers")
    public Result<Page<Driver>> drivers(
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false)    String status,
            HttpSession session) {
        checkAdmin(session);
        return Result.ok(adminService.listDrivers(page, size, status));
    }

    /** 订单列表（分页 + 状态筛选） */

    @GetMapping("/orders")
    public Result<Page<Order>> orders(
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false)    String status,
            HttpSession session) {
        checkAdmin(session);
        return Result.ok(adminService.listOrders(page, size, status));
    }


    /**
     * 司机资质审核（通过 / 拒绝）
     *
     * 请求体：{ "action": "APPROVED" / "REJECTED" }
     * 通过后司机可上线接单；拒绝后司机需重新提交材料。
     */
    @PostMapping("/drivers/{id}/audit")
    public Result<Void> audit(@PathVariable Long id,
                              @RequestBody Map<String, String> body,
                              HttpSession session) {
        checkAdmin(session);
        adminService.auditDriver(id, body.get("action"));
        return Result.ok();
    }

    /** 封禁 / 解封司机 */

    @PostMapping("/drivers/{id}/ban")
    public Result<Void> banDriver(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        adminService.banDriver(id);
        return Result.ok();
    }

    @PostMapping("/drivers/{id}/unban")
    public Result<Void> unbanDriver(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        adminService.unbanDriver(id);
        return Result.ok();
    }

    /** 乘客列表 + 封禁 / 解封乘客 */

    @GetMapping("/passengers")
    public Result<Page<User>> passengers(
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "10") int size,
            HttpSession session) {
        checkAdmin(session);
        return Result.ok(adminService.listPassengers(page, size));
    }

    @PostMapping("/passengers/{id}/ban")
    public Result<Void> banPassenger(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        adminService.banPassenger(id);
        return Result.ok();
    }

    @PostMapping("/passengers/{id}/unban")
    public Result<Void> unbanPassenger(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        adminService.unbanPassenger(id);
        return Result.ok();
    }

    /** 强制取消订单 */

    @PostMapping("/orders/{id}/cancel")
    public Result<Void> cancelOrder(@PathVariable Long id, HttpSession session) {
        checkAdmin(session);
        adminService.cancelOrder(id);
        return Result.ok();
    }

    /** 统计看板数据 */

    @GetMapping("/stats")
    public Result<Map<String, Object>> stats(HttpSession session) {
        checkAdmin(session);
        return Result.ok(adminService.getStats());
    }
}