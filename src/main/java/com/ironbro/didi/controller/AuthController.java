package com.ironbro.didi.controller;

import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.service.AuthService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 认证接口
 *
 * 登录策略：手机号 + 验证码（mock 固定为 123456）
 * 首次登录自动注册为乘客（PASSENGER），司机需管理员后台创建。
 */
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    /**
     * 管理员登录
     *
     * 请求体：{ "username": "admin", "password": "admin123" }
     */
    @PostMapping("/admin/login")
    public Result<Map<String, Object>> adminLogin(@RequestBody Map<String, String> body, HttpSession session) {
        String username = authService.adminLogin(body.get("username"), body.get("password"));
        SessionUtils.setLogin(session, -1L, com.ironbro.didi.enums.UserRole.ADMIN);
        return Result.ok(Map.of("username", username, "role", "ADMIN"));
    }

    /**
     * 登录 / 自动注册
     *
     * 请求体：{ "phone": "13800000000", "code": "123456", "role": "PASSENGER" }
     * role 可选，默认 PASSENGER；首次登录时按 role 注册。
     *
     * 返回：{ userId, phone, role }
     */
    @PostMapping("/login")
    public Result<Map<String, Object>> login(@RequestBody Map<String, String> body, HttpSession session) {
        User user = authService.login(
                body.get("phone"),
                body.get("code"),
                body.getOrDefault("role", "PASSENGER")
        );
        SessionUtils.setLogin(session, user.getId(), user.getRole());
        return Result.ok(Map.of(
                "userId", user.getId(),
                "phone",  user.getPhone(),
                "role",   user.getRole()
        ));
    }

    /** 登出 */
    @PostMapping("/logout")
    public Result<Void> logout(HttpSession session) {
        SessionUtils.logout(session);
        return Result.ok();
    }
}