package com.ironbro.didi.controller;

import com.ironbro.didi.common.BizException;
import com.ironbro.didi.common.Result;
import com.ironbro.didi.common.SessionUtils;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.enums.UserRole;
import com.ironbro.didi.enums.UserStatus;
import com.ironbro.didi.mapper.UserMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 认证接口
 *
 * 登录策略：手机号 + 验证码（mock 固定为 123456）
 * 首次登录自动注册为乘客（PASSENGER），司机需管理员后台创建或单独注册接口。
 * 身份识别使用 HttpSession，无 JWT。
 */
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    /** mock 验证码，固定值，不做真实短信 */
    private static final String MOCK_CODE = "123456";

    private final UserMapper userMapper;

    @Value("${admin.username}")
    private String adminUsername;

    @Value("${admin.password}")
    private String adminPassword;

    /**
     * 管理员登录（账号密码写死在 yml，不走数据库）
     *
     * 请求体：{ "username": "admin", "password": "admin123" }
     * 登录成功后 Session 中 role=ADMIN，userId=-1（虚拟 ID，管理员不存数据库）
     */
    @PostMapping("/admin/login")
    public Result<Map<String, Object>> adminLogin(@RequestBody Map<String, String> body, HttpSession session) {
        if (!adminUsername.equals(body.get("username")) || !adminPassword.equals(body.get("password"))) {
            throw new BizException(401, "用户名或密码错误");
        }
        SessionUtils.setLogin(session, -1L, UserRole.ADMIN);
        return Result.ok(Map.of("username", adminUsername, "role", "ADMIN"));
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
        String phone = body.get("phone");
        String code  = body.get("code");
        String roleStr = body.getOrDefault("role", "PASSENGER");

        if (phone == null || phone.isBlank()) {
            throw new BizException("手机号不能为空");
        }
        if (!MOCK_CODE.equals(code)) {
            throw new BizException(401, "验证码错误");
        }

        // 查找已有用户，不存在则自动注册
        User user = userMapper.selectOne(
                new LambdaQueryWrapper<User>().eq(User::getPhone, phone));

        if (user == null) {
            user = new User();
            user.setPhone(phone);
            user.setRole(UserRole.valueOf(roleStr));
            user.setStatus(UserStatus.NORMAL);
            userMapper.insert(user);
        }

        if (user.getStatus() == UserStatus.BANNED) {
            throw new BizException(403, "账号已被封禁");
        }

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