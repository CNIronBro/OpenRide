package com.ironbro.didi.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ironbro.didi.common.BizException;
import com.ironbro.didi.entity.User;
import com.ironbro.didi.enums.UserRole;
import com.ironbro.didi.enums.UserStatus;
import com.ironbro.didi.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * 认证服务
 *
 * 职责：
 * 1. 管理员账号密码校验
 * 2. 乘客/司机手机号登录 + 首次自动注册
 * 3. 封禁状态校验
 *
 * 验证码说明：当前为 mock 固定值 "123456"，不做真实短信。
 */
@Service
@RequiredArgsConstructor
public class AuthService {

    /** mock 验证码，固定值 */
    private static final String MOCK_CODE = "123456";

    private final UserMapper userMapper;

    @Value("${admin.username}")
    private String adminUsername;

    @Value("${admin.password}")
    private String adminPassword;

    /**
     * 管理员登录校验
     *
     *
     * @param username 用户名
     * @param password 密码
     * @return 管理员用户名（用于返回给前端展示）
     */
    public String adminLogin(String username, String password) {
        if (!adminUsername.equals(username) || !adminPassword.equals(password)) {
            throw new BizException(401, "用户名或密码错误");
        }
        return adminUsername;
    }

    /**
     * 用户登录 / 自动注册
     *
     * 业务流程：
     * 1. 校验手机号和验证码
     * 2. 查找已有用户，不存在则按 role 自动注册（默认 PASSENGER）
     * 3. 校验封禁状态
     *
     *
     * @param phone   手机号
     * @param code    验证码
     * @param roleStr 角色字符串（PASSENGER / DRIVER），首次注册时使用
     * @return 登录成功的用户实体
     */
    public User login(String phone, String code, String roleStr) {
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

        // 封禁校验：已注册用户登录时检查状态
        if (user.getStatus() == UserStatus.BANNED) {
            throw new BizException(403, "账号已被封禁");
        }

        return user;
    }
}