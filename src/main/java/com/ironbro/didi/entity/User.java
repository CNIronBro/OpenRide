package com.ironbro.didi.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ironbro.didi.enums.UserRole;
import com.ironbro.didi.enums.UserStatus;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 系统用户实体（乘客 / 司机 / 管理员共用）
 * 对应 user 表
 */
@Data
@TableName("user")
public class User {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 手机号，登录凭证，全局唯一 */
    private String phone;

    /** 角色：PASSENGER / DRIVER / ADMIN */
    private UserRole role;

    /** 账号状态：NORMAL / BANNED */
    private UserStatus status;

    private LocalDateTime createdAt;
}