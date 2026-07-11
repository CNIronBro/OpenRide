package com.ironbro.didi.common;

import com.ironbro.didi.enums.UserRole;
import jakarta.servlet.http.HttpSession;

/**
 * Session 工具类
 *
 * 采用 HttpSession 存储登录态，方便快捷，不引入额外复杂度。
 */
public class SessionUtils {

    private static final String KEY_USER_ID = "userId";
    private static final String KEY_ROLE    = "role";

    public static void setLogin(HttpSession session, Long userId, UserRole role) {
        session.setAttribute(KEY_USER_ID, userId);
        session.setAttribute(KEY_ROLE, role);
    }

    public static Long getUserId(HttpSession session) {
        return (Long) session.getAttribute(KEY_USER_ID);
    }

    public static UserRole getRole(HttpSession session) {
        return (UserRole) session.getAttribute(KEY_ROLE);
    }

    public static boolean isLoggedIn(HttpSession session) {
        return getUserId(session) != null;
    }

    public static void logout(HttpSession session) {
        session.invalidate();
    }
}