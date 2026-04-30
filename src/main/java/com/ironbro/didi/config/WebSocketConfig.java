package com.ironbro.didi.config;

import com.ironbro.didi.websocket.RideWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;

/**
 * WebSocket 配置
 *
 * 注册 WebSocket handler，路径为 /ws。
 *
 * HttpSessionHandshakeInterceptor 的作用：
 * 在 WebSocket 握手阶段，将当前 HTTP 请求关联的 HttpSession 中的所有 attributes
 * （含 userId、role，由 SessionUtils.setLogin 写入）复制到 WebSocket session 的 attributes 中。
 * 这样 RideWebSocketHandler 可以直接通过 session.getAttributes().get("userId") 取到登录用户 ID，
 * 无需引入 JWT，与现有 HttpSession 认证体系完全兼容。
 *
 * setAllowedOriginPatterns("*")：
 * 开发阶段允许所有来源，生产环境应替换为实际域名。
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final RideWebSocketHandler rideWebSocketHandler;

    public WebSocketConfig(RideWebSocketHandler rideWebSocketHandler) {
        this.rideWebSocketHandler = rideWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(rideWebSocketHandler, "/ws")
                // 握手拦截器：将 HttpSession attributes 复制到 WS session attributes
                .addInterceptors(new HttpSessionHandshakeInterceptor())
                // 开发阶段允许所有来源，生产环境应限制为实际域名
                .setAllowedOriginPatterns("*");
    }
}