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
 * 注册 WebSocket handler，路径为 /ws
 *
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