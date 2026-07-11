package com.ironbro.didi.websocket;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * WebSocket 连接生命周期处理器
 *
 * 职责：
 * 1. 连接建立时：从 session attributes 取 userId，注册到 WebSocketSessionManager
 * 2. 连接关闭时：从 WebSocketSessionManager 移除
 * 3. 收到消息时：处理心跳 PING，回复 PONG
 *
 * 心跳说明：
 * 客户端每 25s 发一次 {"type":"PING"}，服务端回复 {"type":"PONG"}。
 * 目的：防止 NAT/防火墙/Nginx 因空闲超时断开连接（通常 30-60s 无数据即断）。
 * 客户端发出 PING 后启动 5s 超时计时器，若未收到 PONG 则认为连接已死，主动关闭触发重连。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RideWebSocketHandler extends TextWebSocketHandler {

    private static final String PING_MESSAGE = "{\"type\":\"PING\"}";
    private static final String PONG_MESSAGE = "{\"type\":\"PONG\"}";

    private final WebSocketSessionManager sessionManager;

    /**
     * 连接建立后：取 userId，注册到 SessionManager
     *
     * userId 由 HttpSessionHandshakeInterceptor 在握手阶段从 HttpSession 复制过来。
     * 若 userId 为 null，说明用户未登录（未经过 /auth/login），拒绝连接。
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        Long userId = (Long) session.getAttributes().get("userId");
        if (userId == null) {
            log.warn("WS 连接拒绝：未登录用户 sessionId={}", session.getId());
            session.close(CloseStatus.POLICY_VIOLATION);
            return;
        }
        sessionManager.register(userId, session);
        log.info("WS 连接建立 userId={} sessionId={}", userId, session.getId());
    }

    /**
     * 连接关闭后：从 SessionManager 移除
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        Long userId = (Long) session.getAttributes().get("userId");
        if (userId != null) {
            sessionManager.remove(userId, session);
        }
        log.info("WS 连接关闭 userId={} sessionId={} status={}", userId, session.getId(), status);
    }

    /**
     * 收到客户端消息：处理心跳 PING
     *
     * 未知消息类型只记录 debug 日志，不做任何处理。
     */
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        if (PING_MESSAGE.equals(payload)) {
            // 回复 PONG，客户端收到后清除超时计时器，确认连接存活
            synchronized (session) {
                session.sendMessage(new TextMessage(PONG_MESSAGE));
            }
            return;
        }
        log.debug("WS 收到未知消息 sessionId={} payload={}", session.getId(), payload);
    }

    /**
     * 传输层异常：记录日志，连接会随后触发 afterConnectionClosed
     */
    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        Long userId = (Long) session.getAttributes().get("userId");
        log.warn("WS 传输异常 userId={} sessionId={}", userId, session.getId(), exception);
    }
}