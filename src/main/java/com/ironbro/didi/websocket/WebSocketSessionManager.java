package com.ironbro.didi.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * WebSocket 会话管理器
 *
 * 维护 userId → WebSocketSession 列表的映射，支持同一用户多端登录（一对多）。
 *
 * - 外层 Map 使用 ConcurrentHashMap，保证 register/remove 的并发安全
 * - 内层 List 使用 CopyOnWriteArrayList，保证遍历发送时不受并发注册/移除影响
 *
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketSessionManager {

    private final ObjectMapper objectMapper;

    /** userId → 该用户的所有活跃 WebSocket 会话（多端登录场景） */
    private final Map<Long, List<WebSocketSession>> sessions = new ConcurrentHashMap<>();

    /**
     * 注册新会话
     * 在 WebSocket 握手完成（afterConnectionEstablished）时调用。
     *
     * @param userId  用户 ID（从 HttpSession 中取，与 SessionUtils 体系一致）
     * @param session 新建立的 WebSocket 会话
     */
    public void register(Long userId, WebSocketSession session) {
        sessions.computeIfAbsent(userId, k -> new CopyOnWriteArrayList<>()).add(session);
        log.debug("WS 会话注册 userId={} sessionId={}", userId, session.getId());
    }

    /**
     * 移除会话
     * 在 WebSocket 连接关闭（afterConnectionClosed）时调用。
     *
     * @param userId  用户 ID
     * @param session 已关闭的 WebSocket 会话
     */
    public void remove(Long userId, WebSocketSession session) {
        List<WebSocketSession> list = sessions.get(userId);
        if (list != null) {
            list.remove(session);
            // 列表为空时清理 Map 条目，避免内存泄漏
            if (list.isEmpty()) {
                sessions.remove(userId, list);
            }
        }
        log.debug("WS 会话移除 userId={} sessionId={}", userId, session.getId());
    }

    /**
     * 向指定用户的所有活跃会话推送消息
     *
     * 推送失败（会话已关闭或网络异常）时只记录 warn 日志，不抛出异常，
     * 不影响调用方的主业务流程（MQ 消费 ACK、事务提交等）。
     *
     * 失败的会话会被从列表中移除，避免后续继续尝试推送到已死亡的会话。
     *
     * @param userId  目标用户 ID
     * @param message 要推送的消息（将被序列化为 JSON）
     */
    public void sendToUser(Long userId, WsMessage message) {
        List<WebSocketSession> list = sessions.get(userId);
        if (list == null || list.isEmpty()) {
            log.debug("用户无活跃 WS 会话，跳过推送 userId={} type={}", userId, message.type());
            return;
        }

        String json;
        try {
            json = objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            log.warn("WS 消息序列化失败 userId={} type={}", userId, message.type(), e);
            return;
        }

        // 收集推送失败的会话，推送完成后统一移除
        List<WebSocketSession> deadSessions = new ArrayList<>();
        for (WebSocketSession session : list) {
            if (!session.isOpen()) {
                deadSessions.add(session);
                continue;
            }
            try {
                // synchronized 保证同一 session 上的并发发送不交叉（WebSocketSession 非线程安全）
                synchronized (session) {
                    session.sendMessage(new TextMessage(json));
                }
            } catch (Exception e) {
                log.warn("WS 推送失败 userId={} sessionId={} type={}", userId, session.getId(), message.type(), e);
                deadSessions.add(session);
            }
        }

        // 清理已死亡的会话
        if (!deadSessions.isEmpty()) {
            list.removeAll(deadSessions);
        }
    }
}