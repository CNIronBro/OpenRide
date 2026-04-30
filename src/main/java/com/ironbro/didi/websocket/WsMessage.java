package com.ironbro.didi.websocket;

/**
 * WebSocket 统一消息格式
 *
 * 所有服务端推送消息均使用此格式，序列化为 JSON 后通过 WebSocket 发送。
 *
 * type 枚举值（当前第一期）：
 *   DISPATCH_NOTIFY    - 服务端 → 司机：有新订单待接单
 *   DISPATCH_CANCELLED - 服务端 → 司机：同批订单已被他人接走，关闭弹窗
 *   ORDER_ACCEPTED     - 服务端 → 乘客：司机已接单
 *   ORDER_CANCELLED    - 服务端 → 乘客：订单已被系统取消
 *   DISPATCH_RETRYING  - 服务端 → 乘客：正在重新匹配司机
 *   PING / PONG        - 双向心跳保活
 *
 * @param type    消息类型字符串
 * @param payload 消息载荷，序列化为 JSON Object；心跳消息 payload 为 null
 */
public record WsMessage(String type, Object payload) {}