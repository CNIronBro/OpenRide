package com.ironbro.didi.controller;

import com.ironbro.didi.common.Result;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 前端配置接口
 *
 * 将需要注入前端页面的配置项（如第三方 API Key）通过接口下发，
 * 避免将敏感 Key 硬编码在静态 HTML 文件中（静态文件会被直接访问，无法通过 .gitignore 保护）。
 */
@RestController
public class ConfigController {

    @Value("${amap.js-key}")
    private String amapJsKey;

    // 高德 JS API v2.0 要求配置安全密钥（jscode），否则路线规划等服务会返回 INVALID_USER_SCODE
    @Value("${amap.js-code}")
    private String amapJsCode;

    /**
     * 返回高德地图 JS API Key 和安全密钥，供前端动态加载 SDK 使用
     *
     * 注意：此接口返回的 Key 仍会在浏览器中可见，这是 Web 端地图 Key 的正常使用方式。
     * 安全措施应在高德开放平台配置域名白名单，而非隐藏 Key 本身。
     */
    @GetMapping("/config/amap-key")
    public Result<Map<String, String>> amapKey() {
        return Result.ok(Map.of("key", amapJsKey, "jscode", amapJsCode));
    }
}