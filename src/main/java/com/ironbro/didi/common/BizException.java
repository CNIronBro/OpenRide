package com.ironbro.didi.common;

/**
 * 业务异常，用于在 Service 层抛出可预期的业务错误
 * 由 GlobalExceptionHandler 统一捕获并返回 Result.fail
 */
public class BizException extends RuntimeException {

    private final int code;

    public BizException(String message) {
        super(message);
        this.code = 400;
    }

    public BizException(int code, String message) {
        super(message);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}