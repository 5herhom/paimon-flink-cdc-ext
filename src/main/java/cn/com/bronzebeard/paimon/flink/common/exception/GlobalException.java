package cn.com.bronzebeard.paimon.flink.common.exception;

import cn.com.bronzebeard.paimon.flink.common.util.ExceptionUtil;

public class GlobalException extends RuntimeException {
    private int code;

    public GlobalException(String message, int code) {
        super(message);
        this.code = code;
    }


    public GlobalException(String messagePattern, Object... args) {
        this(String.format(messagePattern, args));
    }

    public GlobalException(String message) {
        super(message);
    }

    public GlobalException(Throwable e) {
        super(e.getMessage(), e);
    }

    public GlobalException(String msg, Throwable e) {
        super(msg, e);
    }
    public String getStackErrorMessage() {
        if (getCause() != null) {
            return ExceptionUtil.toStackTraceString(getCause());
        } else {
            return ExceptionUtil.toStackTraceString(this);
        }
    }
    @Override
    public String toString() {
        return "GlobalException{" +
                "code=" + code +
                "} " + super.toString();
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
}
