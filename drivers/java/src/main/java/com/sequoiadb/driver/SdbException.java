package com.sequoiadb.driver;

public class SdbException extends RuntimeException {
    private final int code;

    public SdbException(int code) {
        super("SdbError[" + code + "]");
        this.code = code;
    }

    public SdbException(int code, String message) {
        super("SdbError[" + code + "]: " + message);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
