package com.timreardon.accumulo.starter.query.impl;

public class QueryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public QueryException() {
        // empty
    }

    public QueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryException(String message) {
        super(message);
    }

    public QueryException(Throwable cause) {
        super(cause);
    }

}
