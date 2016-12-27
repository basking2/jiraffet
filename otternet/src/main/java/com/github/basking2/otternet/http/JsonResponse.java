package com.github.basking2.otternet.http;

/**
 */
public class JsonResponse {
    public static final String OK = "OK";
    public static final String ERROR = "ERROR";

    private String status;
    private String message;

    public JsonResponse(final String status, final String message) {
        this.status = status;
        this.message = message;
    }

    public JsonResponse() {
        this(OK, "");
    }

    public JsonResponse(final Throwable t) {
        this(ERROR, t.getMessage());
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
