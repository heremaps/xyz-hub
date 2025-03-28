package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "DetailedErrorResponse")
public class DetailedErrorResponse extends ErrorResponse {

    private String title;
    private String code;
    private String cause;
    private String action;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public DetailedErrorResponse withTitle(String errorTitle) {
        setTitle(errorTitle);
        return this;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public DetailedErrorResponse withCode(String code) {
        setCode(code);
        return this;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public DetailedErrorResponse withCause(String errorCause) {
        setCause(errorCause);
        return this;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public DetailedErrorResponse withAction(String errorAction) {
        setAction(errorAction);
        return this;
    }
}
