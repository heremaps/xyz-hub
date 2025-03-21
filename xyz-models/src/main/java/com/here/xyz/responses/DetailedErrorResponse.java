package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "DetailedErrorResponse")
public class DetailedErrorResponse extends ErrorResponse {

    private String errorTitle;
    private String errorAction;
    private String errorCause;

    public String getErrorTitle() {
        return errorTitle;
    }

    public void setErrorTitle(String errorTitle) {
        this.errorTitle = errorTitle;
    }

    public DetailedErrorResponse withErrorTitle(String errorTitle) {
        setErrorTitle(errorTitle);
        return this;
    }

    public String getErrorAction() {
        return errorAction;
    }

    public void setErrorAction(String errorAction) {
        this.errorAction = errorAction;
    }

    public DetailedErrorResponse withErrorAction(String errorAction) {
        setErrorAction(errorAction);
        return this;
    }

    public String getErrorCause() {
        return errorCause;
    }

    public void setErrorCause(String errorCause) {
        this.errorCause = errorCause;
    }

    public DetailedErrorResponse withErrorCause(String errorCause) {
        setErrorCause(errorCause);
        return this;
    }
}
