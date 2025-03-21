package com.here.xyz.util.errors;

public class ErrorDefinition {

    private String title;
    private String code;
    private int status;
    private String cause;
    private String action;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public ErrorDefinition withPlaceholders(String formattedTitle, String formattedCause, String formattedAction) {
        ErrorDefinition copy = new ErrorDefinition();
        copy.title = formattedTitle;
        copy.cause = formattedCause;
        copy.action = formattedAction;
        copy.code = this.code;
        copy.status = this.status;
        return copy;
    }

    public String composeMessage() {
        return this.title + " " + this.cause + " " + this.action;
    }
}