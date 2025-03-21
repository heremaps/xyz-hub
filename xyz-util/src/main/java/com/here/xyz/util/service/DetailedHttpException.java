package com.here.xyz.util.service;

import com.here.xyz.util.errors.ErrorDefinition;
import io.netty.handler.codec.http.HttpResponseStatus;

public class DetailedHttpException extends HttpException {

    private static final long serialVersionUID = 1L;

    public final ErrorDefinition errorDefinition;
    public final Throwable cause;

    public DetailedHttpException(ErrorDefinition errorDefinition, Throwable cause) {
        super(HttpResponseStatus.valueOf(errorDefinition.getStatus()), errorDefinition.composeMessage());
        this.errorDefinition = errorDefinition;
        this.cause = cause;
    }
}