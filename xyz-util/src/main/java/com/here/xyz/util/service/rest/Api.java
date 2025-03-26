/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.util.service.rest;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.ACCEPT_ENCODING;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.XyzSerializable.SerializationView;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.BaseConfig;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import com.here.xyz.util.service.BaseHttpServerVerticle.RequestCancelledException;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.logging.LogUtil;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import java.io.ByteArrayOutputStream;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class Api {
    public static final int MAX_SERVICE_RESPONSE_SIZE = (BaseConfig.instance == null ? 0 : BaseConfig.instance.MAX_SERVICE_RESPONSE_SIZE);
    public static final int MAX_HTTP_RESPONSE_SIZE = (BaseConfig.instance == null ? 0 : BaseConfig.instance.MAX_HTTP_RESPONSE_SIZE);
    public static final HttpResponseStatus RESPONSE_PAYLOAD_TOO_LARGE = new HttpResponseStatus(513, "Response payload too large");
    public static final String RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE =
            "The response payload was too large. Please try to reduce the expected amount of data.";
    protected static final Logger logger = LogManager.getLogger();

    /**
     * Converts the given response into a {@link HttpException}.
     *
     * @param response the response to be converted.
     * @return the {@link HttpException} that reflects the response best.
     */
    public static HttpException responseToHttpException(final XyzResponse response) {
        if (response instanceof ErrorResponse) {
            return new HttpException(BAD_GATEWAY, ((ErrorResponse) response).getErrorMessage());
        }
        return new HttpException(BAD_GATEWAY, "Received invalid response of type '" + response.getClass().getSimpleName() + "'");
    }

    public static Marker getMarker(RoutingContext context) {
        return LogUtil.getMarker(context);
    }

    private void sendInternalServerError(RoutingContext context, Throwable t) {
        logger.error("Handling as internal server error:", t);
        sendErrorResponse(context, new HttpException(INTERNAL_SERVER_ERROR, "Server error!", t));
    }

    protected Handler<RoutingContext> handleErrors(ThrowingHandler<RoutingContext> handler) {
        return context -> {
            try {
                handler.handle(context);
            }
            catch (HttpException | IllegalArgumentException | ValidationException | AccessDeniedException | RequestCancelledException e) {
                sendErrorResponse(context, e);
            }
            catch (Exception e) {
                sendInternalServerError(context, e);
            }
        };
    }

    protected <R> Handler<RoutingContext> handle(ThrowingTask<R, RoutingContext> taskHandler) {
        return handleErrors(context -> {
            taskHandler.execute(context)
                    .onSuccess(response -> sendResponseWithXyzSerialization(context, OK, response))
                    .onFailure(t -> {
                        if (t instanceof HttpException httpException)
                            sendErrorResponse(context, httpException);
                        else
                            sendInternalServerError(context, t);
                    });
        });
    }

    /**
     * Send an error response to the client when an exception occurred while processing a task.
     *
     * @param context the context for which to return an error response.
     * @param e       the exception that should be used to generate an {@link ErrorResponse}, if null an internal server error is returned.
     */
    protected void sendErrorResponse(final RoutingContext context, Throwable e) {
        if (e instanceof RequestCancelledException)
            return;

        if (e instanceof ValidationException || e instanceof IllegalArgumentException || e instanceof IllegalStateException)
            e = new HttpException(BAD_REQUEST, e.getMessage(), e);
        else if (e instanceof AccessDeniedException)
            e = new HttpException(FORBIDDEN, e.getMessage(), e);

        if (e instanceof HttpException httpException) {

            if (INTERNAL_SERVER_ERROR.code() != httpException.status.code()) {
                XyzError error;
                if (BAD_GATEWAY.code() == httpException.status.code())
                    error = XyzError.BAD_GATEWAY;
                else if (GATEWAY_TIMEOUT.code() == httpException.status.code())
                    error = XyzError.TIMEOUT;
                else if (BAD_REQUEST.code() == httpException.status.code())
                    error = XyzError.ILLEGAL_ARGUMENT;
                else if (NOT_FOUND.code() == httpException.status.code())
                    error = XyzError.NOT_FOUND;
                else
                    error = XyzError.EXCEPTION;

                //This is an exception sent by intention and nothing special, no need for stacktrace logging.
                logger.warn(Api.getMarker(context), "Error was handled by Api and will be sent as response: {}", httpException.status.code());
                logger.info(Api.getMarker(context), "Handled exception was:", httpException);
                sendErrorResponse(context, httpException, error);
                return;
            }
        }

        //This is an exception that is not done by intention.
        logger.error(Api.getMarker(context), "Unintentional Error:", e);
        BaseHttpServerVerticle.sendErrorResponse(context, e);
    }

    /**
     * Send an error response to the client.
     *
     * @param context      the routing context for which to return an error response.
     * @param status       the HTTP status code to set.
     * @param error        the error type that will become part of the {@link ErrorResponse}.
     * @param errorMessage the error message that will become part of the {@link ErrorResponse}.
     */
    protected void sendErrorResponse(final RoutingContext context, final HttpResponseStatus status, final XyzError error,
                                     final String errorMessage) {
        context.response()
                .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setStatusCode(status.code())
                .setStatusMessage(status.reasonPhrase())
                .end(new ErrorResponse()
                        .withStreamId(Api.getMarker(context).getName())
                        .withError(error)
                        .withErrorMessage(errorMessage).serialize());
    }

    /**
     * Send an error response to the client.
     *
     * @param context the routing context for which to return an error response.
     * @param httpError the HTTPException with all information
     * @param error the error type that will become part of the {@link ErrorResponse}.
     */
    private void sendErrorResponse(final RoutingContext context, final HttpException httpError, final XyzError error) {
        ErrorResponse errorResponse;
        if (httpError instanceof DetailedHttpException detailedHttpException) {
          errorResponse = detailedHttpException.errorDefinition.toErrorResponse(detailedHttpException.placeholders)
                .withStreamId(Api.getMarker(context).getName())
                .withErrorDetails(httpError.errorDetails)
                .withError(error)
                .withErrorMessage(httpError.getMessage());
        }
        else {
            errorResponse = new ErrorResponse()
                .withStreamId(Api.getMarker(context).getName())
                .withErrorDetails(httpError.errorDetails)
                .withError(error)
                .withErrorMessage(httpError.getMessage());
        }
        context.response()
            .putHeader(CONTENT_TYPE, APPLICATION_JSON)
            .setStatusCode(httpError.status.code())
            .setStatusMessage(httpError.status.reasonPhrase())
            .end(errorResponse.serialize());
    }

    protected long getMaxResponseLength(final RoutingContext context) {
        long serviceSize = MAX_SERVICE_RESPONSE_SIZE > 0 ? MAX_SERVICE_RESPONSE_SIZE : Long.MAX_VALUE;
        long httpSize = MAX_HTTP_RESPONSE_SIZE > 0 ? MAX_HTTP_RESPONSE_SIZE : Long.MAX_VALUE;
        return XYZHttpContentCompressor.isCompressionEnabled(context.request().getHeader(ACCEPT_ENCODING)) ? serviceSize : httpSize;
    }

    /**
     * @deprecated Use {@link #sendResponse(RoutingContext, int, XyzSerializable)} instead!
     * @param context
     * @param status
     * @param o
     */
    @Deprecated
    protected void sendResponse(RoutingContext context, HttpResponseStatus status, Object o) {
        HttpServerResponse httpResponse = context.response().setStatusCode(status.code());

        byte[] response;
        try {
            if (o instanceof ByteArrayOutputStream baos)
                response = baos.toByteArray();
            else
                response = Json.encode(o).getBytes();
        }
        catch (EncodeException e) {
            sendErrorResponse(context, new HttpException(INTERNAL_SERVER_ERROR, "Could not serialize response.", e));
            return;
        }

        sendResponseBytes(context, httpResponse, response);
    }

    /**
     * @deprecated Please use {@link #sendResponse(RoutingContext, int, XyzSerializable)} or {@link #sendResponse(RoutingContext, int, List)} instead.
     * @param context
     * @param status
     * @param o
     */
    protected void sendResponseWithXyzSerialization(RoutingContext context, HttpResponseStatus status, Object o) {
        sendResponseWithXyzSerialization(context, status, o, null);
    }

    /**
     * @deprecated Please use {@link #sendResponse(RoutingContext, int, XyzSerializable)} or {@link #sendResponse(RoutingContext, int, List)} instead.
     * @param context
     * @param status
     * @param o
     * @param type
     */
    @Deprecated
    protected void sendResponseWithXyzSerialization(RoutingContext context, HttpResponseStatus status, Object o, TypeReference type) {
        HttpServerResponse httpResponse = context.response().setStatusCode(status.code());

        byte[] response;
        try {
            if (o == null)
                response = new byte[]{};
            else
                response = o instanceof ByteArrayOutputStream bos ? bos.toByteArray() : (type == null ? XyzSerializable.serialize(o)
                    : XyzSerializable.serialize(o, type)).getBytes();
        }
        catch (EncodeException e) {
            sendErrorResponse(context, new HttpException(INTERNAL_SERVER_ERROR, "Could not serialize response.", e));
            return;
        }

        sendResponseBytes(context, httpResponse, response);
    }

    protected void sendResponseBytes(RoutingContext context, HttpServerResponse httpResponse, byte[] response) {
        if (response.length == 0)
            httpResponse.setStatusCode(NO_CONTENT.code()).end();
        else if (response.length > getMaxResponseLength(context))
            sendErrorResponse(context, new HttpException(RESPONSE_PAYLOAD_TOO_LARGE, RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE));
        else {
            httpResponse.putHeader(CONTENT_TYPE, APPLICATION_JSON);
            httpResponse.end(Buffer.buffer(response));
        }
    }

    protected void sendResponse(RoutingContext context, int statusCode, XyzSerializable object) {
        serializeAndSendResponse(context, statusCode, object, null, Public.class);
    }

    protected void sendResponse(RoutingContext context, int statusCode, List<? extends XyzSerializable> list) {
        serializeAndSendResponse(context, statusCode, list, null, Public.class);
    }

    protected void sendResponse(RoutingContext context, int statusCode, List<? extends XyzSerializable> list,
        TypeReference listItemTypeReference) {
        serializeAndSendResponse(context, statusCode, list, listItemTypeReference, Public.class);
    }

    protected void sendInternalResponse(RoutingContext context, int statusCode, XyzSerializable object) {
        serializeAndSendResponse(context, statusCode, object, null, null); //TODO: Use Internal view here in future
    }

    protected void sendInternalResponse(RoutingContext context, int statusCode, List<? extends XyzSerializable> list) {
        serializeAndSendResponse(context, statusCode, list, null, null); //TODO: Use Internal view here in future
    }

    protected void sendInternalResponse(RoutingContext context, int statusCode, List<? extends XyzSerializable> list,
        TypeReference listItemTypeReference) {
        serializeAndSendResponse(context, statusCode, list, listItemTypeReference, null); //TODO: Use Internal view here in future
    }

    private void serializeAndSendResponse(RoutingContext context, int statusCode, Object object,
        TypeReference listItemTypeReference, Class<? extends SerializationView> view) {
        if (listItemTypeReference != null && !(object instanceof List))
            throw new IllegalArgumentException("Type info for list items may only be specified if the object to be serialized is a list.");

        HttpServerResponse httpResponse = context.response().setStatusCode(statusCode);

        byte[] response;
        try {
            if (object == null)
                response = new byte[]{};
            else
                response = object instanceof ByteArrayOutputStream bos ? bos.toByteArray()
                    : (listItemTypeReference == null
                        ? XyzSerializable.serialize(object, view)
                        : XyzSerializable.serialize((List<?>) object, view, listItemTypeReference)).getBytes();
        }
        catch (EncodeException e) {
            sendErrorResponse(context, new HttpException(INTERNAL_SERVER_ERROR, "Could not serialize response.", e));
            return;
        }

        sendResponseBytes(context, httpResponse, response);
    }

    public interface ThrowingHandler<E> {
        void handle(E event) throws Exception;
    }

    public interface ThrowingTask<R, E> {
        Future<R> execute(E event) throws Exception;
    }

    private static class XYZHttpContentCompressor extends HttpContentCompressor {

        private static final XYZHttpContentCompressor instance = new XYZHttpContentCompressor();

        static boolean isCompressionEnabled(String acceptEncoding) {
            if (acceptEncoding == null) {
                return false;
            }

            final ZlibWrapper wrapper = instance.determineWrapper(acceptEncoding);
            return wrapper == ZlibWrapper.GZIP || wrapper == ZlibWrapper.ZLIB;
        }
    }

    public static class AccessDeniedException extends Exception {
        public AccessDeniedException(String message) {
            super(message);
        }

        public AccessDeniedException(String message, Exception cause) {
            super(message, cause);
        }
    }
}
