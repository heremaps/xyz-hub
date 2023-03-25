/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub;

import static com.google.common.net.HttpHeaders.ACCEPT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static com.here.xyz.hub.rest.Context.logId;
import static com.here.xyz.hub.rest.Context.logStream;
import static com.here.xyz.hub.rest.Context.logTime;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.lang.System.currentTimeMillis;

import com.here.xyz.IEventContext;
import com.here.xyz.IEventHandler;
import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.http.HttpProcessorParams;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.ext.web.RoutingContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpProcessor implements IEventHandler {

  protected static final Logger logger = LoggerFactory.getLogger(HttpProcessor.class);

  protected static final HttpClient httpClient = Service.vertx.createHttpClient(
      new HttpClientOptions()
          .setMaxPoolSize(Service.configuration.MAX_GLOBAL_HTTP_CLIENT_CONNECTIONS)
          .setHttp2MaxPoolSize(Service.configuration.MAX_GLOBAL_HTTP_CLIENT_CONNECTIONS)
          .setTcpKeepAlive(Service.configuration.HTTP_CLIENT_TCP_KEEPALIVE)
          .setIdleTimeout(Service.configuration.HTTP_CLIENT_IDLE_TIMEOUT)
          .setTcpQuickAck(true)
          .setTcpFastOpen(true)
          .setPipelining(Service.configuration.HTTP_CLIENT_PIPELINING))
      /*.connectionHandler(HTTPFunctionClient::newConnectionCreated)*/;

  public HttpProcessor(@NotNull RoutingContext context, @NotNull Connector connector) {
    this.context = context;
    this.connector = connector;
    final Map<@NotNull String, @NotNull Object> params = connector.params;
    if (params == null) {
      throw new IllegalStateException("The connector " + connector.id + " does not have 'params'");
    }
    this.params = new HttpProcessorParams(params, logId(context));
  }

  protected final @NotNull RoutingContext context;
  protected final @NotNull Connector connector;
  protected final @NotNull HttpProcessorParams params;
  private Buffer body;
  private final AtomicReference<XyzResponse<?>> xyzResponse = new AtomicReference<>();
  private final AtomicReference<HttpException> exception = new AtomicReference<>();
  private HttpClientRequest httpRequest;
  private HttpClientResponse httpResponse;

  private synchronized void throwException(@NotNull Throwable t) {
    logger.info("{}:{}:{}us - Remote HTTP connector service error", logId(context), logStream(context), logTime(context), t);
    final HttpException e;
    if (t instanceof HttpException) {
      e = (HttpException) t;
    } else {
      e = new HttpException(BAD_GATEWAY, "Unknown error while accessing remote HTTP connector service: "+t.getMessage());
    }
    exception.set(e);
    this.notifyAll();
  }

  private synchronized void returnResult(@NotNull XyzResponse<?> response) {
    xyzResponse.set(response);
    this.notifyAll();
  }

  private void onSocketOpen(@NotNull HttpClientRequest request) {
    logger.info("{}:{}:{}us - Remote HTTP connector, socket open", logId(context), logStream(context), logTime(context));
    request.setTimeout(params.readTimeout);
    this.httpRequest = request;
    request.exceptionHandler(this::throwException);
    request.send(body)
        .onFailure(this::throwException)
        .onSuccess(this::onClientResponse);
  }

  private void onClientResponse(@NotNull HttpClientResponse response) {
    logger.info("{}:{}:{}us - Remote HTTP connector, client response", logId(context), logStream(context), logTime(context));
    this.httpResponse = response;
    final int statusCode = response.statusCode();
    final String statusMessage = response.statusMessage();
    if (statusCode != OK.code() && statusCode != CREATED.code()) {
      throwException(new HttpException(BAD_GATEWAY, "Remote HTTP connector service responded with: " + statusCode + " " + statusMessage));
    } else {
      response.body(this::onBody);
    }
  }

  private void onBody(@NotNull AsyncResult<Buffer> ar) {
    logger.info("{}:{}:{}us - Remote HTTP connector, body response", logId(context), logStream(context), logTime(context));
    if (ar.failed()) {
      throwException(ar.cause());
      return;
    }
    final byte[] body = ar.result().getBytes();
    if (body == null || body.length == 0) {
      throwException(new HttpException(BAD_GATEWAY, "Remote HTTP connector service responded with empty body"));
      return;
    }
    try {
      final InputStream is = Payload.prepareInputStream(new ByteArrayInputStream(body));
      final String rawResponse;
      try (final Scanner scanner = new Scanner(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        rawResponse = scanner.useDelimiter("\\A").next();
      }
      logger.info("{}:{}:{}us - Raw response received from HTTP connector: {}", logId(context), logStream(context), logTime(context), rawResponse);
      final Typed response = XyzSerializable.deserialize(rawResponse);
      if (response == null) {
        throw new NullPointerException();
      }
      if (response instanceof XyzResponse) {
        returnResult((XyzResponse<?>) response);
        return;
      }
      throwException(new HttpException(BAD_GATEWAY, "Failed to parse response received by HTTP connector service"));
    } catch (IOException e) {
      logger.info("{}:{}:{}us - Failed to parse HTTP connector response", logId(context), logStream(context), logTime(context), e);
      throwException(new HttpException(BAD_GATEWAY, "Failed to parse response received by HTTP connector service"));
    }
  }

  @Override
  public @NotNull XyzResponse<?> processEvent(@NotNull IEventContext eventContext) {
    logger.info("{}:{}:{}us - Send event to URL: {}", logId(context), logStream(context), logTime(context), params.url);
    // Either Throwable or HttpClientResponse:
    final AtomicReference<Object> result = new AtomicReference<>();
    final Event<?> event = eventContext.event();
    body = Buffer.buffer(Payload.compress(event.toByteArray()));
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (result) {
      httpClient.request(new RequestOptions()
          .setMethod(HttpMethod.POST)
          .setTimeout(params.connTimeout)
          .putHeader(CONTENT_TYPE, "application/json; charset=" + Charset.defaultCharset().name())
          .putHeader(CONTENT_ENCODING, "gzip")
          .putHeader(STREAM_ID, logStream(context))
          .putHeader(ACCEPT_ENCODING, "gzip")
          .putHeader(USER_AGENT, Service.XYZ_HUB_USER_AGENT)
          .setAbsoluteURI(params.url)
      ).onFailure(this::throwException).onSuccess(this::onSocketOpen);

    }
    return getResult(currentTimeMillis() + Math.max(params.connTimeout, params.readTimeout));
  }

  private synchronized @NotNull XyzResponse<?> getResult(long TIMEOUT) {
    while (xyzResponse.get() == null
        && exception.get() == null
        && currentTimeMillis() < TIMEOUT) {
      try {
        final long waitMillis = Math.max(TIMEOUT - currentTimeMillis(), 0L);
        if (waitMillis > 0L) {
          this.wait(waitMillis);
        }
      } catch (InterruptedException ignore) {
      }
    }
    final XyzResponse<?> response = xyzResponse.get();
    if (response != null) return response;

    HttpException e = exception.get();
    if (e == null) {
      e = new HttpException(GATEWAY_TIMEOUT, "Remote HTTP connector service did not respond in time");
    }
    return new ErrorResponse()
        .withStreamId(logStream(context))
        .withError(XyzError.forValue(e.status.reasonPhrase(), XyzError.EXCEPTION))
        .withErrorMessage(e.getMessage());
  }
}
