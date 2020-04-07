/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.connectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.REQUEST_ENTITY_TOO_LARGE;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteStreams;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.RelocationClient;
import com.here.xyz.events.Event;
import com.here.xyz.events.RelocatedEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzResponse;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class RpcClient {

  private static final Logger logger = LogManager.getLogger();

  private static final ConcurrentHashMap<String, RpcClient> connectorIdToClient = new ConcurrentHashMap<>();
  private static final RelocationClient relocationClient = new RelocationClient(Service.configuration.XYZ_HUB_S3_BUCKET);

  private RemoteFunctionClient functionClient;

  /**
   * Creates a new connector client.
   *
   * @param connector the connector for which this client should be created
   */
  private RpcClient(Connector connector) throws NullPointerException {
    if (connector == null) {
      throw new NullPointerException("Can not create RpcClient without connector configuration.");
    }

    if (connector.remoteFunction instanceof Connector.RemoteFunctionConfig.AWSLambda) {
      this.functionClient = new LambdaFunctionClient(connector);
    }
    else if (connector.remoteFunction instanceof Connector.RemoteFunctionConfig.Embedded) {
      this.functionClient = new EmbeddedFunctionClient(connector);
    }
    else if (connector.remoteFunction instanceof Http) {
      this.functionClient = new HTTPFunctionClient(connector);
    }
    else {
      throw new IllegalArgumentException("Unknown remote function type: " + connector.getClass().getSimpleName());
    }
  }

  static RpcClient getInstanceFor(Connector connector, boolean createIfNotExists) {
    if (connector == null) {
      throw new NullPointerException("connector");
    }
    RpcClient client = connectorIdToClient.get(connector.id);
    if (client == null) {
      if (!createIfNotExists) throw new IllegalStateException("No RpcClient is ready for the given connector with ID " + connector.id);
      client = new RpcClient(connector);
      synchronized (connectorIdToClient) {
        connectorIdToClient.put(connector.id, client);
      }
    }
    return client;
  }

  /**
   * Returns the RPC client singleton for the configuration with the ID of the given one. In other words, there will be only one instance
   * per configuration ID.
   *
   * @param connector the configuration for which to return the RPC client.
   * @return the RPC client.
   */
  public static RpcClient getInstanceFor(Connector connector) {
    return getInstanceFor(connector, false);
  }

  public static Collection<RpcClient> getAllInstances() {
    return Collections.unmodifiableCollection(connectorIdToClient.values());
  }

  synchronized void destroy() throws IllegalStateException {
    if (functionClient == null) {
      throw new IllegalStateException("The RpcClient is already destroyed");
    }
    final Connector connectorConfig = functionClient.getConnectorConfig();
    if (connectorConfig != null) {
      synchronized (connectorIdToClient) {
        connectorIdToClient.remove(connectorConfig.id, this);
      }
    }
    //Destroy the functionClient (which then closes all its connections aso.)
    functionClient.destroy();
    functionClient = null;
  }

  public final void setConnectorConfig(Connector connectorConfig) throws NullPointerException, IllegalArgumentException {
    this.functionClient.setConnectorConfig(connectorConfig);
  }

  /**
   * Returns the connector configuration.
   *
   * @return the connector configuration
   */
  public Connector getConnector() {
    final RemoteFunctionClient functionClient = this.functionClient;
    if (functionClient != null) {
      return functionClient.getConnectorConfig();
    }
    return null;
  }

  public RemoteFunctionClient getFunctionClient() {
    return functionClient;
  }

  private void invokeWithRelocation(final Marker marker, byte[] bytes, boolean fireAndForget, final Handler<AsyncResult<byte[]>> callback) {
    try {
      final Connector connector = getConnector();
      if (bytes.length > connector.capabilities.maxPayloadSize) { // If the payload is too large to send directly to the connector
        if (!connector.capabilities.relocationSupport) {
          // The size is to large, the event cannot be sent to the connector.
          callback.handle(Future
              .failedFuture(new HttpException(REQUEST_ENTITY_TOO_LARGE, "The request entity size is over the limit for this connector.")));
          return;
        }

        // If relocation is supported, use the relocation client to transfer the event to the connector
        relocateAsync(marker, bytes, ar -> {
          if (ar.failed()) {
            callback.handle(Future.failedFuture(ar.cause()));
            return;
          }
          functionClient.submit(marker, ar.result(), fireAndForget, callback);
        });
      }
      else {
        functionClient.submit(marker, bytes, fireAndForget, callback);
      }
    } catch (Exception e) {
      callback.handle(Future.failedFuture(e));
    }
  }

  private void relocateAsync(Marker marker, byte[] bytes, Handler<AsyncResult<byte[]>> callback) {
    logger.info(marker, "Relocating event. Total event byte size: {}", bytes.length);
    Service.vertx.executeBlocking(future -> {
      try {
        future.complete(relocationClient.relocate(marker.getName(), bytes));
      }
      catch (Exception e) {
        logger.error("An error occurred when trying to relocate the event.", e);
        future.fail(new HttpException(BAD_GATEWAY, "Unable to relocate event.", e));
      }
    }, ar -> {
      if (ar.failed())
        callback.handle(Future.failedFuture(ar.cause()));
      else
        callback.handle(Future.succeededFuture((byte[]) ar.result()));
    });
  }

  /**
   * Executes an event and returns the parsed FeatureCollection response.
   *
   * @param marker the log marker
   * @param event the event
   * @param callback the callback handler
   */
  @SuppressWarnings("rawtypes")
  public void execute(final Marker marker, final Event event, final Handler<AsyncResult<XyzResponse>> callback) {
    final Connector connector = getConnector();
    event.setConnectorParams(connector.params);
    final String eventJson = event.serialize();
    final byte[] bytes = eventJson.getBytes();
    logger.info(marker, "Invoking remote function \"{}\". Total uncompressed event size: {}, Event: {}", connector.id, bytes.length,
        preview(eventJson, 4092));

    invokeWithRelocation(marker, bytes, false, bytesResult -> {
      if (bytesResult.failed()) {
        callback.handle(Future.failedFuture(bytesResult.cause()));
        return;
      }

      parseResponse(marker, bytesResult.result(), r -> {
        if (r.failed()) {
          logger.error(marker, "Error while handling the response from connector \"{}\".", connector.id, r.cause());
          callback.handle(Future.failedFuture(r.cause()));
          return;
        }
        callback.handle(Future.succeededFuture(r.result()));
      });
    });
  }

  private String preview(String eventJson, @SuppressWarnings("SameParameterValue") int previewLength) {
    if (eventJson == null || eventJson.length() <= previewLength) {
      return eventJson;
    }
    return eventJson.substring(0, previewLength);
  }

  /**
   * Sends an event to the connector without returning anything. Every connector client implements this class.
   *
   * This method should only be used to "inform" a connector about an event rather then "calling" it.
   *
   * @param marker the log marker
   * @param event  the event
   * @throws NullPointerException if this RPC client is closed or the function client it is bound to is closed.
   */
  public void send(final Marker marker, @SuppressWarnings("rawtypes") final Event event) throws NullPointerException {
    final Connector connector = getConnector();
    event.setConnectorParams(connector.params);
    invokeWithRelocation(marker, event.serialize().getBytes(), true, r -> {
      if (r.failed()) {
        logger.error(marker, "Failed to send event to remote function {}.", connector.remoteFunction.id, r.cause());
      }
    });
  }

  @SuppressWarnings("rawtypes")
  private void parseResponse(Marker marker, final Typed payload, final Handler<AsyncResult<XyzResponse>> callback) {
    try {
      if (payload == null)
        throw new NullPointerException("Response payload is null");
      if (payload instanceof ErrorResponse) {
        ErrorResponse errorResponse = (ErrorResponse) payload;
        logger.warn(marker, "The connector responded with an error of type {}: {}", errorResponse.getError(),
            errorResponse.getErrorMessage());

        switch (errorResponse.getError()) {
          case NOT_IMPLEMENTED:
            throw new HttpException(NOT_IMPLEMENTED, "The connector is unable to process this request.");
          case CONFLICT:
            throw new HttpException(CONFLICT, "A conflict occurred when updating a feature: " + errorResponse.getErrorMessage());
          case FORBIDDEN:
            throw new HttpException(FORBIDDEN, "The user is not authorized.");
          case TOO_MANY_REQUESTS:
            throw new HttpException(TOO_MANY_REQUESTS,
                "The connector cannot process the message due to a limitation in an upstream service or a database.");
          case ILLEGAL_ARGUMENT:
            throw new HttpException(BAD_REQUEST, errorResponse.getErrorMessage());
          case TIMEOUT:
            throw new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.");
          case EXCEPTION:
          case BAD_GATEWAY:
            throw new HttpException(BAD_GATEWAY, "Connector error.");
        }
      }
      if (payload instanceof XyzResponse) {
        //noinspection rawtypes
        callback.handle(Future.succeededFuture((XyzResponse) payload));
        return;
      }

      logger.warn(marker, "The connector responded with an unexpected response type {}", payload.getClass().getSimpleName());
      callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "The connector responded with unexpected response type.")));
    } catch (HttpException e) {
      callback.handle(Future.failedFuture(e));
    }
  }

  @SuppressWarnings({"rawtypes", "UnusedAssignment"})
  private void parseResponse(final Marker marker, byte[] bytes, final Handler<AsyncResult<XyzResponse>> callback) {
    final String stringResponse = bytes == null ? null : new String(bytes);
    bytes = null; //GC may collect the bytes now.
    try {
      if (stringResponse == null || stringResponse.length() == 0)
        throw new NullPointerException("Response string is null or empty");

      final Typed payload = XyzSerializable.deserialize(stringResponse);
      if (payload instanceof RelocatedEvent) {
        processRelocatedEventAsync((RelocatedEvent) payload, ar -> {
          if (ar.failed()) {
            callback.handle(Future.failedFuture(ar.cause()));
            return;
          }
          parseResponse(marker, ar.result(), callback);
        });
      }
      else {
        parseResponse(marker, payload, callback);
      }
    } catch (NullPointerException e) {
      logger.error(marker, "Received empty response from connector \"{}\", but expected a JSON response.", getConnector().id, e);
      callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Received an empty response from the connector.")));
    } catch (JsonMappingException e) {
      logger.error(marker, "Error in the provided content {} from connector \"{}\"", stringResponse, getConnector().id, e);
      HttpException parsedError = getErrorMessage(stringResponse);
      callback.handle(Future.failedFuture(parsedError != null ? parsedError : new HttpException(BAD_GATEWAY,
          "Invalid content provided by the connector \"" + getConnector().id
          + "\": Invalid JSON type. Expected is a sub-type of XyzResponse.")));
    } catch (JsonParseException e) {
      logger.error(marker, "Error in the provided content from connector \"{}\"", getConnector().id, e);
      callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Invalid content provided by the connector \"" + getConnector().id
          + "\": Invalid JSON string. Error at line " + e.getLocation().getLineNr() + ", column " + e.getLocation().getColumnNr() + ".")));
    } catch (IOException e) {
      logger.error(marker, "Error in the provided content ", e);
      callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Cannot read input JSON string from the connector \""
          + getConnector().id + "\".")));
    } catch (Exception e) {
      logger.error(marker, "Unexpected exception while processing connector \"{}\" response: {}", getConnector().id, stringResponse, e);
      callback.handle(
          Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unexpected exception while processing connector response.")));
    }
  }

  private void processRelocatedEventAsync(RelocatedEvent relocatedEvent, Handler<AsyncResult<byte[]>> callback) {
    Service.vertx.executeBlocking(future -> {
      try {
        InputStream input = relocationClient.processRelocatedEvent(relocatedEvent);
        future.complete(ByteStreams.toByteArray(input));
      }
      catch (Exception e) {
        logger.error("An error when processing a relocated response.", e);
        future.fail(new HttpException(BAD_GATEWAY, "Unable to load the relocated event.", e));
      }
    }, ar -> {
      if (ar.failed()) {
        callback.handle(Future.failedFuture(ar.cause()));
      }
      else {
        callback.handle(Future.succeededFuture((byte[]) ar.result()));
      }
    });
  }

  /**
   * Tries to parse the stringResponse and checks for errorMessage. In case of found, it throws a new exception with the errorMessage.
   * Additionally checks if the message is related to Time Out and throws a GATEWAY_TIMEOUT. Also, if the message is not parsable at all,
   * throws an exception informing "Invalid JSON string"
   *
   * @param stringResponse the original response
   */
  private HttpException getErrorMessage(final String stringResponse) {
    try {
      final JsonNode node = XyzSerializable.DEFAULT_MAPPER.get().readTree(stringResponse);
      if (node.has("errorMessage")) {
        final String errorMessage = node.get("errorMessage").asText();
        if (errorMessage.contains("Task timed out after ")) {
          return new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.");
        }
        return new HttpException(BAD_GATEWAY, errorMessage);
      }
    } catch (Exception jpe) {
      logger.error("Invalid content provided by the connector: Invalid JSON string: " + stringResponse, jpe);
      return new HttpException(BAD_GATEWAY, "Invalid content provided by the connector");
    }
    return null;
  }
}
