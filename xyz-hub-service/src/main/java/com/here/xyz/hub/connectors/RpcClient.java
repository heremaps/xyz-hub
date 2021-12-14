/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.REQUEST_ENTITY_TOO_LARGE;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.google.common.io.ByteStreams;
import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.RelocationClient;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.RelocatedEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.RemoteFunctionClient.FunctionCall;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.HistoryStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.DhString;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.ArrayUtils;
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

    final RemoteFunctionConfig remoteFunction = connector.getRemoteFunction();
    if (remoteFunction instanceof Connector.RemoteFunctionConfig.AWSLambda) {
      this.functionClient = new LambdaFunctionClient(connector);
    }
    else if (remoteFunction instanceof Connector.RemoteFunctionConfig.Embedded) {
      this.functionClient = new EmbeddedFunctionClient(connector);
    }
    else if (remoteFunction instanceof Http) {
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
    if(!connector.active)
      throw new IllegalStateException("Related connector is not active: " + connector.id);
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

  private void invokeWithRelocation(final Marker marker, RpcContext context, byte[] bytes, boolean fireAndForget, boolean hasPriority, final Handler<AsyncResult<byte[]>> callback) {
    try {
      final Connector connector = getConnector();
      if (bytes.length > connector.capabilities.maxPayloadSize) { // If the payload is too large to send directly to the connector
        if (!connector.capabilities.relocationSupport) {
          //The size is too large, the event cannot be sent to the connector.
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
          context.functionCall = functionClient.submit(marker, ar.result(), fireAndForget, hasPriority, callback);
        });
      }
      else {
        context.functionCall = functionClient.submit(marker, bytes, fireAndForget, hasPriority, callback);
      }
    }
    catch (Exception e) {
      callback.handle(Future.failedFuture(e));
    }
  }

  private void relocateAsync(Marker marker, byte[] bytes, Handler<AsyncResult<byte[]>> callback) {
    logger.info(marker, "Relocating event. Total event byte size: {}", bytes.length);
    Service.vertx.executeBlocking(
        future -> {
          try {
            future.complete(relocationClient.relocate(marker.getName(), Payload.compress(bytes)));
          }
          catch (Exception e) {
            logger.error("An error occurred when trying to relocate the event.", e);
            future.fail(new HttpException(BAD_GATEWAY, "Unable to relocate event.", e));
          }
        },
        ar -> {
          if (ar.failed())
            callback.handle(Future.failedFuture(ar.cause()));
          else
            callback.handle(Future.succeededFuture((byte[]) ar.result()));
        }
    );
  }

  /**
   * Determines, for a given event and depending on the storage's capabilities and protocol-version, whether to expect a binary payload
   * as response for the event.
   * @param event
   * @return Whether to expect a binary response from the storage connector
   */
  private boolean expectBinaryResponse(Event event) {
    return event instanceof GetFeaturesByTileEvent
        && (((GetFeaturesByTileEvent) event).getResponseType() == MVT || ((GetFeaturesByTileEvent) event).getResponseType() == MVT_FLATTENED)
        && getConnector().capabilities.mvtSupport
        && Payload.compareVersions(getConnector().getRemoteFunction().protocolVersion, BinaryResponse.BINARY_SUPPORT_VERSION) >= 0;
  }

  /**
   * Executes an event and returns the parsed FeatureCollection response.
   *
   * @param marker the log marker
   * @param event the event
   * @param callback the callback handler
   * @param hasPriority if true the enqueuing get bypassed
   * @return The rpc context belonging to the request
   */
  @SuppressWarnings("rawtypes")
  public RpcContext execute(final Marker marker, final Event event, final boolean hasPriority, final Handler<AsyncResult<XyzResponse>> callback) {
    final Connector connector = getConnector();
    event.setConnectorParams(connector.params);
    final boolean expectBinaryResponse = expectBinaryResponse(event);
    final String eventJson = event.serialize();
    final byte[] eventBytes = eventJson.getBytes();
    final RpcContext context = new RpcContext().withRequestSize(eventBytes.length);
    logger.info(marker, "Invoking remote function \"{}\". Total uncompressed event size: {}, Event: {}", connector.id, eventBytes.length,
            preview(eventJson, 4092));

    invokeWithRelocation(marker, context, eventBytes, false, hasPriority, bytesResult -> {
      if (functionClient == null) {
        logger.warn("RpcClient for connector with ID {} was destroyed in the meantime, cancelling handling of response.",
            connector.id);
        context.cancelRequest();
      }
      if (context.cancelled)
        return;
      if (bytesResult.failed()) {
        callback.handle(Future.failedFuture(bytesResult.cause()));
        return;
      }

      // this is the original event size sent by the connector, it can be different from the payload size, in case of relocation.
      context.setResponseSize(bytesResult.result().length);
      parseResponse(marker, bytesResult.result(), expectBinaryResponse, r -> {
        if (r.failed()) {
          logger.warn(marker, "Error while handling the response from connector \"{}\".", connector.id, r.cause());
          callback.handle(Future.failedFuture(r.cause()));
          return;
        }
        callback.handle(Future.succeededFuture(r.result()));
      });
    });
    return context;
  }
  /**
   * Executes an event and returns the parsed FeatureCollection response.
   *
   * @param marker the log marker
   * @param event the event
   * @param callback the callback handler
   * @return The rpc context belonging to the request
   */
  @SuppressWarnings("rawtypes")
  public RpcContext execute(final Marker marker, final Event event, final Handler<AsyncResult<XyzResponse>> callback) {
    return execute(marker, event, false, callback);
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
   * @return The rpc context belonging to the request
   * @throws NullPointerException if this RPC client is closed or the function client it is bound to is closed.
   */
  public RpcContext send(final Marker marker, @SuppressWarnings("rawtypes") final Event event) throws NullPointerException {
    final Connector connector = getConnector();
    event.setConnectorParams(connector.params);
    final byte[] eventBytes = event.toByteArray();
    RpcContext context = new RpcContext().withRequestSize(eventBytes.length);
    invokeWithRelocation(marker, context, eventBytes, true, false, r -> {
      if (r.failed()) {
        if (r.cause() instanceof HttpException
            && ((HttpException) r.cause()).status.code() >= 400 && ((HttpException) r.cause()).status.code() <= 499) {
          logger.warn(marker, "Failed to send event to remote function {}.", connector.getRemoteFunction().id, r.cause());
        }
        else
          logger.error(marker, "Failed to send event to remote function {}.", connector.getRemoteFunction().id, r.cause());
      }
    });
    return context;
  }

  @SuppressWarnings("rawtypes")
  private void validateResponsePayload(Marker marker, final Typed payload) throws HttpException {
    if (payload == null)
      throw new NullPointerException("Response payload is null");
    if (!(payload instanceof XyzResponse)) {
      logger.warn(marker, "The connector responded with an unexpected response type {}", payload.getClass().getSimpleName());
      throw new HttpException(BAD_GATEWAY, "The connector responded with unexpected response type.");
    }
    if (payload instanceof ErrorResponse) {
      ErrorResponse errorResponse = (ErrorResponse) payload;
      logger.warn(marker, "The connector {} [{}] responded with an error of type {}: {}", getConnector().id, (getConnector().getRemoteFunction()).getClass().getSimpleName() ,errorResponse.getError(),
          errorResponse.getErrorMessage());

      switch (errorResponse.getError()) {
        case NOT_IMPLEMENTED:
          throw new HttpException(NOT_IMPLEMENTED, "The connector is unable to process this request.", errorResponse.getErrorDetails());
        case CONFLICT:
          throw new HttpException(CONFLICT, "A conflict occurred when writing a feature: " + errorResponse.getErrorMessage(), errorResponse.getErrorDetails());
        case FORBIDDEN:
          throw new HttpException(FORBIDDEN, "The user is not authorized.", errorResponse.getErrorDetails());
        case TOO_MANY_REQUESTS:
          throw new HttpException(TOO_MANY_REQUESTS,
              "The connector cannot process the message due to a limitation in an upstream service or a database.", errorResponse.getErrorDetails());
        case ILLEGAL_ARGUMENT:
          throw new HttpException(BAD_REQUEST, errorResponse.getErrorMessage(), errorResponse.getErrorDetails());
        case TIMEOUT:
          throw new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.", errorResponse.getErrorDetails());
        case EXCEPTION:
        case BAD_GATEWAY:
          throw new HttpException(BAD_GATEWAY, "Connector error.", errorResponse.getErrorDetails());
        case PAYLOAD_TO_LARGE:
          throw new HttpException(Api.RESPONSE_PAYLOAD_TOO_LARGE, DhString.format("%s %s",Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE, errorResponse.getErrorMessage()) , errorResponse.getErrorDetails());
      }
    }
  }

  private boolean isOldHealthStatus(JsonObject response) {
    return response.containsKey("status") && !response.containsKey("type");
  }

  @SuppressWarnings({"rawtypes", "UnusedAssignment"})
  private void parseResponse(final Marker marker, byte[] bytes, boolean expectBinaryResponse, final Handler<AsyncResult<XyzResponse>> callback) {
    String stringResponse = null;

    try {
      checkResponseSize(marker, bytes);

      if (Payload.isGzipped(bytes))
        bytes = Payload.decompress(bytes);
      checkUncompressedResponseSize(marker, bytes);

      if (expectBinaryResponse) {
        tryDecodeBinaryResponse(marker, bytes, callback);
        return;
      }

      stringResponse = new String(bytes);
      bytes = null; //GC may collect the bytes now.

      Typed payload;
      try {
        payload = XyzSerializable.deserialize(stringResponse);
      }
      catch (InvalidTypeIdException e) {
        JsonObject response = new JsonObject(stringResponse);

        if (!isOldHealthStatus(response)) throw e;

        //Keep backward compatibility for old HealthStatus responses
        logger.warn(marker, "Connector {} responds with an old version of the HealthStatus response.", getConnector().id);
        payload = new HealthStatus().withStatus(response.getString("status"));
      }

      if (payload instanceof RelocatedEvent) {
        //Unwrap the RelocatedEvent and download the actual content, afterwards call this method again with the unwrapped result
        processRelocatedEventAsync((RelocatedEvent) payload, ar -> {
          if (ar.failed()) {
            callback.handle(Future.failedFuture(ar.cause()));
            return;
          }
          parseResponse(marker, ar.result(), expectBinaryResponse, callback);
        });
      }
      else {
        validateResponsePayload(marker, payload);
        postProcessResponsePayload(marker, payload);
        callback.handle(Future.succeededFuture((XyzResponse) payload));
      }
    }
    catch (NullPointerException e) {
      logger.warn(marker, "Received empty response from connector \"{}\", but expected a JSON response.", getConnector().id, e);
      callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Received an empty response from the connector.")));
    }
    catch (JsonMappingException e) {
      logger.warn(marker, "Mapping error in the provided content {} from connector \"{}\".", stringResponse, getConnector().id, e);
      callback.handle(Future.failedFuture(getJsonMappingErrorMessage(stringResponse)));
    }
    catch (JsonParseException | DecodeException e) {
      logger.warn(marker, "Parsing error in the provided content {} from connector \"{}\".", stringResponse, getConnector().id, e);
      callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Invalid content provided by the connector: Invalid JSON string. "
          + (e instanceof JsonParseException ? "Error at line " + ((JsonParseException) e).getLocation().getLineNr() + ", column "
          + ((JsonParseException) e).getLocation().getColumnNr() + "." : ""))));
    }
    catch (HttpException e) {
      logger.warn(marker, "Error from connector.", e);
      callback.handle(Future.failedFuture(e));
    }
    catch (Exception e) {
      logger.warn(marker, "Unexpected exception while processing connector \"{}\" response: {}.", getConnector().id, stringResponse, e);
      callback.handle(
          Future.failedFuture(new HttpException(BAD_GATEWAY, "Unexpected exception while processing connector response.")));
    }
  }

  /**
   * Try to decode the binary response and fall back to JSON-decoding in case it fails.
   * @param marker
   * @param bytes
   * @param callback
   */
  private void tryDecodeBinaryResponse(Marker marker, byte[] bytes, Handler<AsyncResult<XyzResponse>> callback) {
    BinaryResponse binaryResponse;
    try {
      binaryResponse = BinaryResponse.fromByteArray(bytes);
    }
    catch (Exception e) {
      /*
      Could happen e.g. in the following cases:
       - Error responses which were sent by the connector but not encoded as BinaryResponse
       - Error-messages from the underlying system of the remote-function in general
       - Relocated responses
       - ...
      */
      logger.info(marker, "Expected a binary response, but did not get one. Falling back to JSON decoding.");
      parseResponse(marker, bytes, false, callback);
      return;
    }

    if (APPLICATION_JSON.equals(binaryResponse.getMimeType())) {
      //In case we got a JSON string encoded within a BinaryResponse, it needs to be un-packed and continued with the JSON-decoding
      parseResponse(marker, binaryResponse.getBytes(), false, callback);
    }
    else
      callback.handle(Future.succeededFuture(binaryResponse));
  }

  private void processRelocatedEventAsync(RelocatedEvent relocatedEvent, Handler<AsyncResult<byte[]>> callback) {
    Service.vertx.executeBlocking(
        future -> {
          try {
            InputStream input = relocationClient.processRelocatedEvent(relocatedEvent, getConnector().getRemoteFunction().getRegion());
            future.complete(ByteStreams.toByteArray(input));
          }
          catch (Exception e) {
            logger.error("An error occurred when processing a relocated response.", e);
            future.fail(new HttpException(BAD_GATEWAY, "Unable to load the relocated event.", e));
          }
        },
        ar -> {
          if (ar.failed()) {
            callback.handle(Future.failedFuture(ar.cause()));
          }
          else {
            callback.handle(Future.succeededFuture((byte[]) ar.result()));
          }
        }
    );
  }

  /**
   * Tries to parse the stringResponse and checks for errorMessage. In case of found, it throws a new exception with the errorMessage.
   * Additionally checks if the message is related to Time Out and throws a GATEWAY_TIMEOUT. Also, if the message is not parsable at all,
   * throws an exception informing "Invalid JSON string"
   *
   * @param stringResponse the original response
   */
  private HttpException getJsonMappingErrorMessage(final String stringResponse) {
    try {
      final JsonNode node = XyzSerializable.DEFAULT_MAPPER.get().readTree(stringResponse);
      if (node.has("errorMessage")) {
        final String errorMessage = node.get("errorMessage").asText();
        if (errorMessage.contains("Task timed out after ")) {
          return new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.");
        }
      }
    }
    catch (Exception e) {
      logger.warn("Unable to parse error response from connector", e);
    }

    return new HttpException(BAD_GATEWAY,
        "Invalid content provided by the connector: Invalid JSON type. Expected is a sub-type of XyzResponse.");
  }

  protected void checkResponseSize(Marker marker, byte[] bytes) throws HttpException {
    if (ArrayUtils.isEmpty(bytes))
      throw new NullPointerException("Response string is null or empty");

    // When MAX_HTTP_RESPONSE_SIZE is bigger than zero, check whether a gzipped payload is bigger than MAX_HTTP_RESPONSE_SIZE
    if (Api.MAX_HTTP_RESPONSE_SIZE > 0 && Payload.isGzipped(bytes) && bytes.length > Api.MAX_HTTP_RESPONSE_SIZE) {
      throwResponseSizeException(marker);
    }

    // When MAX_SERVICE_RESPONSE_SIZE is bigger than zero, check whether an ungzipped payload is bigger than MAX_SERVICE_RESPONSE_SIZE
    if (Api.MAX_SERVICE_RESPONSE_SIZE > 0 && bytes.length > Api.MAX_SERVICE_RESPONSE_SIZE) {
      throwResponseSizeException(marker);
    }

    // when the data from connector is uncompressed and its size is bigger than the current MAX_HTTP_RESPONSE_SIZE, compress it and check again.
    if (!Payload.isGzipped(bytes) && Api.MAX_HTTP_RESPONSE_SIZE > 0 && bytes.length > Api.MAX_HTTP_RESPONSE_SIZE) {
      // uncompressed data should be checked whether it's compression would fit under MAX_HTTP_RESPONSE_SIZE
      byte[] compressed = Payload.compress(bytes);
      if (compressed != null && compressed.length > Api.MAX_HTTP_RESPONSE_SIZE)
        throwResponseSizeException(marker);
    }
  }

  protected void checkUncompressedResponseSize(Marker marker, byte[] bytes) throws HttpException {
    if (ArrayUtils.isEmpty(bytes))
      throw new NullPointerException("Response string is null or empty");

    if (Service.configuration.MAX_UNCOMPRESSED_RESPONSE_SIZE > 0 && bytes.length > Service.configuration.MAX_UNCOMPRESSED_RESPONSE_SIZE) {
      throwResponseSizeException(marker);
    }
  }

  protected void throwResponseSizeException(Marker marker) throws HttpException {
    logger.warn(marker, "Too large payload received from the connector \"{}\"", getConnector().id);
    throw new HttpException(Api.RESPONSE_PAYLOAD_TOO_LARGE, Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE);
  }

  private void postProcessResponsePayload(Marker marker, final Typed payload) {
    if (payload instanceof FeatureCollection) {
      final FeatureCollection fc = (FeatureCollection) payload;
      // TODO copy handle property over nextPageToken, when handle is finally removed, this code won't be necessary anymore
      fc.setNextPageToken(fc.getHandle());
      return;
    }

    if (payload instanceof StatisticsResponse) {
      final StatisticsResponse sr = (StatisticsResponse) payload;
      // TODO copy byteSize property over dataSize, when byteSize is finally removed, this code won't be necessary anymore
      sr.setDataSize(sr.getByteSize());
      return;
    }

    if (payload instanceof HistoryStatisticsResponse) {
      final HistoryStatisticsResponse hsr = (HistoryStatisticsResponse) payload;
      // TODO copy byteSize property over dataSize, when byteSize is finally removed, this code won't be necessary anymore
      hsr.setDataSize(hsr.getByteSize());
      return;
    }
  }

  public static class RpcContext {
    private int requestSize = -1;
    private int responseSize = -1;
    private volatile boolean cancelled = false;
    private FunctionCall functionCall;

    public void cancelRequest() {
      cancelled = true;
      functionCall.cancel();
    }

    public int getRequestSize() {
      return requestSize;
    }

    public void setRequestSize(int requestSize) {
      this.requestSize = requestSize;
    }

    public int getResponseSize() {
      return responseSize;
    }

    public void setResponseSize(int responseSize) {
      this.responseSize = responseSize;
    }

    public RpcContext withRequestSize(int requestSize) {
      setRequestSize(requestSize);
      return this;
    }

    public RpcContext withResponseSize(int responseSize) {
      setResponseSize(responseSize);
      return this;
    }
  }
}
