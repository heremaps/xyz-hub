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

package com.here.xyz.connectors;

import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.FORBIDDEN;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.decryptors.EventDecryptor;
import com.here.xyz.connectors.decryptors.EventDecryptor.Decryptors;
import com.here.xyz.events.Event;
import com.here.xyz.events.EventNotification;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.XyzError;
import com.here.xyz.util.runtime.FunctionRuntime;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

/**
 * A default implementation of a request handler that can be reused. It supports out of the box caching via e-tag.
 */
public abstract class AbstractConnectorHandler{
  private static final Logger logger = LogManager.getLogger();

  /**
   * The event-type-suffix for response notifications.
   */
  @SuppressWarnings("WeakerAccess")
  protected static final String RESPONSE = ".response";

  /**
   * The event-type-suffix for request notifications.
   */
  protected static final String REQUEST = ".request";

  /**
   * The lambda client, used for warmup.
   * Only used when running in AWS Lambda environment.
   */
  private static AWSLambda lambdaClient;

  /**
   * Environment variable for setting the custom event decryptor. Currently only KMS, PRIVATE_KEY, or DUMMY is supported
   */
  @Deprecated
  public static final String ENV_DECRYPTOR = "EVENT_DECRYPTOR";

  /**
   * The stream-id that should be added to every log output.
   */
  protected String streamId;

  /**
   * Can be used for writing Log-Entries (streamId + connectorId). The connectorId should get configured in the
   * Connector Parameters of the Connector Config.
   */
  protected TraceItem traceItem;

  /**
   * Start timestamp for logging.
   */
  private long start;

  /**
   * A flag to inform, if the lambda is running in embedded mode.
   */
  private boolean embedded = false;

  /**
   * {@link EventDecryptor} used for decrypting the parameters.
   */
  protected final EventDecryptor eventDecryptor;

  private static final String DEFAULT_STORAGE_REGION_MAPPING = "DEFAULT_STORAGE_REGION_MAPPING";
  private static final Map<String, Set<String>> allowedEventTypes;

  static {
    try {
      allowedEventTypes = Strings.isNotEmpty(System.getenv(DEFAULT_STORAGE_REGION_MAPPING))
          ? XyzSerializable.deserialize(System.getenv(DEFAULT_STORAGE_REGION_MAPPING), new TypeReference<Map<String, Set<String>>>() {})
          : null;
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing DEFAULT_STORAGE_REGION_MAPPING.", e);
    }
  }

  /**
   * Default constructor that sets the correct decryptor based on the {@see ENV_DECRYPTOR} environment variable.
   */
  protected AbstractConnectorHandler() {
    Decryptors decryptor = Decryptors.DUMMY;
    if (System.getenv(ENV_DECRYPTOR) != null) {
      try {
        decryptor = Decryptors.valueOf(System.getenv(ENV_DECRYPTOR));
      } catch (IllegalArgumentException e) {
        logger.warn("Unknown decryptor" + System.getenv(ENV_DECRYPTOR) + ". Using DummyDecryptor instead.", e);
      }
    }
    eventDecryptor = EventDecryptor.getInstance(decryptor);
  }

  /**
   * Returns the number of milliseconds that have passed since the request started (for time measuring inside the lambda).
   *
   * @return the number of milliseconds that have passed since the request started (for time measuring inside the lambda).
   */
  @SuppressWarnings("WeakerAccess")
  private long ms() {
    return System.currentTimeMillis() - start;
  }

  private static void checkEventTypeAllowed(Event event) throws ErrorResponseException {
    if (event.getSourceRegion() != null && allowedEventTypes != null
        && !Event.isAllowedEventType(allowedEventTypes, event.getClass().getSimpleName(), event.getSourceRegion()))
      throw new ErrorResponseException(FORBIDDEN, "Calls from source-region \"" + event.getSourceRegion() + "\" are not allowed to this"
          + " connector.");
  }

  public Typed handleEvent(Event event) throws Exception {
      String connectorId = null;

      start = System.currentTimeMillis();
      streamId = streamId != null ? streamId : event.getStreamId();

      if (event.getConnectorParams() != null  && event.getConnectorParams().get("connectorId") != null)
          connectorId = (String) event.getConnectorParams().get("connectorId");

      traceItem = new TraceItem(streamId, connectorId);

      checkEventTypeAllowed(event);
      initialize(event);

      return processEvent(event);
  }

  /**
   * The event processor.
   *
   * @param event the incoming event
   * @return the result of the processing operation.
   */
  protected abstract Typed processEvent(Event event) throws Exception;

  /**
   * Processes a HealthCheckEvent event.
   *
   * These type of events are sent in regular intervals to the lambda handler and could be used to keep the handler's container active and
   * the connection to the database open.
   */
  protected HealthStatus processHealthCheckEvent(HealthCheckEvent event) throws Exception {
    if (event.getWarmupCount() > 0 && !FunctionRuntime.getInstance().isRunningLocally()) {
      int warmupCount = event.getWarmupCount();
      event.setWarmupCount(0);
      byte[] newEvent = event.toByteArray();
      logger.debug("{} Calling myself. WarmupCount: {}", traceItem, warmupCount);
      List<Thread> threads = new ArrayList<>(warmupCount);
      for (int i = 0; i < warmupCount; i++) {
        if (lambdaClient == null)
          lambdaClient = AWSLambdaClientBuilder.defaultClient();
        threads.add(new Thread(() -> lambdaClient.invoke(new InvokeRequest()
                .withFunctionName(((LambdaFunctionRuntime) FunctionRuntime.getInstance()).getInvokedFunctionArn())
                .withPayload(ByteBuffer.wrap(newEvent)))));
      }
      threads.forEach(t -> t.start());
      threads.forEach(t -> {try {t.join();} catch (InterruptedException ignore){}});
    }

    if (System.currentTimeMillis() < event.getMinResponseTime() + start) {
      try {
        Thread.sleep((event.getMinResponseTime() + start) - System.currentTimeMillis());
      }
      catch (InterruptedException e) {
        throw new ErrorResponseException(EXCEPTION, e.getMessage());
      }
    }
    return new HealthStatus();
  }

  /**
   * Initializes this handler.
   *
   * @param event The event
   * @throws Exception if any error occurred.
   */
  protected abstract void initialize(Event event) throws Exception;

  /**
   * Can be used for Log-Entries: "streamId - (cid=connector -)"
   */
  public static class TraceItem {
    private String streamId;
    private String connectorId;

    public TraceItem(String streamId, String connectorId) {
      this.streamId = streamId;
      this.connectorId = connectorId;
    }

    @Override
    public String toString (){
      return (streamId != null ? streamId : "no-stream-id") + (connectorId != null ? " - cid="+connectorId+" -" : " -");
    }

    public String getStreamId() {
      return streamId;
    }

    public String getConnectorId() {
      return connectorId;
    }
  }

  protected NotificationParams getNotificationParams(EventNotification notification) throws ErrorResponseException {
    if (notification == null) {
      throw new ErrorResponseException(streamId, XyzError.NOT_IMPLEMENTED, "Unknown event type");
    }

    return new NotificationParams(
        eventDecryptor.decryptParams(notification.getParams(), notification.getSpace()),
        eventDecryptor.decryptParams(notification.getConnectorParams(), notification.getSpace()),
        eventDecryptor.decryptParams(notification.getMetadata(), notification.getSpace()),
        notification.getTid(), notification.getAid(), notification.getJwt());
  }
}
