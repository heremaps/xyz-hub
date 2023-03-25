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

package com.here.xyz.connectors;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.decryptors.EventDecryptor;
import com.here.xyz.connectors.decryptors.EventDecryptor.Decryptors;
import com.here.xyz.events.Event;
import com.here.xyz.events.info.HealthCheckEvent;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.NotModifiedResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A default implementation of a request handler that can be reused. It supports out of the box caching via e-tag.
 */
public abstract class AbstractConnectorHandler implements RequestStreamHandler {

  /**
   * Logger
   */
  private static final Logger logger = LogManager.getLogger();

  /**
   * The event-type-suffix for response notifications.
   */
  @SuppressWarnings("WeakerAccess")
  public static final String RESPONSE = ".response";

  /**
   * The event-type-suffix for request notifications.
   */
  static final String REQUEST = ".request";

  /**
   * The relocation client
   */
  private static final RelocationClient relocationClient = new RelocationClient(System.getenv("S3_BUCKET"));

  /**
   * The lambda client, used for warmup. Only used when running in AWS Lambda environment.
   */
  private static final AWSLambda lambda = System.getenv("AWS_LAMBDA_FUNCTION_NAME") != null ? AWSLambdaClientBuilder.defaultClient() : null;

  /**
   * The number of the bytes to read from an input stream and preview as a String in the logs.
   */
  private static final int INPUT_PREVIEW_BYTE_SIZE = 4 * 1024; // 4K

  /**
   * The etag string, which is used to inject the etag value into JSON strings without the need of deserializing & re-serializing them.
   */
  private static final String ETAG_STRING = ",\"etag\":\"_\"}";

  /**
   * Environment variable for setting the custom event decryptor. Currently only KMS, PRIVATE_KEY, or DUMMY is supported
   */
  public static final String ENV_DECRYPTOR = "EVENT_DECRYPTOR";

  /**
   * Environment variable or connector param for setting the max uncompressed response size which can be written out of the connector.
   */
  public static final String MAX_UNCOMPRESSED_RESPONSE_SIZE = "MAX_UNCOMPRESSED_RESPONSE_SIZE";

  /**
   * The maximal response size in bytes that can be sent back without relocating the response.
   */
  @SuppressWarnings("WeakerAccess")
  public static int RELOCATION_THRESHOLD_SIZE = 6 * 1024 * 1024;

  /**
   * The maximal size of uncompressed bytes. Exceeding that limit leads to the response getting gzipped.
   */
  @SuppressWarnings("WeakerAccess")
  public static int GZIP_THRESHOLD_SIZE = 1024 * 1024; // 1MB

  /**
   * The context for this request.
   */
  @SuppressWarnings("WeakerAccess")
  protected Context context;

  /**
   * The stream-id that should be added to every log output.
   */
  protected String streamId;

  /**
   * Can be used for writing Log-Entries (streamId + connectorId). The connectorId should get configured in the Connector Parameters of the
   * Connector Config.
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
  final protected EventDecryptor eventDecryptor;

  /**
   * The max uncompressed response size in bytes read from connector params or environment variables
   */
  protected long maxUncompressedResponseSize = Long.MAX_VALUE;

  /**
   * Default constructor that sets the correct decryptor based on the {@see ENV_DECRYPTOR} environment variable.
   */
  public AbstractConnectorHandler() {
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
  protected long ms() {
    return System.currentTimeMillis() - start;
  }

  /**
   * Informs the connector that it is running in embedded mode.
   *
   * @param embedded true, if the lambda is running in embedded mode.
   */
  @SuppressWarnings("unused")
  public void setEmbedded(boolean embedded) {
    this.embedded = embedded;
  }

  /**
   * The entry point for processing an event.
   *
   * @param input   The Lambda function input stream
   * @param output  The Lambda function output stream
   * @param context The Lambda execution environment context object
   */
  @Override
  public void handleRequest(InputStream input, OutputStream output, Context context) {
    handleRequest(input, output, context, null);
  }

  public void handleRequest(InputStream input, OutputStream output, Context context, String streamId) {
    try {
      start = System.currentTimeMillis();
      Typed dataOut;
      this.context = context;
      String ifNoneMatch = null;
      try {
        Event event = readEvent(input);

        String connectorId = null;
        this.streamId = streamId != null ? streamId : event.getStreamId();

        if (event.getConnectorParams() != null && event.getConnectorParams().get("connectorId") != null) {
          connectorId = (String) event.getConnectorParams().get("connectorId");
        }

        maxUncompressedResponseSize = getMaxUncompressedResponseSize(event);
        traceItem = new TraceItem(this.streamId, connectorId);

        ifNoneMatch = event.getIfNoneMatch();

        initialize(event);
        dataOut = processEvent(event);
      } catch (ErrorResponseException e) {
        if (e.getErrorResponse().getStreamId() == null) {
          e.getErrorResponse().setStreamId(this.streamId);
        }
        dataOut = e.getErrorResponse();
      } catch (Exception e) {
        logger.error("{} Unexpected exception occurred:", traceItem, e);
        dataOut = new ErrorResponse()
            .withStreamId(this.streamId)
            .withError(XyzError.EXCEPTION)
            .withErrorMessage("Unexpected exception occurred.");
      } catch (OutOfMemoryError e) {
        throw e;
      }
      writeDataOut(output, dataOut, ifNoneMatch);
    } catch (Exception e) {
      logger.error("{} Unexpected exception occurred:", traceItem, e);
    } catch (OutOfMemoryError e) {
      logger.error("{} Unexpected exception occurred (heap space):", traceItem, e);
    }
  }

  /**
   * Read the connector event from the provided input stream
   *
   * @param input the input stream
   * @return the event
   */
  Event readEvent(InputStream input) throws ErrorResponseException {
    String streamPreview = null;
    try {
      input = Payload.prepareInputStream(input);
      streamPreview = previewInput(input);

      Event receivedEvent = XyzSerializable.deserialize(input);
      logger.debug("{} [{} ms] - Parsed event: {}", receivedEvent.getStreamId(), ms(), streamPreview);
      return receivedEvent;
    } catch (JsonMappingException e) {
      logger.error("{} [{} ms] - Exception {} occurred while reading the event: {}", "FATAL", ms(), e.getMessage(), streamPreview, e);
      throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "Unknown event type");
    } catch (ClassCastException e) {
      logger.error("{} [{} ms] - Exception {} occurred while reading the event: {}", "FATAL", ms(), e.getMessage(), streamPreview, e);
      throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "The input should be of type Event");
    } catch (Exception e) {
      logger.error("{} [{} ms] - Exception {} occurred while reading the event: {}", "FATAL", ms(), e.getMessage(), streamPreview, e);
      throw new ErrorResponseException(streamId, XyzError.EXCEPTION, e);
    }
  }

  /**
   * Write the output object to the output stream.
   * <p>
   * If the serialized object is too large it will be relocated and a RelocatedEvent will be written instead.
   */
  @SuppressWarnings("UnstableApiUsage")
  void writeDataOut(OutputStream output, Typed dataOut, String ifNoneMatch) {
    try {
      byte[] bytes = dataOut == null ? null : dataOut.toByteArray();

      if (bytes == null) {
        return;
      }

      logger.debug("{} Writing data out for response with type: {}", traceItem, dataOut.getClass().getSimpleName());

      if (bytes.length > maxUncompressedResponseSize) {
        logger.warn("{} Response payload was too large to send. ({} bytes)", traceItem, bytes.length);
        bytes = new ErrorResponse()
            .withStreamId(streamId)
            .withError(XyzError.PAYLOAD_TO_LARGE)
            .withErrorMessage("Response size is too large")
            .toByteArray();
      }

      if (dataOut instanceof BinaryResponse) {
        //NOTE: BinaryResponses contain an ETag automatically, nothing to calculate here
        String etag = ((BinaryResponse) dataOut).getEtag();
        if (XyzResponse.etagMatches(ifNoneMatch, etag)) {
          final NotModifiedResponse r = new NotModifiedResponse();
          r.setEtag(etag);
          bytes = r.toByteArray();
        } else if (!embedded && bytes.length > GZIP_THRESHOLD_SIZE) {
          bytes = Payload.compress(bytes);
        }
      } else {
        //Calculate ETag
        String etag = XyzResponse.calculateEtagFor(bytes);
        if (XyzResponse.etagMatches(ifNoneMatch, etag)) {
          final NotModifiedResponse r = new NotModifiedResponse();
          r.setEtag(etag);
          bytes = r.toByteArray();
        } else {
          //Handle compression and ETag injection
          byte[] etagBytes = ETAG_STRING.replace("_", etag.replace("\"", "\\\"")).getBytes();
          try (ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length - 1 + etagBytes.length)) {
            OutputStream targetOs = (!embedded && bytes.length > GZIP_THRESHOLD_SIZE ? Payload.gzip(os) : os);
            targetOs.write(bytes, 0, bytes.length - 1);
            targetOs.write(etagBytes);
            os.close();
            targetOs.close();
            bytes = os.toByteArray();
          }
        }
      }

      //Relocate
      if (!embedded && bytes.length > RELOCATION_THRESHOLD_SIZE) {
        bytes = relocationClient.relocate(streamId, Payload.isGzipped(bytes) ? bytes : Payload.compress(bytes));
      }

      //Write result
      output.write(bytes);
    } catch (Exception e) {
      logger.error("{} Unexpected exception occurred:", traceItem, e);
    }
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
   * <p>
   * This type of events are sent in regular intervals to the lambda handler and could be used to keep the handler's container active and
   * the connection to the database open.
   */
  protected XyzResponse processHealthCheckEvent(HealthCheckEvent event) {
    if (event.getWarmupCount() > 0 && this.context != null && this.context.getInvokedFunctionArn() != null && lambda != null) {
      int warmupCount = event.getWarmupCount();
      event.setWarmupCount(0);
      byte[] newEvent = event.toByteArray();
      logger.debug("{} Calling myself. WarmupCount: {}", traceItem, warmupCount);
      List<Thread> threads = new ArrayList<>(warmupCount);
      for (int i = 0; i < warmupCount; i++) {
        threads.add(new Thread(() -> lambda.invoke(new InvokeRequest()
            .withFunctionName(this.context.getInvokedFunctionArn())
            .withPayload(ByteBuffer.wrap(newEvent)))));
      }
      threads.forEach(t -> t.start());
      threads.forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException ignore) {
        }
      });
    }

    if (System.currentTimeMillis() < event.getMinResponseTime() + start) {
      try {
        Thread.sleep((event.getMinResponseTime() + start) - System.currentTimeMillis());
      } catch (InterruptedException e) {
        return new ErrorResponse()
            .withErrorMessage(e.getMessage())
            .withStreamId(streamId)
            .withError(XyzError.EXCEPTION);
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

  private String previewInput(InputStream input) throws IOException {
    input.mark(INPUT_PREVIEW_BYTE_SIZE);
    byte[] bytes = new byte[INPUT_PREVIEW_BYTE_SIZE];
    int limit = input.read(bytes);

    input.reset();

    return new String(bytes, 0, limit);
  }

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
    public String toString() {
      return (streamId != null ? streamId : "no-stream-id") + (connectorId != null ? " - cid=" + connectorId + " -" : " -");
    }

    public String getStreamId() {
      return streamId;
    }

    public String getConnectorId() {
      return connectorId;
    }
  }

  private long getMaxUncompressedResponseSize(Event event) {
    String size = System.getenv(MAX_UNCOMPRESSED_RESPONSE_SIZE);

    if (event.getConnectorParams() != null && event.getConnectorParams().containsKey(MAX_UNCOMPRESSED_RESPONSE_SIZE)) {
      size = event.getConnectorParams().get(MAX_UNCOMPRESSED_RESPONSE_SIZE).toString();
    }

    try {
      long lSize = Long.parseLong(size);
      return lSize > 0 ? lSize : Long.MAX_VALUE;
    } catch (NumberFormatException e) {
      return Long.MAX_VALUE;
    }
  }
}
