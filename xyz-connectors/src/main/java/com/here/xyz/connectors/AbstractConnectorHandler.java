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

package com.here.xyz.connectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.hash.Hashing;
import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.decryptors.EventDecryptor;
import com.here.xyz.connectors.decryptors.EventDecryptor.Decryptors;
import com.here.xyz.events.Event;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.RelocatedEvent;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.NotModifiedResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
   * The number of the bytes to read from an input stream and preview as a String in the logs.
   */
  private static final int INPUT_PREVIEW_BYTE_SIZE = 4 * 1024; // 4K
  /**
   * The etag string
   */
  private static final String ETAG_STRING = ",\"etag\":\"_\"}";
  /**
   * Environment variable for setting the custom event decryptor. Currently only KMS, PRIVATE_KEY, or DUMMY is supported
   */
  public static final String ENV_DECRYPTOR = "EVENT_DECRYPTOR";
  /**
   * The maximal response size in bytes that can be sent back without relocating the response.
   */
  @SuppressWarnings("WeakerAccess")
  public static int MAX_RESPONSE_SIZE = 6 * 1024 * 1024;
  /**
   * The maximal response size in bytes that can be sent back without relocating the response.
   */
  @SuppressWarnings("WeakerAccess")
  public static int MIN_COMPRESS_SIZE = 1024 * 1024; // 1MB
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
   * @param input The Lambda function input stream
   * @param output The Lambda function output stream
   * @param context The Lambda execution environment context object
   */
  @Override
  public void handleRequest(InputStream input, OutputStream output, Context context) {
    try {
      start = System.currentTimeMillis();
      Typed dataOut;
      this.context = context;
      String ifNoneMatch = null;
      try {
        Event event = readEvent(input);
        streamId = event.getStreamId();
        ifNoneMatch = event.getIfNoneMatch();

        if (event instanceof RelocatedEvent) {
          handleRequest(relocationClient.processRelocatedEvent((RelocatedEvent) event), output, context);
          return;
        }
        initialize(event);
        dataOut = processEvent(event);
      } catch (ErrorResponseException e) {
        dataOut = e.getErrorResponse();
      } catch (Exception e) {
        logger.error("{} - Unexpected exception occurred: {}\n{}", streamId, e.getMessage(), e.getStackTrace());
        dataOut = new ErrorResponse()
            .withStreamId(streamId)
            .withError(XyzError.EXCEPTION)
            .withErrorMessage("Unexpected exception occurred.");
      } catch (OutOfMemoryError e) {
       throw e; 
      }
      writeDataOut(output, dataOut, ifNoneMatch);
    } catch (Exception e) {
      logger.error("{} - Unexpected exception occurred: {}\n{}", streamId, e.getMessage(), e.getStackTrace());
    } catch (OutOfMemoryError e) {
      logger.error("{} - Unexpected exception occurred (heap space): {}\n{}", streamId, e.getMessage(), e.getStackTrace());
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
      logger.info("{} [{} ms] - Parsed event: {}", receivedEvent.getStreamId(), ms(), streamPreview);
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
   *
   * If the serialized object is too large it will be relocated and a RelocatedEvent will be written instead.
   */
  @SuppressWarnings("UnstableApiUsage")
  void writeDataOut(OutputStream output, Typed dataOut, String ifNoneMatch) {
    try {
      byte[] bytes = dataOut == null ? null : dataOut.serialize().getBytes();
      if (bytes == null) {
        return;
      }
      logger.info("{} - Writing data out for response with type: {}", streamId, dataOut.getClass().getSimpleName());

      // Calculate ETag
      String hash = Hashing.murmur3_128().newHasher().putBytes(bytes).hash().toString();
      byte[] etagBytes = ETAG_STRING.replace("_", hash).getBytes();
      if (hash.equals(ifNoneMatch)) {
        bytes = new NotModifiedResponse().serialize().getBytes();
      }

      // Transform: handle compression and etag injection
      try (ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length + etagBytes.length - 1)) {
        OutputStream targetOs = (!embedded && bytes.length > MIN_COMPRESS_SIZE ? Payload.gzip(os) : os);
        targetOs.write(bytes, 0, bytes.length - 1);
        targetOs.write(etagBytes);
        os.close();
        targetOs.close();
        bytes = os.toByteArray();
      }

      // Relocate
      if (!embedded && bytes.length > MAX_RESPONSE_SIZE) {
        bytes = relocationClient.relocate(streamId, bytes);
      }

      // Write result
      output.write(bytes);
    } catch (Exception e) {
      logger.error("{} - Unexpected exception occurred: {}\n{}", streamId, e.getMessage(), e.getStackTrace());
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
   *
   * This type of events are sent in regular intervals to the lambda handler and could be used to keep the handler's container active and
   * the connection to the database open.
   */
  protected XyzResponse processHealthCheckEvent(HealthCheckEvent event) {
    if (event.getMinResponseTime() > 0) {
      try {
        Thread.sleep(event.getMinResponseTime());
      }
      catch (InterruptedException ignored) {}
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
}
