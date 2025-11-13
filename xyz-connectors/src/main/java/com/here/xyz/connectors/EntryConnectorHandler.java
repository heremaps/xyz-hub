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

import static com.here.xyz.events.BinaryEvent.isXyzBinaryPayload;
import static com.here.xyz.responses.XyzError.EXCEPTION;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.BinaryEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.RelocatedEvent;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.NotModifiedResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.runtime.FunctionRuntime;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public abstract class EntryConnectorHandler extends AbstractConnectorHandler implements RequestStreamHandler {

  /**
   * Environment variable or connector param for setting the max uncompressed response size which can be written out of the connector.
   */
  public static final String MAX_UNCOMPRESSED_RESPONSE_SIZE = "MAX_UNCOMPRESSED_RESPONSE_SIZE";
  private static final Logger logger = LogManager.getLogger();
  /**
   * The relocation client
   */
  private static final RelocationClient relocationClient = new RelocationClient(System.getenv("S3_BUCKET"));
  /**
   * The number of the bytes to read from an input stream and preview as a String in the logs.
   */
  private static final int INPUT_PREVIEW_BYTE_SIZE = 4 * 1024; // 4K
  /**
   * The etag string, which is used to inject the etag value into JSON strings without the need of deserializing & re-serializing them.
   */
  private static final String ETAG_STRING = ",\"etag\":\"_\"}";
  /**
   * The maximal response size in bytes that can be sent back without relocating the response.
   */
  @SuppressWarnings("WeakerAccess")
  private static final int RELOCATION_THRESHOLD_SIZE = 6 * 1024 * 1024;
  /**
   * The maximal size of uncompressed bytes. Exceeding that limit leads to the response getting gzipped.
   */
  @SuppressWarnings("WeakerAccess")
  private static final int GZIP_THRESHOLD_SIZE = 1024 * 1024; // 1MB

  /**
   * The entry point for processing an event.
   *
   * @param input The Lambda function input stream
   * @param output The Lambda function output stream
   * @param context The Lambda execution environment context object
   */
  @Override
  public void handleRequest(InputStream input, OutputStream output, Context context) {
    String streamId = null;
    try {
      Typed dataOut;
      String ifNoneMatch = null;
      long maxUncompressedResponseSize = Long.MAX_VALUE;

      try {
        Event event = readEvent(input);
        maxUncompressedResponseSize = getMaxUncompressedResponseSize(event);

        streamId = event.getStreamId();
        new LambdaFunctionRuntime(context, streamId);

        String className = "com.here.xyz.psql.PSQLXyzConnector";
        if (event.getConnectorParams() != null && event.getConnectorParams().containsKey("className"))
          className = event.getConnectorParams().get("className").toString();
        //TBD: read from connector config
        final Class<?> mainClass = Class.forName(className);
        final AbstractConnectorHandler reqHandler = (AbstractConnectorHandler) mainClass.getDeclaredConstructor().newInstance();

        ifNoneMatch = event.getIfNoneMatch();

        if (event instanceof RelocatedEvent) {
          handleRequest(Payload.prepareInputStream(relocationClient.processRelocatedEvent((RelocatedEvent) event)), output, context);
          return;
        }

        dataOut = reqHandler.handleEvent(event);
      }
      catch (ErrorResponseException e) {
        switch (e.getErrorResponse().getError()) {
          case EXCEPTION:
          case BAD_GATEWAY:
          case TIMEOUT:
            logger.error("{} Exception in Connector:", streamId, e);
            break;
        }
        e.getErrorResponse().setStreamId(streamId);
        dataOut = e.getErrorResponse();
      }
      catch (Exception e) {
        logger.error("{} Unexpected exception occurred:", streamId, e);
        dataOut = new ErrorResponse()
            .withStreamId(streamId)
            .withError(EXCEPTION)
            .withErrorMessage("Unexpected exception occurred.");
      }

      writeDataOut(output, dataOut, ifNoneMatch, streamId, maxUncompressedResponseSize);
    }
    catch (Exception e) {
      logger.error("{} Unexpected exception occurred:", streamId, e);
    }
    catch (OutOfMemoryError e) {
      logger.error("{} Unexpected exception occurred (heap space):", streamId, e);
    }
  }

  /**
   * Read the connector event from the provided input stream
   *
   * @param input the input stream
   * @return the event
   */
  private Event readEvent(InputStream input) throws ErrorResponseException {
    String streamPreview = null;
    long start = System.currentTimeMillis();

    try {
      input = Payload.prepareInputStream(input);
      streamPreview = previewInput(input);

      Event receivedEvent = isXyzBinaryPayload(streamPreview.substring(4, 8))
          ? BinaryEvent.fromByteArray(input.readAllBytes())
          : XyzSerializable.deserialize(input);
      logger.debug("{} [{} ms] - Parsed event: {}", receivedEvent.getStreamId(), duration(start), streamPreview);
      return receivedEvent;
    }
    catch (JsonMappingException e) {
      logger.error("{} [{} ms] - Exception {} occurred while reading the event: {}", "FATAL", duration(start), e.getMessage(),
          streamPreview, e);
      throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "Unknown event type");
    }
    catch (ClassCastException e) {
      logger.error("{} [{} ms] - Exception {} occurred while reading the event: {}", "FATAL", duration(start), e.getMessage(),
          streamPreview, e);
      throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "The input should be of type Event");
    }
    catch (Exception e) {
      logger.error("{} [{} ms] - Exception {} occurred while reading the event: {}", "FATAL", duration(start), e.getMessage(),
          streamPreview, e);
      throw new ErrorResponseException(e);
    }
  }

  /**
   * Write the output object to the output stream.
   *
   * If the serialized object is too large it will be relocated and a RelocatedEvent will be written instead.
   */
  private void writeDataOut(OutputStream output, Typed dataOut, String ifNoneMatch, String streamId, long maxUncompressedResponseSize) {
    try {
      byte[] bytes = dataOut == null ? null : dataOut.toByteArray();

      if (bytes == null)
        return;

      logger.debug("{} Writing data out for response with type: {}", streamId, dataOut.getClass().getSimpleName());

      if (bytes.length > maxUncompressedResponseSize) {
        logger.warn("{} Response payload was too large to send. ({} bytes)", streamId, bytes.length);
        bytes = new ErrorResponse()
            .withStreamId(streamId)
            .withError(XyzError.PAYLOAD_TO_LARGE)
            .withErrorMessage("Response size is too large")
            .toByteArray();
      }

      final boolean runningLocally = FunctionRuntime.getInstance().isRunningLocally();
      if (dataOut instanceof BinaryResponse) {
        //NOTE: BinaryResponses contain an ETag automatically, nothing to calculate here
        String etag = ((BinaryResponse) dataOut).getEtag();
        if (XyzResponse.etagMatches(ifNoneMatch, etag))
          bytes = new NotModifiedResponse().withEtag(etag).toByteArray();
        else if (!runningLocally && bytes.length > GZIP_THRESHOLD_SIZE)
          bytes = Payload.compress(bytes);
      }
      else {
        //Calculate ETag
        String etag = XyzResponse.calculateEtagFor(bytes);
        if (XyzResponse.etagMatches(ifNoneMatch, etag))
          bytes = new NotModifiedResponse().withEtag(etag).toByteArray();
        else {
          //Handle compression and ETag injection
          byte[] etagBytes = ETAG_STRING.replace("_", etag.replace("\"", "\\\"")).getBytes();
          try (ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length - 1 + etagBytes.length)) {
            OutputStream targetOs = (!runningLocally && bytes.length > GZIP_THRESHOLD_SIZE ? Payload.gzip(os) : os);
            targetOs.write(bytes, 0, bytes.length - 1);
            targetOs.write(etagBytes);
            os.close();
            targetOs.close();
            bytes = os.toByteArray();
          }
        }
      }

      //Relocate
      if (!runningLocally && bytes.length > RELOCATION_THRESHOLD_SIZE)
        bytes = relocationClient.relocate(streamId, Payload.isGzipped(bytes) ? bytes : Payload.compress(bytes));

      //Write result
      output.write(bytes);
    }
    catch (Exception e) {
      logger.error("{} Unexpected exception occurred:", streamId, e);
    }
  }

  private String previewInput(InputStream input) throws IOException {
    input.mark(INPUT_PREVIEW_BYTE_SIZE);
    byte[] bytes = new byte[INPUT_PREVIEW_BYTE_SIZE];
    int limit = input.read(bytes);

    input.reset();

    return new String(bytes, 0, limit);
  }

  private long duration(long startTs) {
    return System.currentTimeMillis() - startTs;
  }

  private long getMaxUncompressedResponseSize(Event event) {
    String size = System.getenv(MAX_UNCOMPRESSED_RESPONSE_SIZE);

    if (event.getConnectorParams() != null && event.getConnectorParams().containsKey(MAX_UNCOMPRESSED_RESPONSE_SIZE))
      size = event.getConnectorParams().get(MAX_UNCOMPRESSED_RESPONSE_SIZE).toString();

    try {
      long lSize = Long.parseLong(size);
      return lSize > 0 ? lSize : Long.MAX_VALUE;
    }
    catch (NumberFormatException e) {
      return Long.MAX_VALUE;
    }
  }
}
