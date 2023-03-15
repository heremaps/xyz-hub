package com.here.xyz;

import com.here.xyz.events.Event;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.NotModifiedResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for an event processor to read the event from an {@link InputStream} and to write the response to an {@link OutputStream}; using
 * JSON representation. This implementation handles all errors to ensure that a correct response is written to the output stream, even in an
 * error case. It can be extended on demand, for example when some extensions needed, like the context in AWS lambdas.
 */
public class IoEventProcessor<PROCESSOR extends IEventProcessor> {

  protected static final @NotNull Logger logger = LoggerFactory.getLogger(IoEventProcessor.class);

  /**
   * The maximal size of uncompressed bytes, exceeding leads to the response getting gzipped.
   */
  protected int GZIP_THRESHOLD_SIZE = 128 * 1024;

  /**
   * The minimal size of uncompressed bytes before starting the if-none-match/e-tag handling. Note that e-tag handling is expensive as it
   * requires to re-encode buffers (so it stresses the garbage collector).
   */
  protected int ETAG_THRESHOLD_SIZE = 32 * 1024;

  /**
   * Create a new IO bound event context. Requires invoking {@link #process(InputStream, OutputStream)} to start the event processing.
   *
   * @param processor the event processor to which to forward the event.
   */
  public IoEventProcessor(@NotNull PROCESSOR processor) {
    this.processor = processor;
  }

  /**
   * The bound processor.
   */
  protected final @NotNull PROCESSOR processor;

  private final AtomicBoolean processCalled = new AtomicBoolean();

  private final long START = NanoTime.now();
  private String logId = getClass().getName();
  private String logStream;

  private long logTime() {
    return NanoTime.timeSince(START, TimeUnit.MICROSECONDS);
  }

  /**
   * To be invoked to process the event. Start reading the event form the given input stream, invoke the
   * {@link IEventProcessor#processEvent(Event)} method and then encode the response and write it to the output.
   *
   * @param input  the input stream to process.
   * @param output the output stream to write the response to.
   * @throws IllegalStateException if this method has already been called.
   */
  public void process(@NotNull InputStream input, @NotNull OutputStream output) {
    if (processCalled.compareAndSet(false, true)) {
      throw new IllegalStateException("process must not be called multiple times");
    }
    try {
      final Event<?> event;
      try {
        String rawEvent;
        input = Payload.prepareInputStream(input);
        try (final Scanner scanner =
            new Scanner(new InputStreamReader(input, StandardCharsets.UTF_8))) {
          rawEvent = scanner.useDelimiter("\\A").next();
        }
        event = XyzSerializable.deserialize(rawEvent);
        if (event == null) {
          throw new NullPointerException();
        }
        event.withStartNanos(START);
        logId = event.logId();
        logStream = event.getStreamId();
        final long parsedInMillis = NanoTime.timeSince(START, TimeUnit.MILLISECONDS);
        logger.info("{}:{}:{} - Event parsed in {} ms", logId, logStream, logTime(), parsedInMillis);
        logger.debug("{}:{}:{} - Parsed event: {}", logId, logStream, logTime(), rawEvent);
        //noinspection UnusedAssignment
        rawEvent = null;
        // Note: We explicitly set the rawEvent to null to ensure that the garbage collector can collect the variables.
        //       Not doing so, could theoretically allow the compiler to keep a reference to the memory until the method left.
        final XyzResponse<?> response = processor.processEvent(event);
        writeDataOut(output, response, event.getIfNoneMatch());
      } catch (Exception e) {
        logger.warn("{}:{}:{} - Exception while parsing the event", logId, logStream, logTime(), e);
        final ErrorResponse errorResponse =
            new ErrorResponse()
                .withStreamId(logStream)
                .withError(XyzError.EXCEPTION)
                .withErrorMessage(e.getMessage());
        writeDataOut(output, errorResponse, null);
        return;
      }
      writeDataOut(output, processor.processEvent(event), event.getIfNoneMatch());
    } catch (Throwable t) {
      logger.error("{}:{}:{} - Fatal exception", logId, logStream, logTime(), t);
    }
  }

  /**
   * Write the output object to the output stream.
   */
  private void writeDataOut(
      @NotNull OutputStream output, @NotNull Typed dataOut, @Nullable String ifNoneMatch) {
    try {
      // Note: https://en.wikipedia.org/wiki/HTTP_ETag
      // TODO: We need to fix this, because a weak e-tag is perfect here. For example the order of
      //       the JSON document is not significant, therefore we should implement weak e-tags,
      //       because {"a":1,"b":2} is semantically equals to {"b":2,"a":1} !
      byte @NotNull [] bytes = dataOut.toByteArray();
      logger.info("{}:{}:{} - Write data out for response with type: {}", logId, logStream, logTime(), dataOut.getClass().getSimpleName());

      // All this effort does not make sense for small responses.
      if (bytes.length >= ETAG_THRESHOLD_SIZE) {
        if (dataOut instanceof BinaryResponse) {
          // NOTE: BinaryResponses contain an ETag automatically, nothing to calculate here
          String etag = ((BinaryResponse) dataOut).getEtag();
          if (XyzResponse.etagMatches(ifNoneMatch, etag)) {
            bytes = new NotModifiedResponse().withEtag(etag).toByteArray();
          } else if (bytes.length > GZIP_THRESHOLD_SIZE) {
            bytes = Payload.compress(bytes);
          }
        } else {
          final @NotNull String etag = XyzResponse.calculateEtagFor(bytes);
          if (XyzResponse.etagMatches(ifNoneMatch, etag)) {
            bytes = new NotModifiedResponse().withEtag(etag).toByteArray();
          } else {
            //noinspection StringBufferReplaceableByString
            final StringBuilder sb = new StringBuilder();
            sb.append(",\"etag\":\"\\\"").append(etag).append("\\\"\"}");
            final String etagJson = sb.toString();
            final byte @NotNull [] etagBytes = etagJson.getBytes(StandardCharsets.UTF_8);
            try (final ByteArrayOutputStream os =
                new ByteArrayOutputStream(bytes.length - 1 + etagBytes.length)) {
              final OutputStream targetOs =
                  (bytes.length > GZIP_THRESHOLD_SIZE ? Payload.gzip(os) : os);
              targetOs.write(bytes, 0, bytes.length - 1); // last character is '}'.
              targetOs.write(etagBytes); // basically looks like: ,"etag":"\"foo\""}
              targetOs.flush();
              targetOs.close();
              os.flush();
              os.close();
              bytes = os.toByteArray();
            } catch (Exception e) {
              logger.error("{}:{}:{} - Unexpected exception while trying to generate response bytes with e-tag",
                  logId, logStream, logTime(), e);
            }
          }
        }
      }
      output.write(bytes);
    } catch (Exception e) {
      logger.error("{}:{}:{} - Unexpected exception while writing to output stream:", logId, logStream, logTime(), e);
    }
  }
}
