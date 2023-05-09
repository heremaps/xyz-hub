package com.here.xyz;

import static com.here.xyz.AbstractTask.currentTask;
import static com.here.xyz.XyzLogger.currentLogger;

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

/**
 * Base implementation for an event pipeline that read the event from an {@link InputStream} and write the response to an
 * {@link OutputStream}; using JSON representation.
 */
public class IoEventPipeline extends EventPipeline {

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
   * Create a new IO bound event pipeline. Requires invoking {@link #sendEvent(InputStream, OutputStream)} to start the event processing.
   */
  public IoEventPipeline() {
  }

  private final AtomicBoolean sendEvent = new AtomicBoolean();

  /**
   * To be invoked to process the event. Start reading the event form the given input stream, invoke the
   * {@link EventPipeline#sendEvent(Event)} method and then encode the response and write it to the output.
   *
   * @param input  the input stream to read the event from.
   * @param output the output stream to write the response to.
   * @return the response that was encoded to the output stream.
   * @throws IllegalStateException if this method has already been called.
   */
  public XyzResponse sendEvent(@NotNull InputStream input, @Nullable OutputStream output) {
    if (!sendEvent.compareAndSet(false, true)) {
      throw new IllegalStateException("process must not be called multiple times");
    }
    XyzResponse response;
    final Typed typed;
    final Event event;
    try {
      String rawEvent;
      final long START = NanoTime.now();
      input = Payload.prepareInputStream(input);
      try (final Scanner scanner =
          new Scanner(new InputStreamReader(input, StandardCharsets.UTF_8))) {
        rawEvent = scanner.useDelimiter("\\A").next();
      }
      typed = XyzSerializable.deserialize(rawEvent);
      if (typed == null) {
        throw new NullPointerException();
      }
      final long parsedInMillis = NanoTime.timeSince(START, TimeUnit.MILLISECONDS);
      if (!(typed instanceof Event)) {
        final String expected = Event.class.getSimpleName();
        final String deserialized = typed.getClass().getSimpleName();
        currentLogger().info("Event parsed in {}ms, but expected {}, deserialized {}", parsedInMillis, expected, deserialized);
        response = new ErrorResponse()
            .withStreamId(currentTask().streamId())
            .withError(XyzError.EXCEPTION)
            .withErrorMessage("Invalid event, expected " + expected + ", but found " + deserialized);
        if (output != null) {
          writeDataOut(output, response, null);
        }
        return response;
      }
      event = (Event) typed;
      event.setStartNanos(START);
      currentLogger().info("Event parsed in {}ms", parsedInMillis);
      currentLogger().debug("Event raw string: {}", rawEvent);
      //noinspection UnusedAssignment
      rawEvent = null;
      // Note: We explicitly set the rawEvent to null to ensure that the garbage collector can collect the variables.
      //       Not doing so, could theoretically allow the compiler to keep a reference to the memory until the method left.
      response = sendEvent(event);
      if (output != null) {
        writeDataOut(output, response, event.getIfNoneMatch());
      }
    } catch (Throwable e) {
      currentLogger().warn("Exception while processing the event", e);
      response = new ErrorResponse()
          .withStreamId(currentTask().streamId())
          .withError(XyzError.EXCEPTION)
          .withErrorMessage(e.getMessage());
      if (output != null) {
        writeDataOut(output, response, null);
      }
    } finally {
      sendEvent.set(false);
    }
    return response;
  }

  /**
   * Write the output object to the output stream.
   */
  private void writeDataOut(@NotNull OutputStream output, @NotNull Typed dataOut, @Nullable String ifNoneMatch) {
    try {
      // Note: https://en.wikipedia.org/wiki/HTTP_ETag
      // TODO: We need to fix this, because a weak e-tag is perfect here. For example the order of
      //       the JSON document is not significant, therefore we should implement weak e-tags,
      //       because {"a":1,"b":2} is semantically equals to {"b":2,"a":1} !
      byte @NotNull [] bytes = dataOut.toByteArray();
      currentLogger().info("Write data out for response with type: {}", dataOut.getClass().getSimpleName());

      // All this effort does not make sense for small responses.
      if (bytes.length >= ETAG_THRESHOLD_SIZE) {
        if (dataOut instanceof BinaryResponse) {
          // NOTE: BinaryResponses contain an ETag automatically, nothing to calculate here
          String etag = ((BinaryResponse) dataOut).getEtag();
          if (XyzResponse.etagMatches(ifNoneMatch, etag)) {
            final NotModifiedResponse resp = new NotModifiedResponse();
            resp.setEtag(etag);
            bytes = resp.toByteArray();
          } else if (bytes.length > GZIP_THRESHOLD_SIZE) {
            bytes = Payload.compress(bytes);
          }
        } else {
          final @NotNull String etag = XyzResponse.calculateEtagFor(bytes);
          if (XyzResponse.etagMatches(ifNoneMatch, etag)) {
            final NotModifiedResponse resp = new NotModifiedResponse();
            resp.setEtag(etag);
            bytes = resp.toByteArray();
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
              currentLogger().error("Unexpected exception while trying to generate response bytes with e-tag", e);
            }
          }
        }
      }
      output.write(bytes);
    } catch (Exception e) {
      currentLogger().error("Unexpected exception while writing to output stream:", e);
    }
  }
}