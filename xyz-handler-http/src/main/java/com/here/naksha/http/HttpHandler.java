package com.here.naksha.http;

import com.here.xyz.IEventHandler;
import com.here.xyz.IEventContext;
import com.here.xyz.models.Payload;
import com.here.xyz.models.Typed;
import com.here.xyz.models.hub.plugins.EventHandler;
import com.here.xyz.models.payload.responses.ModifiedEventResponse;
import com.here.xyz.util.json.JsonSerializable;
import com.here.xyz.models.payload.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.payload.responses.ErrorResponse;
import com.here.xyz.models.payload.responses.XyzError;
import com.here.xyz.models.payload.XyzResponse;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Scanner;
import org.jetbrains.annotations.NotNull;

/**
 * The HTTP handler that sends events to a foreign host.
 */
public class HttpHandler implements IEventHandler {

  public static final String ID = "naksha:http"; // com.here.naksha.http.HttpHandler

  /**
   * Creates a new HTTP handler.
   *
   * @param eventHandler The connector configuration.
   * @throws XyzErrorException If any error occurred.
   */
  public HttpHandler(@NotNull EventHandler eventHandler) throws XyzErrorException {
    try {
      this.params = new HttpHandlerParams(eventHandler.getProperties());
    } catch (Exception e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
    }
  }

  final @NotNull HttpHandlerParams params;

  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
    final Event event = eventContext.getEvent();
    try {
      byte @NotNull [] bytes = event.toByteArray();
      final HttpURLConnection conn = (HttpURLConnection) params.url.openConnection();
      conn.setConnectTimeout((int) params.connTimeout);
      conn.setReadTimeout((int) params.readTimeout);
      conn.setRequestMethod(params.httpMethod);
      conn.setRequestProperty("Content-type", "application/json");
      if (Boolean.TRUE.equals(params.gzip) || params.gzip == null && bytes.length >= 16384) {
        bytes = Payload.compress(event.toByteArray());
        conn.setRequestProperty("Content-encoding", "gzip");
      }
      conn.setFixedLengthStreamingMode(bytes.length);
      conn.setUseCaches(false);
      conn.setDoInput(true);
      conn.setDoOutput(true);
      final OutputStream out = conn.getOutputStream();
      out.write(bytes);
      out.flush();
      out.close();
      final InputStream in = Payload.prepareInputStream(conn.getInputStream());
      final String rawEvent;
      try (final Scanner scanner = new Scanner(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        rawEvent = scanner.useDelimiter("\\A").next();
      }
      // conn.getResponseCode() == 200
      //noinspection RedundantClassCall
      final Typed deserialized = JsonSerializable.deserialize(rawEvent);
      try {
        return Objects.requireNonNull((XyzResponse) deserialized);
      } catch (ClassCastException e) {
        final Event castedEvent = Objects.requireNonNull((Event) deserialized);
        eventContext.sendUpstream(castedEvent);
        return new ModifiedEventResponse().withEvent(castedEvent);
      }
    } catch (Exception e) {
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.ILLEGAL_ARGUMENT)
          .withErrorMessage(e.toString());
    }
  }
}
