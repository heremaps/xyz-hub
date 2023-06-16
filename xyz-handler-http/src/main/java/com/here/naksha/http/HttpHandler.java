package com.here.naksha.http;

import com.here.xyz.EventHandler;
import com.here.xyz.IEventContext;
import com.here.xyz.Payload;
import com.here.xyz.util.json.JsonSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
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
public class HttpHandler extends EventHandler {

  public static final String ID = "naksha:http"; // com.here.naksha.http.HttpHandler

  /**
   * Creates a new HTTP handler.
   *
   * @param connector The connector configuration.
   * @throws XyzErrorException If any error occurred.
   */
  public HttpHandler(@NotNull Connector connector) throws XyzErrorException {
    super(connector);
    try {
      this.params = new HttpHandlerParams(connector.getParams());
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
      return Objects.requireNonNull(XyzResponse.class.cast(JsonSerializable.deserialize(rawEvent)));
    } catch (Exception e) {
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.BAD_GATEWAY)
          .withErrorMessage(e.toString());
    }
  }
}
