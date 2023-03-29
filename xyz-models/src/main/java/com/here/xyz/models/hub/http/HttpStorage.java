package com.here.xyz.models.hub.http;

import com.here.xyz.EventHandler;
import com.here.xyz.IEventContext;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
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
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

//
// Note: Performance wise HttpUrlConnection is totally fine.
//       It is slightly slower than some other implementations, but overall it does not make a difference for our use-case:
//       https://cwiki.apache.org/confluence/display/HttpComponents/HttpClient3vsHttpClient4vsHttpCore
//

/**
 * A processor that sends the event to a remote server using an HTTP(s) POST.
 */
public class HttpStorage extends EventHandler {

  /**
   * Creates a new HTTP event handler instance with the given parameters.
   *
   * @param connector The connector of that storage.
   * @throws XyzErrorException if initialization of the handler failed.
   */
  public HttpStorage(@NotNull Connector connector) throws XyzErrorException {
    super(connector);
    try {
      this.params = new HttpStorageParams(connector.params);
    } catch (MalformedURLException e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
    }
  }

  private final @NotNull HttpStorageParams params;

  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
    final Event event = eventContext.event();
    try {
      byte @NotNull [] bytes = Payload.compress(event.toByteArray());
      final HttpURLConnection conn = (HttpURLConnection) params.url.openConnection();
      conn.setConnectTimeout((int) params.connTimeout);
      conn.setReadTimeout((int) params.readTimeout);
      conn.setRequestMethod("POST");
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
      return Objects.requireNonNull(XyzResponse.class.cast(XyzSerializable.deserialize(rawEvent)));
    } catch (Exception e) {
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.BAD_GATEWAY)
          .withErrorMessage(e.getMessage());
    }
  }
}
