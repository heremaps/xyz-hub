package com.here.naksha.activitylog;

import com.here.xyz.EventHandlerParams;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The parameter parser.
 */
class HttpParams extends EventHandlerParams {

  /**
   * Parse the given connector params into this type-safe class.
   *
   * @param connectorParams the connector parameters.
   * @throws NullPointerException     if a value is {@code null} that must not be null.
   * @throws IllegalArgumentException if a value has an invalid type, for example a map expected, and a string found.
   * @throws MalformedURLException    if the given URL is invalid.
   */
  HttpParams(@NotNull Map<@NotNull String, @Nullable Object> connectorParams) throws MalformedURLException {
    url = new URL(parseValue(connectorParams, "url", String.class));
    connTimeout = parseValueWithDefault(connectorParams, "connTimeout", TimeUnit.SECONDS.toMillis(5));
    readTimeout = parseValueWithDefault(connectorParams, "readTimeout", TimeUnit.SECONDS.toMillis(60));
  }

  /**
   * The URL to which to POST the events.
   */
  final @NotNull URL url;

  /**
   * The timeout in milliseconds when trying to establish a new connection to the {@link #url}.
   */
  final long connTimeout;

  /**
   * Abort read timeout in milliseconds.
   */
  final long readTimeout;
}
