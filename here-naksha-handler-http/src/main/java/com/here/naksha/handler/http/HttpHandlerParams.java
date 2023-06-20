package com.here.naksha.handler.http;


import com.here.naksha.lib.core.EventHandlerParams;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The parameter parser. */
class HttpHandlerParams extends EventHandlerParams {

    /** Enums */
    public static final String URL = "url";

    public static final String CONN_TIMEOUT = "connTimeout";
    public static final String READ_TIMEOUT = "readTimeout";
    public static final String HTTP_METHOD = "httpMethod";
    public static final String GZIP = "gzip";
    public static final String HTTP_GET = "GET";
    public static final String HTTP_PUT = "PUT";
    public static final String HTTP_POST = "POST";
    public static final String HTTP_PATCH = "PATCH";

    /**
     * Parse the given connector params into this type-safe class.
     *
     * @param connectorParams the connector parameters.
     * @throws NullPointerException if a value is {@code null} that must not be null.
     * @throws IllegalArgumentException if a value has an invalid type, for example a map expected,
     *     and a string found.
     * @throws MalformedURLException if the given URL is invalid.
     */
    HttpHandlerParams(@NotNull Map<@NotNull String, @Nullable Object> connectorParams) throws MalformedURLException {
        url = new URL(parseValue(connectorParams, URL, String.class));
        connTimeout = parseValueWithDefault(connectorParams, CONN_TIMEOUT, TimeUnit.SECONDS.toMillis(5));
        readTimeout = parseValueWithDefault(connectorParams, READ_TIMEOUT, TimeUnit.SECONDS.toMillis(60));
        httpMethod = parseValueWithDefault(connectorParams, HTTP_METHOD, HTTP_POST);
        gzip = parseOptionalValue(connectorParams, GZIP, Boolean.class);
    }

    /** The URL to which to POST the events. */
    final @NotNull URL url;

    /** The timeout in milliseconds when trying to establish a new connection to the {@link #url}. */
    final long connTimeout;

    /** Abort read timeout in milliseconds. */
    final long readTimeout;

    /** HTTP method */
    final String httpMethod;

    /**
     * If the JSON should be gzip compressed, {@code null} means auto-detect, {@code true} means
     * always compress, {@code false} means never compress.
     */
    final Boolean gzip;
}
