package com.here.naksha.lib.core.models.hub.plugins;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.Payload;
import com.here.naksha.lib.core.models.RemoteInvocation;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An extension is a remote event handler is an artificially deployed component of Naksha, which runs in an own process, accessed using a
 * URL. All events are “POST” (see {@link #method}) to the URL and wrapped into an {@link RemoteInvocation} object. The handler need to
 * return either an {@link Event} or an {@link XyzResponse}, which both extend {@link Payload}. If an {@link Event} is returned, this event
 * is send upstream in the event-pipeline. If an {@link XyzResponse} is returned, this response will be returned. Naksha provides everything
 * that is needed to create artificial extensions.
 */
@SuppressWarnings("unused")
@JsonTypeName(value = "RemoteEventHandler")
public class RemoteEventHandler extends EventHandler {

    @AvailableSince(INaksha.v2_0)
    public static final String METHOD = "method";

    @AvailableSince(INaksha.v2_0)
    public static final String HTTP_VERSION = "httpVersion";

    @AvailableSince(INaksha.v2_0)
    public static final String URL = "url";

    @AvailableSince(INaksha.v2_0)
    public static final String COOKIES = "cookies";

    @AvailableSince(INaksha.v2_0)
    public static final String FORWARD_COOKIES = "forwardCookies";

    @AvailableSince(INaksha.v2_0)
    public static final String HEADERS = "headers";

    @AvailableSince(INaksha.v2_0)
    public static final String FORWARD_HEADERS = "forwardHeaders";

    /**
     * The full qualified name of the class, that implements the remote procedure call.
     */
    public static final String HTTP_PROXY_CLASS_NAME = "com.here.naksha.lib.models.ExtensionHttpProxy";

    /**
     * Create new remote details.
     *
     * @param id  the identifier of the event handler.
     * @param url the URL to call.
     */
    public RemoteEventHandler(@NotNull String id, @NotNull URL url) {
        super(id, HTTP_PROXY_CLASS_NAME);
        this.url = url;
        this.method = "POST";
        this.httpVersion = "1.1";
    }

    /**
     * Create new remote details.
     *
     * @param id  the identifier of the event handler.
     * @param url the URL to call.
     * @throws MalformedURLException if the given URL is not {@code null} and malformed.
     */
    @JsonCreator
    public RemoteEventHandler(@JsonProperty(ID) @NotNull String id, @JsonProperty(URL) @NotNull String url)
            throws MalformedURLException {
        super(id, HTTP_PROXY_CLASS_NAME);
        this.url = new URL(url);
        this.method = "POST";
        this.httpVersion = "1.1";
    }

    /**
     * The HTTP method to use.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(METHOD)
    public @NotNull String method;

    /**
     * The HTTP version to use.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(HTTP_VERSION)
    public @NotNull String httpVersion;

    /**
     * The URL to contact the event handler.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(URL)
    public @NotNull URL url;

    /**
     * The cookies to send to the event handler.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(COOKIES)
    public @Nullable Map<@NotNull String, @NotNull String> cookies;

    /**
     * The name of the cookies to forward to the event handler. To set this property special rights are requires.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(FORWARD_COOKIES)
    public @Nullable List<@NotNull String> forwardCookies;

    /**
     * The HTTP headers to send to the event handler.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(HEADERS)
    public @Nullable Map<@NotNull String, @NotNull String> headers;

    /**
     * The name of the HTTP headers to forward to the event handler. To set this property special rights are requires.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(FORWARD_HEADERS)
    public @Nullable List<@NotNull String> forwardHeaders;

    @Override
    public @NotNull IEventHandler newInstance() throws Exception {
        try {
            // TODO: If the extension is remote, create an HTTP proxy instead of creating the extension directly!
            //       The HTTP proxy will need the connector and this extension object, and will then, when it receives
            // an event,
            //       create an RemoteInvocation object via "new RemoteInvocation(connector, this, event)".
            //       --> return new ExtensionHttpProxy(connector, this);
            //       Note: The ExtensionHttpProxy must implement the IEventHandler interface!
            throw new UnsupportedOperationException("HTTP Proxy implementation for remote extensions missing");
        } catch (Throwable ite) { // InvocationTargetException ite
            if (ite.getCause() instanceof Exception e) {
                throw e;
            }
            throw ite;
        }
    }
}
