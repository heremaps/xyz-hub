package com.here.naksha.lib.core.models.hub.plugins;

import com.here.naksha.lib.core.INaksha;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The extension parameters.
 */
@AvailableSince(INaksha.v2_0_3)
public record ExtensionConfig(
        /** HTTP method to use, defaults to POST. */
        @NotNull String httpMethod,
        /** The URL to which to send the events. */
        @NotNull URL url,
        /** The timeout in milliseconds when trying to establish a new connection to the host. */
        long connTimeout,
        /** The timeout in milliseconds when aborting socket read, so after a connection has been established. */
        long readTimeout,
        /**
         * If the JSON payload should be GZIP compressed; {@code null} means auto-detect; {@code true} means always compress; {@code false} means never compress.
         */
        @Nullable Boolean gzip) {

    /**
     * Create the default extension parameters.
     *
     * @param url the URL.
     */
    @AvailableSince(INaksha.v2_0_3)
    public ExtensionConfig(@NotNull URL url) {
        this("POST", url, TimeUnit.SECONDS.toMillis(5), TimeUnit.SECONDS.toMillis(60), null);
    }

    /**
     * Create the default extension parameters.
     *
     * @param url the URL.
     * @throws MalformedURLException if the given URL is malformed.
     */
    @AvailableSince(INaksha.v2_0_3)
    public ExtensionConfig(@NotNull String url) throws MalformedURLException {
        this("POST", new URL(url), TimeUnit.SECONDS.toMillis(5), TimeUnit.SECONDS.toMillis(60), null);
    }
}
