package com.here.naksha.lib.core.extension;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.here.naksha.lib.core.INaksha;
import java.net.MalformedURLException;
import java.net.URL;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The extension parameters.
 *
 * @param httpMethod  HTTP method to use, defaults to POST.
 * @param url         the URL to which to send the events.
 * @param connTimeout the timeout in milliseconds when trying to establish a new connection to the host.
 * @param readTimeout the timeout in milliseconds when aborting socket read, so after a connection has been established.
 * @param gzip        if the JSON payload should be GZIP compressed; {@code null} means auto-detect; {@code true} means always compress;
 *                    {@code false} means never compress.
 */
@AvailableSince(INaksha.v2_0_3)
public record ExtensionConfig(
        @NotNull String httpMethod, @NotNull URL url, int connTimeout, int readTimeout, @Nullable Boolean gzip) {

    /**
     * Create the default extension parameters.
     *
     * @param url the URL.
     */
    @AvailableSince(INaksha.v2_0_3)
    public ExtensionConfig(@NotNull URL url) {
        this("POST", url, (int) SECONDS.toMillis(5), (int) SECONDS.toMillis(60), null);
    }

    /**
     * Create the default extension parameters.
     *
     * @param url the URL.
     * @throws MalformedURLException if the given URL is malformed.
     */
    @AvailableSince(INaksha.v2_0_3)
    public ExtensionConfig(@NotNull String url) throws MalformedURLException {
        this("POST", new URL(url), (int) SECONDS.toMillis(5), (int) SECONDS.toMillis(60), null);
    }
}
