package com.here.naksha.lib.core.models.hub.plugins;

import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.Payload;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A special internal proxy-handler that is internally used to forward events to a handler running in lib-extension component.
 */
@AvailableSince(INaksha.v2_0_3)
class ExtensionHandler implements IEventHandler {

    /**
     * Creates a new extension handler.
     *
     * @param connector the connector that must have a valid extension number.
     */
    @AvailableSince(INaksha.v2_0_3)
    ExtensionHandler(@NotNull Connector connector) {
        final ExtensionConfig config = INaksha.get().getExtensionById(connector.getExtension());
        if (config == null) {
            throw new IllegalArgumentException("No such extension exists: " + connector.getId());
        }
        this.config = config;
    }

    @AvailableSince(INaksha.v2_0_3)
    final @NotNull ExtensionConfig config;

    @AvailableSince(INaksha.v2_0_3)
    @Override
    public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
        final Event event = eventContext.getEvent();
        try {
            // TODO: Add the connector, extension number and the class-name into the event.
            // TODO: The here-naksha-lib-extension should then use the received class-name to execute this.
            byte @NotNull [] bytes = event.toByteArray();
            final HttpURLConnection conn = (HttpURLConnection) config.url().openConnection();
            conn.setConnectTimeout((int) config.connTimeout());
            conn.setReadTimeout((int) config.readTimeout());
            conn.setRequestMethod(config.httpMethod());
            conn.setRequestProperty("Content-type", "application/json");
            if ((config.gzip() == null && bytes.length >= 16384) || Boolean.TRUE.equals(config.gzip())) {
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
            final Typed deserialized = JsonSerializable.deserialize(rawEvent);
            try {
                if (deserialized instanceof Event modifiedEvent) {
                    eventContext.sendUpstream(modifiedEvent);
                } else if (deserialized instanceof XyzResponse response) {
                    return response;
                }
                // Illegal response.
                throw new ClassCastException();
            } catch (ClassCastException e) {
                return new ErrorResponse()
                        .withError(XyzError.BAD_GATEWAY)
                        .withErrorMessage("The extension returned neither an event nor a valid XYZ Response.");
            }
        } catch (Exception e) {
            return new ErrorResponse()
                    .withStreamId(event.getStreamId())
                    .withError(XyzError.ILLEGAL_ARGUMENT)
                    .withErrorMessage(e.toString());
        }
    }
}
