package com.here.naksha.lib.core.extension;

import static com.here.naksha.lib.core.NakshaContext.currentLogger;

import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.extension.messages.ExtensionMessage;
import com.here.naksha.lib.core.extension.messages.ProcessEventMsg;
import com.here.naksha.lib.core.extension.messages.ResponseMsg;
import com.here.naksha.lib.core.extension.messages.SendUpstreamMsg;
import com.here.naksha.lib.core.models.hub.plugins.Connector;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import java.io.IOException;
import java.net.UnknownHostException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A special proxy-handler that is internally used to forward events to a handler running in lib-extension component.
 */
@AvailableSince(INaksha.v2_0_3)
public class ExtensionHandler implements IEventHandler {
  // extension: 1234
  // className: com.here.dcu.ValidationHandler <-- IEventHandler

  /**
   * Creates a new extension handler.
   *
   * @param connector the connector that must have a valid extension number.
   */
  @AvailableSince(INaksha.v2_0_3)
  public ExtensionHandler(@NotNull Connector connector) {
    final ExtensionConfig config = INaksha.get().getExtensionById(connector.getExtension());
    if (config == null) {
      throw new IllegalArgumentException("No such extension exists: " + connector.getId());
    }
    this.connector = connector;
    this.config = config;
  }

  /**
   * Creates a new extension handler.
   *
   * @param connector the connector that must have a valid extension number.
   * @param config    the configuration to use.
   */
  @AvailableSince(INaksha.v2_0_3)
  public ExtensionHandler(@NotNull Connector connector, @NotNull ExtensionConfig config) {
    this.connector = connector;
    this.config = config;
  }

  private final @NotNull ExtensionConfig config;
  private final @NotNull Connector connector;

  @AvailableSince(INaksha.v2_0_3)
  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
    final Event event = eventContext.getEvent();
    final String host = config.url().getHost();
    try (final NakshaExtSocket nakshaExtSocket = NakshaExtSocket.connect(config)) {
      nakshaExtSocket.sendMessage(new ProcessEventMsg(connector, event));
      while (true) {
        final ExtensionMessage extMsg = nakshaExtSocket.readMessage();
        if (extMsg instanceof ResponseMsg extResponse) {
          return extResponse.response;
        }
        if (extMsg instanceof SendUpstreamMsg sendUpstream) {
          final XyzResponse xyzResponse = eventContext.sendUpstream(sendUpstream.event);
          nakshaExtSocket.sendMessage(new ResponseMsg(xyzResponse));
          // We then need to read the response again.
        } else {
          currentLogger().info("Received invalid response from Naksha extension: {}", extMsg);
          throw new XyzErrorException(XyzError.EXCEPTION, "Received invalid response from Naksha extension");
        }
      }
    } catch (UnknownHostException e) {
      return new ErrorResponse()
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Unknown host: " + host)
          .withStreamId(event.getStreamId());
    } catch (XyzErrorException e) {
      throw e;
    } catch (IOException e) {
      return new ErrorResponse()
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Communication to Naksha extension " + connector.getExtension() + " failed")
          .withStreamId(event.getStreamId());
    } catch (Exception e) {
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.ILLEGAL_ARGUMENT)
          .withErrorMessage(e.toString());
    }
  }
}
