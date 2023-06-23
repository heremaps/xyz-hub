package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.extension.NakshaExtSocket;
import com.here.naksha.lib.core.extension.messages.ExtensionMessage;
import com.here.naksha.lib.core.extension.messages.ProcessEvent;
import com.here.naksha.lib.core.extension.messages.ReturnResponse;
import com.here.naksha.lib.core.extension.messages.SendUpstream;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import java.io.IOException;
import java.net.Socket;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

class ExtensionServerConnection extends Thread implements IEventContext {
  private final NakshaExtSocket nakshaExtSocket;

  ExtensionServerConnection(Socket socket) throws IOException {
    nakshaExtSocket = new NakshaExtSocket(socket);
    start();
  }

  @Override
  public void run() {
    final ExtensionMessage msg;
      try {
        msg = nakshaExtSocket.readMessage();
        if (msg instanceof ProcessEvent processEvent) {
          event = processEvent.event;
          final IEventHandler handler = Objects.requireNonNull(processEvent.connector).newInstance();
          final XyzResponse response = handler.processEvent(this);
          nakshaExtSocket.sendMessage(new ReturnResponse(response));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      finally {
        nakshaExtSocket.close();
      }
  }

  private Event event;

  /**
   * Returns the current event of the underlying pipeline.
   *
   * @return the current event of the underlying pipeline.
   */
  @Override
  public @NotNull Event getEvent() {
    return event;
  }

  /**
   * Replace the event in the pipeline, returning the old even.
   *
   * @param event The new event.
   * @return The previous event.
   */
  @Override
  public @NotNull Event setEvent(@NotNull Event event) {
    var oldEvent = this.event;
    this.event = event;
    return oldEvent;
  }

  /**
   * Send the event upstream to the next event handler. If no further handler is available, the default implementation at the end of each
   * pipeline will return a not implemented error response.
   *
   * @return the generated response.
   */
  @Override
  public @NotNull XyzResponse sendUpstream() {
    final SendUpstream sendUpstream = new SendUpstream(event);
    try {
      nakshaExtSocket.sendMessage(sendUpstream);
      final ExtensionMessage message = nakshaExtSocket.readMessage();
      if (message instanceof ReturnResponse responseMsg) {
        return responseMsg.response;
      }
      return new ErrorResponse().withError(XyzError.EXCEPTION)
          .withErrorMessage("Received invalid message from Naksha.")
          .withStreamId(event.getStreamId());
    } catch (IOException e) {
      return new ErrorResponse().withError(XyzError.EXCEPTION)
          .withErrorMessage("Received invalid message from Naksha.")
          .withStreamId(event.getStreamId());
    }
  }
}
