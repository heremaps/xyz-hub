package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.extension.NakshaExtSocket;
import com.here.naksha.lib.core.extension.messages.ExtensionMessage;
import com.here.naksha.lib.core.extension.messages.ProcessEvent;
import com.here.naksha.lib.core.extension.messages.ReturnResponse;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Objects;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/** The remote extension server that will be hosted separatedly from Naksha. */
@AvailableSince(INaksha.v2_0_3)
public class RemoteExtensionServer extends Thread {

  ServerSocket serverSocket;
  NakshaExtSocket nakshaSocket;

  /**
   * Initiate an instance of the remote server.
   * @param port the port to bind to, also treated as the ID of the extensions.
   */
  public RemoteExtensionServer(int port) throws IOException {
    serverSocket = new ServerSocket(port);
    nakshaSocket = new NakshaExtSocket(serverSocket.accept());
  }

  @Override
  public void run() {
    final ExtensionMessage msg;
    try {
      msg = nakshaSocket.readMessage();
      if (msg instanceof ProcessEvent processEvent) {
        final Event event = processEvent.event;
        final IEventHandler handler = Objects.requireNonNull(event.getConnector()).newInstance();
        final EventPipeline eventPipeline = new EventPipeline(handler);
        eventPipeline.setEvent(event);
        final XyzResponse response = handler.processEvent(eventPipeline);
        nakshaSocket.sendMessage(new ReturnResponse(response));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}