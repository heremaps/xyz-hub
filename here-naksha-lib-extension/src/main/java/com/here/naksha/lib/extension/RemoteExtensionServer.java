package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.INaksha;
import java.io.IOException;
import java.net.ServerSocket;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * The remote extension server that will be hosted separatedly from Naksha.
 */
@AvailableSince(INaksha.v2_0_3)
public class RemoteExtensionServer {

  ServerSocket serverSocket;

  /**
   * Initiate an instance of the remote server.
   *
   * @param port the port to bind to, also treated as the ID of the extensions.
   */
  public RemoteExtensionServer(int port) throws IOException {
    serverSocket = new ServerSocket(port);
    serverThread = new Thread(this::run);
    serverThread.start();
  }

  public synchronized void stop() {
    doStop = true;
    notifyAll();
  }

  private volatile boolean doStop;
  private final Thread serverThread;

  private void run()  {
    while (!doStop) {
      try {
        synchronized (this) {
          new ExtensionServerConnection(serverSocket.accept());
        }
      } catch (Exception e) {
      }
    }
    try {
      serverSocket.close();
    } catch (IOException e) {
    }
  }
}