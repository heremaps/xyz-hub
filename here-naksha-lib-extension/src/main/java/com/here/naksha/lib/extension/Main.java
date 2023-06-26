package com.here.naksha.lib.extension;

import java.io.IOException;

public class Main {

  static RemoteExtensionServer remoteExtensionServer;

  public static void main(String... args) throws IOException {
    remoteExtensionServer = new RemoteExtensionServer(Integer.parseInt(args[0]));
    final ShutdownHook shutdownHook = new ShutdownHook();
    Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook));

  }

  static class ShutdownHook implements Runnable {
    @Override
    public void run() {
      // Perform cleanup tasks or actions here
      remoteExtensionServer.stop();
    }
  }
}
