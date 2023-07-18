/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.extension;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.NakshaVersion;
import java.io.IOException;
import java.net.ServerSocket;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * The remote extension server that will be hosted separately from Naksha.
 */
@AvailableSince(NakshaVersion.v2_0_3)
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

  void forceCloseSocket() {
    try {
      serverSocket.close();
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  private volatile boolean doStop;
  private final Thread serverThread;

  private void run() {
    while (!doStop) {
      try {
        synchronized (this) {
          new ExtensionServerConnection(serverSocket.accept());
        }
      } catch (final Throwable t) {
        currentLogger()
            .atError("Uncaught exception while accepting connection")
            .setCause(t)
            .log();
      }
    }
    try {
      serverSocket.close();
    } catch (final Throwable t) {
      currentLogger()
          .atWarn("Failed to gracefully close the server socket")
          .setCause(t)
          .log();
    }
  }
}
