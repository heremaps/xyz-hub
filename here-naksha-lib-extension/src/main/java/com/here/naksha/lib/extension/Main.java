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
      try {
        remoteExtensionServer.forceCloseSocket();
      } catch (IOException e) {
      }
      remoteExtensionServer.stop();
      System.out.println("Shutdown hook invoked.");
    }
  }
}
