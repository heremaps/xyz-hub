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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import org.jetbrains.annotations.NotNull;

public class MockHttpServer {

  public final @NotNull HttpServer server;

  public MockHttpServer(int port) throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/", new MyHandler());
    server.setExecutor(null); // Use the default executor
    server.start();
    System.out.println("Server running on port " + port);
  }

  static class MyHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String response = """
{"type":"HealthStatus","status":"OK"}""";
      exchange.sendResponseHeaders(200, response.length());
      OutputStream outputStream = exchange.getResponseBody();
      outputStream.write(response.getBytes());
      outputStream.close();
    }
  }
}
