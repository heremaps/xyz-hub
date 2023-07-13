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
package com.here.naksha.lib.core.extension;

import com.here.naksha.lib.core.extension.messages.ExtensionMessage;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.core.view.ViewSerialize;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import org.jetbrains.annotations.NotNull;

/**
 * Naksha extension socket to be used by Naksha and the extension-lib.
 */
public class NakshaExtSocket implements AutoCloseable {

  /**
   * Creates a new client and connect it to the extension.
   *
   * @param config     the extension configuration.
   * @return the established socket.
   * @throws IOException          if any error occurs.
   * @throws UnknownHostException if the host name configured is unknown.
   */
  public static @NotNull NakshaExtSocket connect(@NotNull Extension config) throws IOException {
    final InetAddress hostAddress = InetAddress.getByName(config.getHost());
    final int port = config.getPort();
    if (port <= 0 || port > 65535) {
      throw new IOException("Invalid port: " + port);
    }
    final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAddress, port);
    final Socket socket = new Socket();
    socket.connect(inetSocketAddress, config.getConnTimeout());
    socket.setTcpNoDelay(true);
    return new NakshaExtSocket(socket)
        .withReadTimeout(config.getReadTimeout())
        .asClient();
  }

  /**
   * Creates a new extension socket, wrapping a native TCP/IP socket.
   *
   * @param socket the TCP/IP socket to wrap.
   * @throws IOException if acquiring the input and output streams of the socket fails.
   */
  public NakshaExtSocket(@NotNull Socket socket) throws IOException {
    this.socket = socket;
    in = new BufferedInputStream(socket.getInputStream());
    out = new BufferedOutputStream(socket.getOutputStream());
    isNew = true;
  }

  public @NotNull NakshaExtSocket withReadTimeout(int timeoutInMillis) throws IOException {
    socket.setSoTimeout(timeoutInMillis);
    return this;
  }

  private final @NotNull Socket socket;
  private final @NotNull BufferedInputStream in;
  private final @NotNull BufferedOutputStream out;
  private boolean isNew;
  private boolean isClient;

  private @NotNull NakshaExtSocket asClient() {
    this.isClient = true;
    return this;
  }

  private void sendHttpRequestHeader() throws IOException {
    //      final StringBuilder sb = new StringBuilder();
    //      sb.append("POST /naksha/extension/ HTTP/1.1\n")
    //          .append("Host: ").append(host).append('\n')
    //          .append("Connection: upgrade\n")
    //          .append("Upgrade: naksha/1.0\n")
    //          .append("Content-Type: application/json\n")
    //          .append("Content-Length: ").append(body.length)
    //          .append('\n'); // end of header
    //      final String headerString = sb.toString();
    //      byte[] header = headerString.getBytes(StandardCharsets.UTF_8);
    //      currentLogger().info("Send event: {}" + headerString);
    //      out.write(header);
    //      out.write(body_size);
    //      out.write(body);
    //      out.flush();
  }

  private void sendHttpResponseHeader() throws IOException {
    // Remove HTTP header
    //      public static final String SWITCH_STRING = "HTTP/1.1 101 Switching Protocols\n"
    //          + "Upgrade: naksha/1.0\n"
    //          + "Connection: Upgrade\n"
    //          + "\n";
    //      public static final byte[] SWITCH_BYTES = SWITCH_STRING.getBytes(StandardCharsets.UTF_8);
  }

  private void removeHttpHeader() throws IOException {}

  public @NotNull ExtensionMessage readMessage() throws IOException {
    if (isNew) {
      removeHttpHeader();
      isNew = false;
    }
    try (final Json json = Json.get()) {
      return json.reader(ViewDeserialize.Internal.class)
          .forType(ExtensionMessage.class)
          .readValue(in);
    }
  }

  public void sendMessage(@NotNull ExtensionMessage msg) throws IOException {
    if (isNew) {
      if (isClient) {
        sendHttpRequestHeader();
      } else {
        sendHttpResponseHeader();
      }
      isNew = false;
    }
    try (final Json json = Json.get()) {
      json.writer(ViewSerialize.Internal.class).writeValue(out, msg);
      out.flush();
    }
  }

  @Override
  public void close() {
    try {
      out.flush();
      out.close();
      in.close();
      socket.close();
    } catch (IOException ignore) {

    }
  }
}
