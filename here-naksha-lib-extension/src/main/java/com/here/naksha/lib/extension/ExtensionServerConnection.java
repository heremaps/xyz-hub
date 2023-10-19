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

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.extension.messages.ExtensionMessage;
import com.here.naksha.lib.extension.messages.ProcessEventMsg;
import com.here.naksha.lib.extension.messages.ResponseMsg;
import com.here.naksha.lib.extension.messages.SendUpstreamMsg;
import java.net.Socket;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

class ExtensionServerConnection extends Thread implements IEvent {
  private final NakshaExtSocket nakshaExtSocket;

  ExtensionServerConnection(Socket socket) {
    nakshaExtSocket = new NakshaExtSocket(socket);
    start();
  }

  @Override
  public void run() {
    final ExtensionMessage msg;
    try {
      msg = nakshaExtSocket.readMessage();
      if (msg instanceof ProcessEventMsg processEvent) {
        event = processEvent.event;
        currentLogger()
            .atInfo("Handling event with streamID {}")
            .add(event.getStreamId())
            .log();
        final IEventHandler handler =
            Objects.requireNonNull(processEvent.eventHandler).newInstance();
        final XyzResponse response = handler.processEvent(this);
        nakshaExtSocket.sendMessage(new ResponseMsg(response));
      }
    } catch (Exception e) {
      currentLogger().atError().setCause(e).log();
      e.printStackTrace();
    } finally {
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
  public @NotNull Event getRequest() {
    return event;
  }

  /**
   * Replace the event in the pipeline, returning the old even.
   *
   * @param event The new event.
   * @return The previous event.
   */
  @Override
  public @NotNull Event setRequest(@NotNull Event event) {
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
    final SendUpstreamMsg sendUpstream = new SendUpstreamMsg(event);
    try {
      nakshaExtSocket.sendMessage(sendUpstream);
      final ExtensionMessage message = nakshaExtSocket.readMessage();
      if (message instanceof ResponseMsg responseMsg) {
        return responseMsg.response;
      }
      return new ErrorResponse()
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Received invalid message from Naksha.")
          .withStreamId(event.getStreamId());
    } catch (final Throwable t) {
      currentLogger()
          .atError("Failed to process event: {}")
          .add(event)
          .setCause(t)
          .log();
      return new ErrorResponse()
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Received invalid message from Naksha.")
          .withStreamId(event.getStreamId());
    }
  }
}
