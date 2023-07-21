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

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.cause;

import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.extension.messages.ExtensionMessage;
import com.here.naksha.lib.core.extension.messages.ProcessEventMsg;
import com.here.naksha.lib.core.extension.messages.ResponseMsg;
import com.here.naksha.lib.core.extension.messages.SendUpstreamMsg;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import com.here.naksha.lib.core.storage.IReadTransaction;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A special proxy-handler that is internally used to forward events to a handler running in lib-extension component.
 */
@AvailableSince(NakshaVersion.v2_0_3)
public class ExtensionHandler implements IEventHandler {
  // extension: 1234
  // className: com.here.dcu.ValidationHandler <-- IEventHandler

  /**
   * Creates a new extension handler.
   *
   * @param connector the connector that must have a valid extension number.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  public ExtensionHandler(@NotNull Connector connector) {
    final INaksha naksha = INaksha.get();
    final Extension config;
    try (final IReadTransaction tx = naksha.adminStorage().openReplicationTransaction()) {
      config = tx.readFeatures(Extension.class, NakshaAdminCollection.EXTENSIONS)
          .getFeatureById(connector.getId());
      if (config == null) {
        throw new IllegalArgumentException("No such extension exists: " + connector.getId());
      }
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
  @AvailableSince(NakshaVersion.v2_0_3)
  public ExtensionHandler(@NotNull Connector connector, @NotNull Extension config) {
    this.connector = connector;
    this.config = config;
  }

  private final @NotNull Extension config;
  private final @NotNull Connector connector;

  @AvailableSince(NakshaVersion.v2_0_3)
  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) {
    final Event event = eventContext.getEvent();
    try (final NakshaExtSocket nakshaExtSocket = NakshaExtSocket.connect(config)) {
      nakshaExtSocket.sendMessage(new ProcessEventMsg(connector, event));
      while (true) {
        final ExtensionMessage message = nakshaExtSocket.readMessage();
        if (message instanceof ResponseMsg extResponse) {
          return extResponse.response;
        }
        if (message instanceof SendUpstreamMsg sendUpstream) {
          final XyzResponse xyzResponse = eventContext.sendUpstream(sendUpstream.event);
          nakshaExtSocket.sendMessage(new ResponseMsg(xyzResponse));
          // We then need to read the response again.
        } else {
          currentLogger()
              .atInfo("Received invalid response from Naksha extension '{}': {}")
              .add(connector.getId())
              .add(message)
              .log();
          throw new XyzErrorException(XyzError.EXCEPTION, "Received invalid response from Naksha extension");
        }
      }
    } catch (final Throwable o) {
      final Throwable t = cause(o);
      if (t instanceof XyzErrorException e) {
        return new ErrorResponse(e, event.getStreamId());
      }
      currentLogger()
          .atError("Uncaught exception in extension handler '{}'")
          .add(connector.getId())
          .setCause(t)
          .log();
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.EXCEPTION)
          .withErrorMessage(t.getMessage());
    }
  }
}
