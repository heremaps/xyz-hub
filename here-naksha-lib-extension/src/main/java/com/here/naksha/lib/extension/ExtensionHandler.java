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

import static com.here.naksha.lib.core.exceptions.UncheckedException.cause;

import com.here.naksha.lib.core.IEventContext;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaBound;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.PluginCache;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.IReadTransaction;
import com.here.naksha.lib.extension.messages.ExtensionMessage;
import com.here.naksha.lib.extension.messages.ProcessEventMsg;
import com.here.naksha.lib.extension.messages.ResponseMsg;
import com.here.naksha.lib.extension.messages.SendUpstreamMsg;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A special proxy-handler that is internally used to forward events to a handler running in lib-extension component.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_3)
public class ExtensionHandler extends NakshaBound implements IEventHandler {

  private static final Logger log = LoggerFactory.getLogger(ExtensionHandler.class);
  // extension: 1234
  // className: com.here.dcu.ValidationHandler <-- IEventHandler

  /**
   * Internally used by Naksha-Hub as replacement for {@link Connector#newInstance()}, because this will return an internal proxy handler,
   * when this is an extension connector.
   *
   * @return the event-handler.
   */
  public static @NotNull IEventHandler newInstanceOrExtensionHandler(
      @NotNull INaksha naksha, @NotNull Connector connector) {
    if (connector.getExtension() > 0) {
      return new ExtensionHandler(naksha, connector);
    }
    return PluginCache.newInstance(connector.getClassName(), IEventHandler.class, connector);
  }

  /**
   * Creates a new extension handler.
   *
   * @param naksha    The naksha host.
   * @param connector the connector that must have a valid extension number.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  public ExtensionHandler(@NotNull INaksha naksha, @NotNull Connector connector) {
    super(naksha);
    final Extension config;
    try (final IReadTransaction tx = naksha.storage().openReplicationTransaction(naksha.settings())) {
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
   * @param naksha    The naksha host.
   * @param connector the connector that must have a valid extension number.
   * @param config    the configuration to use.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  public ExtensionHandler(@NotNull INaksha naksha, @NotNull Connector connector, @NotNull Extension config) {
    super(naksha);
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
          log.atInfo()
              .setMessage("Received invalid response from Naksha extension '{}': {}")
              .addArgument(connector.getId())
              .addArgument(message)
              .log();
          throw new XyzErrorException(XyzError.EXCEPTION, "Received invalid response from Naksha extension");
        }
      }
    } catch (final Throwable o) {
      final Throwable t = cause(o);
      if (t instanceof XyzErrorException e) {
        return new ErrorResponse(e, event.getStreamId());
      }
      log.atError()
          .setMessage("Uncaught exception in extension handler '{}'")
          .addArgument(connector.getId())
          .setCause(t)
          .log();
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.EXCEPTION)
          .withErrorMessage(t.getMessage());
    }
  }
}
