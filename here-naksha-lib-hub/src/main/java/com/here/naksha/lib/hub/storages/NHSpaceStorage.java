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
package com.here.naksha.lib.hub.storages;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.storage.*;
import com.here.naksha.lib.handlers.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NHSpaceStorage implements IStorage {

  protected final @NotNull INaksha nakshaHub;

  /** List of Admin virtual spaces with relevant event handlers required to support event processing */
  protected final @NotNull Map<String, List<IEventHandler>> virtualSpaces;

  public NHSpaceStorage(final @NotNull INaksha hub) {
    this.nakshaHub = hub;
    this.virtualSpaces = configureVirtualSpaces(hub);
  }

  private @NotNull Map<String, List<IEventHandler>> configureVirtualSpaces(final @NotNull INaksha hub) {
    final Map<String, List<IEventHandler>> adminSpaces = new HashMap<>();
    // common auth handler
    final IEventHandler authHandler = new AuthorizationEventHandler(hub);
    // add event handlers for each admin space
    for (final String spaceId : NakshaAdminCollection.ALL) {
      adminSpaces.put(
          spaceId,
          switch (spaceId) {
            case NakshaAdminCollection.CONFIGS -> List.of(authHandler, new IntHandlerForConfigs(hub));
            case NakshaAdminCollection.SPACES -> List.of(authHandler, new IntHandlerForSpaces(hub));
            case NakshaAdminCollection.SUBSCRIPTIONS -> List.of(
                authHandler, new IntHandlerForSubscriptions(hub));
            case NakshaAdminCollection.EVENT_HANDLERS -> List.of(
                authHandler, new IntHandlerForEventHandlers(hub));
            case NakshaAdminCollection.STORAGES -> List.of(authHandler, new IntHandlerForStorages(hub));
            case NakshaAdminCollection.EXTENSIONS -> List.of(authHandler, new IntHandlerForExtensions(hub));
            default -> throw unchecked(new Exception("Unsupported virtual space " + spaceId));
          });
    }
    return adminSpaces;
  }

  /**
   * Initializes the storage, create the transaction table, install needed scripts and extensions.
   */
  @Override
  public void initStorage() {
    nakshaHub.getAdminStorage().initStorage();
  }

  /**
   * Starts the maintainer thread that will take about history garbage collection, sequencing and other background jobs.
   */
  @Override
  public void startMaintainer() {
    nakshaHub.getAdminStorage().startMaintainer();
  }

  /**
   * Blocking call to perform maintenance tasks right now. One-time maintenance.
   */
  @Override
  public void maintainNow() {
    nakshaHub.getAdminStorage().maintainNow();
  }

  /**
   * Stops the maintainer thread.
   */
  @Override
  public void stopMaintainer() {
    nakshaHub.getAdminStorage().stopMaintainer();
  }

  @Override
  public @NotNull IWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHSpaceStorageWriter(this.nakshaHub, virtualSpaces, context, useMaster);
  }

  @Override
  public @NotNull IReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHSpaceStorageReader(this.nakshaHub, virtualSpaces, context, useMaster);
  }
}
