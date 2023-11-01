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

import com.here.naksha.lib.core.*;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.hub.EventPipelineFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NHSpaceStorageReader implements IReadSession {

  /** Singleton instance of NakshaHub storage implementation */
  protected final @NotNull INaksha nakshaHub;
  /** Runtime NakshaContext which is to be associated with read operations */
  protected final @NotNull NakshaContext context;
  /** Flag to indicate whether it has to connect to master storage instance or not */
  protected final boolean useMaster;
  /** List of Admin virtual spaces with relevant event handlers required to support event processing */
  protected final @NotNull Map<String, List<IEventHandler>> virtualSpaces;

  protected final @NotNull EventPipelineFactory pipelineFactory;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public NHSpaceStorageReader(
      final @NotNull INaksha hub,
      final @NotNull Map<String, List<IEventHandler>> virtualSpaces,
      final @NotNull EventPipelineFactory pipelineFactory,
      final @Nullable NakshaContext context,
      boolean useMaster) {
    this.nakshaHub = hub;
    this.virtualSpaces = virtualSpaces;
    this.pipelineFactory = pipelineFactory;
    this.context = (context != null) ? context : NakshaContext.currentContext();
    this.useMaster = useMaster;
  }

  /**
   * Tests whether this session is connected to the master-node.
   *
   * @return {@code true}, if this session is connected to the master-node; {@code false} otherwise.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public boolean isMasterConnect() {
    return useMaster;
  }

  /**
   * Returns the Naksha context bound to this read-connection.
   *
   * @return the Naksha context bound to this read-connection.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull NakshaContext getNakshaContext() {
    return this.context;
  }

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Execute the given read-request.
   *
   * @param readRequest input read request
   * @return the result.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull Result execute(final @NotNull ReadRequest<?> readRequest) {
    if (readRequest instanceof ReadCollections rc) {
      return executeReadCollections(rc);
    } else if (readRequest instanceof ReadFeatures rf) {
      return executeReadFeatures(rf);
    }
    throw new UnsupportedOperationException(
        "ReadRequest with unsupported type " + readRequest.getClass().getName());
  }

  private @NotNull Result executeReadCollections(final @NotNull ReadCollections rc) {
    try (final IReadSession admin = nakshaHub.getAdminStorage().newReadSession(context, useMaster)) {
      return admin.execute(rc);
    }
  }

  private @NotNull Result executeReadFeatures(final @NotNull ReadFeatures rf) {
    if (rf.getCollections().size() > 1) {
      throw new UnsupportedOperationException("Reading from multiple spaces not supported!");
    }
    if (virtualSpaces.containsKey(rf.getCollections().get(0))) {
      // Request is to read from Naksha Admin space
      return executeReadFeaturesFromAdminSpaces(rf);
    } else {
      // Request is to read from Custom space
      return executeReadFeaturesFromCustomSpaces(rf);
    }
  }

  private @NotNull Result executeReadFeaturesFromAdminSpaces(final @NotNull ReadFeatures rf) {
    // Run pipeline against virtual space
    final EventPipeline pipeline = pipelineFactory.eventPipeline();
    // add internal Admin resource specific event handlers
    for (final IEventHandler handler : virtualSpaces.get(rf.getCollections().get(0))) {
      pipeline.addEventHandler(handler);
    }
    return pipeline.sendEvent(rf);
  }

  private @NotNull Result executeReadFeaturesFromCustomSpaces(final @NotNull ReadFeatures rf) {
    // TODO : Add logic to support running pipeline for custom space
    throw new UnsupportedOperationException("ReadFeatures from custom space not supported as of now");
  }

  /**
   * Process the given notification.
   *
   * @param notification notification event
   * @return the result.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull Result process(@NotNull Notification<?> notification) {
    throw new UnsupportedOperationException("Notification processing not supported!");
  }

  /**
   * Closes the session, returns the underlying connection back to the connection pool. Any method of the session will from now on throw an
   * {@link IllegalStateException}.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void close() {}
}
