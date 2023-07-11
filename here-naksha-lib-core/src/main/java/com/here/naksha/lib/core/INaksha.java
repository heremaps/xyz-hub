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
package com.here.naksha.lib.core;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.core.models.features.Space;
import com.here.naksha.lib.core.models.features.Storage;
import com.here.naksha.lib.core.models.features.Subscription;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IFeatureReader;
import com.here.naksha.lib.core.storage.IStorage;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The Naksha host interface. When an application bootstraps, it creates a Naksha host implementation and exposes it via the
 * {@link #instance} reference. The reference implementation is based upon the PostgresQL database, but alternative implementations are
 * possible, for example the Naksha extension library will fake a Naksha-Hub.
 */
@SuppressWarnings("unused")
public interface INaksha {

  /**
   * All well-known collections. Still, not all Naksha-Hubs may support them, for example the Naksha extension library currently does not
   * support any collection out of the box!
   */
  final class AdminCollections {

    /**
     * The collections for all catalogs.
     */
    public static final CollectionInfo CATALOGS = new CollectionInfo("naksha:catalogs", 0L);

    /**
     * The collections for all spaces.
     */
    public static final CollectionInfo SPACES = new CollectionInfo("naksha:spaces", 0L);

    /**
     * The collections for all subscriptions.
     */
    public static final CollectionInfo SUBSCRIPTIONS = new CollectionInfo("naksha:subscriptions", 0L);

    /**
     * The collections for all connectors.
     */
    public static final CollectionInfo CONNECTORS = new CollectionInfo("naksha:connectors", 0L);

    /**
     * The collections for all storages.
     */
    public static final CollectionInfo STORAGES = new CollectionInfo("naksha:storages", 0L);

    /**
     * The collections for all extensions.
     */
    public static final CollectionInfo EXTENSIONS = new CollectionInfo("naksha:extensions", 0L);
  }

  /**
   * Naksha version constant. The last version compatible with XYZ-Hub.
   */
  String v0_6 = "0.6.0";

  String v2_0_0 = "2.0.0";
  String v2_0_3 = "2.0.3";
  String v2_0_4 = "2.0.4";
  String v2_0_5 = "2.0.5";

  /**
   * The latest version of the naksha-extension stored in the resources.
   */
  NakshaVersion latest = NakshaVersion.of(v2_0_5);

  /**
   * The reference to the Naksha implementation provided by the host. Rather use the {@link #get()} method to get the instance.
   */
  @AvailableSince(v2_0_0)
  AtomicReference<@Nullable INaksha> instance = new AtomicReference<>();

  /**
   * Returns the reference to the Naksha implementation provided by the host.
   *
   * @return the reference to the Naksha implementation provided by the host.
   * @throws NullPointerException if the Naksha interface is not available (no host registered).
   */
  static @NotNull INaksha get() {
    final INaksha hub = instance.getPlain();
    if (hub == null) {
      throw new NullPointerException();
    }
    return hub;
  }

  /**
   * Create a new task for the given event.
   *
   * @param eventClass the class of the event-type to create a task for.
   * @param <EVENT>    the event-type.
   * @return The created task.
   * @throws XyzErrorException If the creation of the task failed for some reason.
   */
  <EVENT extends Event, TASK extends AbstractTask<EVENT>> @NotNull TASK newTask(@NotNull Class<EVENT> eventClass)
      throws XyzErrorException;

  /**
   * Returns the administration storage that is guaranteed to have all the {@link AdminCollections admin collections}. This storage does
   * have the storage number 0.
   *
   * @return the administration storage.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IStorage adminStorage();

  /**
   * Returns the extension with the given extension number.
   *
   * @param number the extension number.
   * @return the extension, if such an extension exists.
   */
  @Nullable
  Extension getExtension(int number);

  /**
   * Returns the cached reader for spaces.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Space> spaceReader();

  /**
   * Returns the cached reader for subscriptions.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Subscription> subscriptionReader();

  /**
   * Returns the cached reader for connectors.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Connector> connectorReader();

  /**
   * Returns the cached reader for storages.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Storage> storageReader();

  /**
   * Returns the cached reader for extensions.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Extension> extensionReader();
}