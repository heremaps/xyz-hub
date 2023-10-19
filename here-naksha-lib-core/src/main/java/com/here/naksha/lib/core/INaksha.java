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

import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

/**
 * The Naksha host interface. When an application bootstraps, it creates a Naksha host implementation and exposes it to the Naksha API. The
 * reference implementation is based upon the PostgresQL database, but alternative implementations are possible, for example the Naksha
 * extension library will fake a Naksha-Hub.
 */
@SuppressWarnings("unused")
public interface INaksha {

  /**
   * Returns a thin wrapper above the admin-database that adds authorization and internal event handling. Basically, this allows access to the admin collections.
   * @return the admin-storage.
   */
  @NotNull
  IStorage getAdminStorage();

  /**
   * Returns a virtual storage that maps spaces to collections and allows to execute requests in spaces.
   * @return the virtual space-storage.
   */
  @NotNull
  IStorage getSpaceStorage();

  /**
   * Returns the user defined space storage instance based on storageId as per space collection defined in Naksha admin storage.
   * @param storageId Id of the space storage
   * @return the space-storage
   */
  @NotNull
  IStorage getStorageById(final @NotNull String storageId);

  /**
   * Ask the Naksha-Hub to generate an error response.
   *
   * @param throwable The exception to convert.
   * @return The error response.
   */
  @Deprecated
  @NotNull
  ErrorResponse toErrorResponse(@NotNull Throwable throwable);

  /**
   * Create a new task and execute the given method.
   *
   * @param <RESPONSE> The response-type.
   * @param execute    The method to be executed in an {@link AbstractTask<RESPONSE>}.
   * @return The future of the response.
   */
  @Deprecated
  <RESPONSE> @NotNull Future<@NotNull RESPONSE> executeTask(@NotNull Supplier<@NotNull RESPONSE> execute);

  /**
   * Ask the Naksha host to send the given event to the given event-feature. Note that the event-feature is just the configuration a logical
   * component, the business logic of the event-feature is constructed out of the {@link EventHandler}'s configured and added into an
   * event-pipeline.
   *
   * @param event        The event to be sent.
   * @param eventFeature The feature to which to send the event.
   * @return The response future.
   */
  @Deprecated
  @NotNull
  Future<@NotNull XyzResponse> executeEvent(@NotNull Event event, @NotNull EventFeature eventFeature);

  /**
   * Returns the administration storage that is guaranteed to have all the {@link NakshaAdminCollection admin collections}. This storage
   * does have the storage number {@link NakshaAdminCollection#ADMIN_DB_NUMBER}.
   *
   * @return the administration storage.
   */
  @Deprecated
  @NotNull
  IStorage storage();

  /**
   * Returns the transaction settings with the application-id and author of the Naksha host.
   *
   * @return The transaction settings.
   */
  @Deprecated
  @NotNull
  ITransactionSettings settings();
}
