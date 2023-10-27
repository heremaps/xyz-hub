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
package com.here.naksha.lib.core.models.storage;

import com.here.naksha.lib.core.NakshaVersion;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * All write requests should extend this base class.
 *
 * @param <T>    the object-type to write.
 * @param <SELF> the self-type.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public abstract class WriteRequest<T, SELF extends WriteRequest<T, SELF>> extends Request<SELF> {

  /**
   * Creates a new write request.
   *
   * @param queries the operations to execute.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  protected WriteRequest(@NotNull List<@NotNull WriteOp<T>> queries) {
    this.queries = queries;
  }

  /**
   * The queries to execute.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull List<@NotNull WriteOp<T>> queries;

  /**
   * The queries to execute.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull boolean noResults;

  /**
   * Add a modification and return this.
   *
   * @param writeOp the modification to add.
   * @return this.
   */
  public @NotNull SELF add(@NotNull WriteOp<T> writeOp) {
    queries.add(writeOp);
    return self();
  }

  /**
   * Generate default INSERT operations for the given object.
   * @param object the object to insert.
   * @return this.
   */
  public final @NotNull SELF insert(@NotNull T object) {
    queries.add(new WriteOp<>(EWriteOp.INSERT, object, false));
    return self();
  }

  // TODO: Add update, upsert, delete and purge
  // TODO: For some we need the uuid or only id (for delete and purge).
  // TODO: Add mass methods that use specific well-known features, for example in WriteFeatures we can support
  //       addAll(List<XyzFeature>) and we could read the UUID from the feature and add more logical handling
  //       to reduce the burden, when the caller simply has a list of features coming from a client!
}
