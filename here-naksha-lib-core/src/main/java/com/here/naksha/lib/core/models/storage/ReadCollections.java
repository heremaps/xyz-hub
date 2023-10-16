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

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"UseBulkOperation", "ManualArrayToCollectionCopy"})
public class ReadCollections extends ReadRequest<ReadCollections> {

  protected final @NotNull List<@NotNull String> ids = new ArrayList<>();

  public boolean readDeleted() {
    return readDeleted;
  }

  public ReadCollections withReadDeleted(boolean readDeleted) {
    this.readDeleted = readDeleted;
    return self();
  }

  protected boolean readDeleted;

  public @NotNull List<@NotNull String> getIds() {
    return ids;
  }

  public @NotNull ReadCollections withIds(@NotNull String... ids) {
    final List<@NotNull String> this_ids = this.ids;
    for (final String id : ids) {
      this_ids.add(id);
    }
    return self();
  }

  public void setIds(@NotNull List<@NotNull String> ids) {
    final List<@NotNull String> this_ids = this.ids;
    this_ids.addAll(ids);
  }
}
