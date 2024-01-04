/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

public class ReadTransactionsByComment extends ReadRequest<ReadTransactionsByComment> {

  protected final @NotNull List<@NotNull String> commentIds = new ArrayList<>();

  public @NotNull List<@NotNull String> getCommentIds() {
    return commentIds;
  }

  @SuppressWarnings({"UseBulkOperation", "ManualArrayToCollectionCopy"})
  public @NotNull ReadTransactionsByComment withCommentIds(@NotNull String... ids) {
    for (final String id : ids) {
      commentIds.add(id);
    }
    return this;
  }

  public @NotNull ReadTransactionsByComment withCommentIds(@NotNull List<@NotNull String> ids) {
    commentIds.addAll(ids);
    return this;
  }

  public void setCommentIds(@NotNull List<@NotNull String> ids) {
    commentIds.addAll(ids);
  }
}
