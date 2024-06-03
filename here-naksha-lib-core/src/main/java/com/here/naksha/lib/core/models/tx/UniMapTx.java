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
package com.here.naksha.lib.core.models.tx;

import com.here.naksha.lib.core.models.Typed;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class UniMapTx implements Typed {

  public UniMapTx(
      long ts,
      @NotNull String txn,
      @Nullable Long seqNumber,
      @Nullable Long seqTs,
      @NotNull Map<@NotNull String, @NotNull UniMapTxComment> comments,
      @NotNull Map<@NotNull String, @NotNull UniMapTxAction> collections) {
    this.ts = ts;
    this.txn = txn;
    this.seqNumber = seqNumber;
    this.seqTs = seqTs;
    this.comments = comments;
    this.collections = collections;
  }

  public final long ts;
  public final @NotNull String txn;
  public final @Nullable Long seqNumber;
  public final @Nullable Long seqTs;
  public final @NotNull Map<@NotNull String, @NotNull UniMapTxComment> comments;
  public final @NotNull Map<@NotNull String, @NotNull UniMapTxAction> collections;
}
