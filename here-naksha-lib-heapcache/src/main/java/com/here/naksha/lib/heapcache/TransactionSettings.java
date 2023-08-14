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
package com.here.naksha.lib.heapcache;

import com.here.naksha.lib.core.storage.ITransactionSettings;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class TransactionSettings implements ITransactionSettings {
  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  @Override
  public @NotNull ITransactionSettings withStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    return null;
  }

  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  @Override
  public @NotNull ITransactionSettings withLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    return null;
  }

  @Override
  public @NotNull String getAppId() {
    return null;
  }

  @Override
  public @NotNull ITransactionSettings withAppId(@NotNull String app_id) {
    return null;
  }

  @Override
  public @Nullable String getAuthor() {
    return null;
  }

  @Override
  public @NotNull ITransactionSettings withAuthor(@Nullable String author) {
    return null;
  }
}
