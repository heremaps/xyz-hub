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
package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Signals a PostgresQL error.
 */
class PsqlError extends ErrorResult {

  PsqlError(@NotNull XyzError reason, @NotNull String message) {
    super(reason, message);
  }

  PsqlError(@NotNull XyzError reason, @NotNull String message, @Nullable Throwable exception) {
    super(reason, message, exception);
  }

  PsqlError(@NotNull XyzError reason, @NotNull String message, @Nullable PsqlCursor<?, ?> cursor) {
    super(reason, message);
    this.cursor = cursor;
  }

  PsqlError(
      @NotNull XyzError reason,
      @NotNull String message,
      @Nullable Throwable exception,
      @Nullable PsqlCursor<?, ?> cursor) {
    super(reason, message, exception);
    this.cursor = cursor;
  }
}