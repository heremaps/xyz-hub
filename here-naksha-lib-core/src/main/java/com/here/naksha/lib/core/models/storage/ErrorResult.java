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

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("ClassCanBeRecord")
public final class ErrorResult {
  public ErrorResult(@NotNull String errCode, @NotNull String errMessage, @NotNull Throwable exception) {
    this.errCode = errCode;
    this.errMessage = errMessage;
    this.exception = exception;
  }

  public final @NotNull String errCode;
  public final @NotNull String errMessage;
  public final @NotNull Throwable exception;
}
