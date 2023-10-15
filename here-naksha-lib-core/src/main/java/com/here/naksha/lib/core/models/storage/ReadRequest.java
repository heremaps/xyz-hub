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

public abstract class ReadRequest<SELF extends ReadRequest<SELF>> extends Request<SELF> {

  protected int fetchSize = 1000;

  public int getFetchSize() {
    return fetchSize;
  }

  public @NotNull SELF widthFetchSize(int size) {
    if (size < 1 || size > 1_000_000) {
      throw new IllegalArgumentException("size");
    }
    this.fetchSize = size;
    return self();
  }
}
