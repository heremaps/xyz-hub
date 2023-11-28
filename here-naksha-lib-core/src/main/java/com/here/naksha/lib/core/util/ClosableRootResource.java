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
package com.here.naksha.lib.core.util;

import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class ClosableRootResource extends CloseableResource<ClosableRootResource> {

  protected ClosableRootResource(@NotNull Object proxy) {
    super(proxy, null);
  }

  protected ClosableRootResource(@NotNull Object proxy, @NotNull ReentrantLock mutex) {
    super(proxy, null, mutex);
  }

  /**
   * Returns {@code null}.
   *
   * @return {@code null}.
   */
  protected @Nullable ClosableRootResource parent() {
    return null;
  }
}
