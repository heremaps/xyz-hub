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

public abstract class ClosableChildResource<PARENT extends CloseableResource<?>> extends CloseableResource<PARENT> {

  protected ClosableChildResource(@NotNull Object proxy, @NotNull PARENT parent) {
    super(proxy, parent);
  }

  protected ClosableChildResource(@NotNull Object proxy, @NotNull PARENT parent, @NotNull ReentrantLock mutex) {
    super(proxy, parent, mutex);
  }

  /**
   * Returns the parent.
   *
   * @return the parent.
   */
  @Override
  protected @NotNull PARENT parent() {
    final PARENT parent = super.parent();
    assert parent != null;
    return parent;
  }
}
