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
package com.here.naksha.lib.core.util;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

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

  @Override
  public void logStats(Logger log) {
    Map<String, Long> childrenCount = this.children.values().stream()
        .collect(groupingBy(child -> child.getClass().getSimpleName(), counting()));
    childrenCount.forEach((key, value) -> log.info(
        "[Root parent child stats => rootName,childName,count] - RootResourceChildTypeCount {} {} {}",
        this.getClass().getSimpleName(),
        key,
        value));
  }
}
