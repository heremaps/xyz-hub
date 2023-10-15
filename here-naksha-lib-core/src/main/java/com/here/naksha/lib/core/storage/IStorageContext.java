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
package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.NakshaContext;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public interface IStorageContext extends AutoCloseable {

  @NotNull
  NakshaContext getNakshaContext();

  void setNakshaContext(@NotNull NakshaContext context);

  @NotNull
  IStorageContext withStmtTimeout(long timeout, TimeUnit timeUnit);

  @NotNull
  IStorageContext withLockTimeout(long timeout, TimeUnit timeUnit);

  @NotNull
  IMasterTransaction openMasterTransaction();

  @NotNull
  IReadTransaction openReadTransaction();

  @NotNull
  IAdminTransaction openAdminTransaction();

  @Override
  void close();
}
