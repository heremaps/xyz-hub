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

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ReadFeatures extends ReadRequest<ReadFeatures> {
  public ReadFeatures() {
    collections = new ArrayList<>();
  }

  protected boolean returnDeleted;
  protected boolean returnAllVersions;
  protected @NotNull List<@NotNull String> collections;

  // TODO: Review if txnOp is really needed and give us any advantage?
  // TODO: Implement spatial op!
  protected @Nullable Object spatialOp;
  protected @Nullable POp propertyOp;
  // TODO: Implement ordering!
  protected @Nullable Object orderOp;

  public @NotNull ReadFeatures withProperties(@NotNull POp propertyOp) {
    this.propertyOp = propertyOp;
    return this;
  }

  // TODO: Add more setter and getter!
}
