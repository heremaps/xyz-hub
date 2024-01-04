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
package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@Deprecated
public class ModifyFeaturesResp {

  private final List<@Nullable XyzFeature> inserted;
  private final List<@Nullable XyzFeature> updated;
  private final List<@Nullable XyzFeature> deleted;

  public ModifyFeaturesResp(
      @NotNull List<@Nullable XyzFeature> inserted,
      @NotNull List<@Nullable XyzFeature> updated,
      @NotNull List<@Nullable XyzFeature> deleted) {
    this.inserted = inserted;
    this.updated = updated;
    this.deleted = deleted;
  }

  public ModifyFeaturesResp() {
    this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }

  public List<XyzFeature> getInserted() {
    return inserted;
  }

  public List<XyzFeature> getUpdated() {
    return updated;
  }

  public List<XyzFeature> getDeleted() {
    return deleted;
  }
}
