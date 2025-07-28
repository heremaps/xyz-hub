/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.jobs.datasets;

import io.vertx.core.Future;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class Datasets extends DatasetDescription {

  private java.util.Map<String, DatasetDescription> datasets;

  public java.util.Map<String, DatasetDescription> getDatasets() {
    return datasets;
  }

  public void setDatasets(java.util.Map<String, DatasetDescription> datasets) {
    this.datasets = datasets;
  }

  public Datasets withDatasets(java.util.Map<String, DatasetDescription> datasets) {
    setDatasets(datasets);
    return this;
  }

  @Override
  public String getKey() {
    return getDatasets().values()
        .stream()
        .filter(dataset -> dataset.getKey() != null)
        .map(DatasetDescription::getKey)
            .findFirst()
            .orElse(null);
  }

  @Override
  public Set<String> getResourceKeys() {
    return getDatasets().values()
        .stream()
        .flatMap(dataset -> dataset.getResourceKeys().stream())
        .collect(Collectors.toSet());
  }

  @Override
  public Future<Void> prepare() {
    return Future.all(getDatasets().values()
            .stream()
            .map(ds -> ds.prepare())
            .toList())
        .mapEmpty();
  }

  public List<Entry<String, DatasetDescription>> getDatasetsByType(Class<? extends DatasetDescription> clazz) {
    return datasets.entrySet().stream()
            .filter(entry -> entry.getValue().getClass().isAssignableFrom(clazz))
            .toList();
  }
}
