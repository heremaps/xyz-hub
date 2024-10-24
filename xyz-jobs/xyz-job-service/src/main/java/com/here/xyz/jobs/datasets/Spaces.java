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

package com.here.xyz.jobs.datasets;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.datasets.DatasetDescription.Space;
import com.here.xyz.jobs.datasets.filters.FilteringSource;
import com.here.xyz.jobs.datasets.filters.Filters;
import java.util.List;
import java.util.stream.Collectors;

public class Spaces<T extends Spaces> extends DatasetDescription implements FilteringSource<T>, CombinedDatasetDescription<Space> {

  private List<String> spaceIds;

  @JsonView({Public.class, Static.class})
  private Filters filters;

  public List<String> getSpaceIds() {
    return spaceIds;
  }

  public void setSpaceIds(List<String> spaceIds) {
    this.spaceIds = spaceIds;
  }

  public Spaces withSpaceIds(List<String> spaceIds) {
    setSpaceIds(spaceIds);
    return this;
  }

  @Override
  public Filters getFilters() {
    return filters;
  }

  @Override
  public void setFilters(Filters filters) {
    this.filters = filters;
  }

  @Override
  public T withFilters(Filters filters) {
    setFilters(filters);
    return (T) this;
  }

  public String getKey() {
    return String.join(",", spaceIds);
  }

  @Override
  public List<Space> createChildEntities() {
    return getSpaceIds()
        .stream()
        .map(spaceId -> (Space) new Space().withFilters(getFilters()).withId(spaceId))
        .collect(Collectors.toList());
  }
}
