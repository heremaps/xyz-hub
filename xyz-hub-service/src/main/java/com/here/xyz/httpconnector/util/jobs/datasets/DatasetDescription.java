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

package com.here.xyz.httpconnector.util.jobs.datasets;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.here.xyz.Typed;
import com.here.xyz.httpconnector.util.jobs.Export.Filters;
import com.here.xyz.httpconnector.util.jobs.datasets.DatasetDescription.Map;
import com.here.xyz.httpconnector.util.jobs.datasets.DatasetDescription.Space;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Map.class, name = "Map"),
    @JsonSubTypes.Type(value = Space.class, name = "Space"),
    @JsonSubTypes.Type(value = Spaces.class, name = "Spaces"),
    @JsonSubTypes.Type(value = Files.class, name = "Files")
})
public abstract class DatasetDescription implements Typed {

  /**
   * @return the (primary) key of this DatasetDescription in order to search for it in the persistence layer.
   *  Returning null means, that any other key is matched.
   */
  @JsonIgnore
  public abstract String getKey();

  public static class Map extends Identifiable {

  }

  public static class Space<T extends Space> extends Identifiable<T> implements FilteringSource<T> {

    private Filters filters;

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
  }

}
