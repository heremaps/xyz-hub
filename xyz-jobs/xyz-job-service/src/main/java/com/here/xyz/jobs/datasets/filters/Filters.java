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

package com.here.xyz.jobs.datasets.filters;

import static com.here.xyz.XyzSerializable.serialize;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.util.Hasher;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Filters {

  //@TODO: Copy Filters to old impl V1 + Rewrite V2 that propertyFilter uses modelBase approach
  @JsonView({Public.class})
  private String propertyFilter;

  @JsonView({Public.class})
  private SpatialFilter spatialFilter;

  @JsonView({Public.class, Static.class})
  private SpaceContext context = DEFAULT;

  public String getPropertyFilter() {
    return propertyFilter;
  }

  public void setPropertyFilter(String propertyFilter) {
    this.propertyFilter = propertyFilter;
  }

  public Filters withPropertyFilter(String propertyFilter) {
    setPropertyFilter(propertyFilter);
    return this;
  }

  public SpatialFilter getSpatialFilter() {
    return spatialFilter;
  }

  public void setSpatialFilter(SpatialFilter spatialFilter) {
    this.spatialFilter = spatialFilter;
  }

  public Filters withSpatialFilter(SpatialFilter spatialFilter) {
    setSpatialFilter(spatialFilter);
    return this;
  }

  public SpaceContext getContext() {
    return context;
  }

  public void setContext(SpaceContext context) {
    this.context = context;
  }

  public Filters withContext(SpaceContext context) {
    setContext(context);
    return this;
  }

  @JsonIgnore
  public String getHash()
  {
   String input = "#" + (propertyFilter != null ? propertyFilter : "" ) 
                + "#" + (spatialFilter != null ? serialize(spatialFilter) : "")
                + "#";

   return Hasher.getHash(input); 
  }
}
