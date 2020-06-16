/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "SearchForFeaturesEvent")
public class SearchForFeaturesEvent<T extends SearchForFeaturesEvent> extends QueryEvent<T> {

  private static final long DEFAULT_LIMIT = 1_000L;
  private static final long MAX_LIMIT = 100_000L;

  private long limit = DEFAULT_LIMIT;
  private boolean force2D;

  public long getLimit() {
    return limit;
  }

  @SuppressWarnings("WeakerAccess")
  public void setLimit(long limit) {
    this.limit = Math.max(1L, Math.min(limit, MAX_LIMIT));
  }

  @SuppressWarnings("unused")
  public T withLimit(long limit) {
    setLimit(limit);
    //noinspection unchecked
    return (T) this;
  }

  @SuppressWarnings("WeakerAccess")
  public boolean isForce2D() {
    return force2D;
  }

  @SuppressWarnings("WeakerAccess")
  public void setForce2D(boolean force2D) {
    this.force2D = force2D;
  }

  @SuppressWarnings("unused")
  public T withForce2D(boolean force2D) {
    setForce2D(force2D);
    //noinspection unchecked
    return (T) this;
  }
}
