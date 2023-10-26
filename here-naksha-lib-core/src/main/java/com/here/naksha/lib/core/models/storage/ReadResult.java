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

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.lib.core.util.json.Json;
import org.jetbrains.annotations.NotNull;

public abstract class ReadResult<T> extends SuccessResult implements AutoCloseable, Iterable<T> {

  protected @NotNull Json json = Json.get();

  public ReadResult(@NotNull Class<T> featureType) {
    this.featureType = featureType;
  }

  public <NT> @NotNull ReadResult<NT> withFeatureType(@NotNull Class<NT> featureClass) {
    this.featureType = featureClass;
    onFeatureTypeChange();
    //noinspection unchecked
    return (ReadResult<NT>) this;
  }

  public abstract IAdvancedReadResult<T> advanced();

  protected abstract void onFeatureTypeChange();

  protected @NotNull Class<?> featureType;

  /**
   * Convert the JSON and geometry into a real feature.
   *
   * @param jsondata The JSON-string of the feature.
   * @return The feature.
   * @throws IllegalArgumentException If parsing the JSON does not return the expected value.
   * @throws JsonProcessingException  If some error happened while parsing the JSON.
   */
  @SuppressWarnings("JavadocDeclaration")
  protected @NotNull <FEATURE> FEATURE featureOf(@NotNull String jsondata, Class<FEATURE> featureType) {
    try {
      final FEATURE feature = json.reader().forType(featureType).readValue(jsondata);
      if (feature == null) {
        throw new IllegalArgumentException("Parsing the jsondata returned null");
      }
      return feature;
    } catch (JsonProcessingException e) {
      throw unchecked(e);
    }
  }

  @Override
  public void close() {}
}
