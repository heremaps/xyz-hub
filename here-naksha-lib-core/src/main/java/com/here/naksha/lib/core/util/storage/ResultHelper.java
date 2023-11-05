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
package com.here.naksha.lib.core.util.storage;

import com.here.naksha.lib.core.models.storage.ReadResult;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ResultHelper {

  /**
   * Helper method to iterate given ReadResult and return list of features with type T limited to given limit.
   *
   * @param rr        the ReadResult which is to be read
   * @param type      the type of feature to be extracted from result
   * @param limit     the max number of features to be extracted
   * @return list of features extracted from ReadResult
   * @param <T> type of feature
   */
  public static <T> @NotNull List<T> readFeaturesFromResult(
      final @NotNull ReadResult<?> rr, final @NotNull Class<T> type, final int limit) {
    final List<T> features = new ArrayList<>();
    int cnt = 0;
    for (final T feature : rr.withFeatureType(type)) {
      features.add(feature);
      if (++cnt >= limit) {
        break;
      }
    }
    return features;
  }

  /**
   * Helper method to read single feature from ReadResult
   *
   * @param <T>  the type parameter
   * @param rr   the ReadResult to read from
   * @param type the type of feature
   * @return the feature of type T if found, else null
   */
  public static <T> @Nullable T readFeatureFromResult(final @NotNull ReadResult<?> rr, final @NotNull Class<T> type) {
    for (final T t : rr.withFeatureType(type)) {
      return t;
    }
    return null;
  }
}
