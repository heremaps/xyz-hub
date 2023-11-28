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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A codec factory.
 */
public interface FeatureCodecFactory<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>> {

  /**
   * Returns a new instance of the codec.
   *
   * @return a new instance of the codec.
   */
  @NotNull
  CODEC newInstance();

  /**
   * Tests whether the given codec is from this factory.
   *
   * @param codec The codec to test.
   * @return {@code true} if this codec is from this factory; {@code false} otherwise.
   */
  boolean isInstance(@Nullable FeatureCodec<?, ?> codec);
}
