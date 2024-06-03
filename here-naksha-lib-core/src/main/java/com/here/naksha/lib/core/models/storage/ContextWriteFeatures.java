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
package com.here.naksha.lib.core.models.storage;

import com.here.naksha.lib.core.NakshaVersion;
import java.util.List;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract class representing WriteFeatures request alongwith list of features as context and list of violations.
 * Implementing class will define actual data type of context and violations.
 */
@ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
public abstract class ContextWriteFeatures<
        FEATURE,
        CTX_TYPE,
        V_TYPE,
        CODEC extends FeatureCodec<FEATURE, CODEC>,
        SELF extends WriteFeatures<FEATURE, CODEC, SELF>>
    extends WriteFeatures<FEATURE, CODEC, SELF> {

  /**
   * The list of features passed as context, as part of Write request
   */
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  private @Nullable List<@NotNull CTX_TYPE> context;

  /**
   * The list of violations passed as part of Write request
   */
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  private @Nullable List<@NotNull V_TYPE> violations;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  protected ContextWriteFeatures(
      @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory, @NotNull String collectionId) {
    super(codecFactory, collectionId);
  }

  /**
   * Creates a new write request, with list of features already supplied as part of argument
   *
   * @param codecFactory The codec factory to use when creating new feature codecs.
   * @param collectionId The identifier of the collection to write into.
   * @param features the list of features to be added to the request
   */
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public ContextWriteFeatures(
      final @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory,
      final @NotNull String collectionId,
      final @NotNull List<@NotNull CODEC> features) {
    super(codecFactory, collectionId, features);
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public @Nullable List<CTX_TYPE> getContext() {
    return context;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public void setContext(@Nullable List<CTX_TYPE> context) {
    this.context = context;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public @Nullable List<V_TYPE> getViolations() {
    return violations;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_11)
  public void setViolations(@Nullable List<V_TYPE> violations) {
    this.violations = violations;
  }
}
