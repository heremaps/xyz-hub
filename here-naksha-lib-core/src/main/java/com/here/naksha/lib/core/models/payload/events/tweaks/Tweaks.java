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
package com.here.naksha.lib.core.models.payload.events.tweaks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.payload.events.Sampling;
import org.jetbrains.annotations.NotNull;

/**
 * Providing this parameter only a subset of the data will be returned. This can be used for rendering higher zoom levels.
 */
@JsonSubTypes({
  @JsonSubTypes.Type(value = TweaksSimplification.class),
  @JsonSubTypes.Type(value = TweaksSampling.class, name = "sampling"),
  @JsonSubTypes.Type(value = TweaksEnsure.class, name = "ensure")
})
public abstract class Tweaks implements Typed {

  /**
   * The sampling settings; if any.
   *
   * <p>This is simplified, its called sometimes strength, then sampling and “samplingthreshold”. This is a combination of all, and we now
   * allow to either select a pre-defined name via {@code &tweaks:sampling} and to override individual values via
   * {@code &tweaks:sampling:strength}. The default selection is always “off”.
   */
  @JsonProperty
  public @NotNull Sampling sampling = Sampling.OFF;
}
