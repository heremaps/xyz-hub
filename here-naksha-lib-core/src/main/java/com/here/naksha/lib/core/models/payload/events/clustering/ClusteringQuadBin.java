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
package com.here.naksha.lib.core.models.payload.events.clustering;

import static com.here.naksha.lib.core.util.json.JsonSerializable.format;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.naksha.lib.core.exceptions.ParameterError;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The "quadbin" algorithm takes the geometry input from the request (for example, quadkey / bbox..)
 * and count the features in it.
 *
 * <p>This clustering mode works also for large spaces and can be used for getting an overview where
 * data is present in a given space. Furthermore, a property filter on one property is applicable.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "QuadBinClustering")
public class ClusteringQuadBin extends Clustering {

  /** The quad resolution [0,4]. The alternative (deprecated) name is “resolution”. */
  private @Nullable Integer relativeResolution;

  public @Nullable Integer getRelativeResolution() {
    if (relativeResolution != null && (relativeResolution < 0 || relativeResolution > 4)) {
      return null;
    }
    return relativeResolution;
  }

  public void setRelativeResolution(@Nullable Integer relativeResolution) throws ParameterError {
    if (relativeResolution != null && (relativeResolution < 0 || relativeResolution > 4)) {
      throw new ParameterError(
          format("relativeResolution must be between %d and %d, but was %d", 0, 4, relativeResolution));
    }
    this.relativeResolution = relativeResolution;
  }

  // TODO: What is the value range and meaning of this?
  private @Nullable Integer absoluteResolution;

  public @Nullable Integer getAbsoluteResolution() {
    if (absoluteResolution != null && (absoluteResolution < 0 || absoluteResolution > 100)) {
      return null;
    }
    return absoluteResolution;
  }

  public void setAbsoluteResolution(@Nullable Integer absoluteResolution) throws ParameterError {
    if (absoluteResolution != null && (absoluteResolution < 0 || absoluteResolution > 100)) {
      throw new ParameterError(
          format("absoluteResolution must be between %d and %d, but was %d", 0, 100, absoluteResolution));
    }
    this.absoluteResolution = absoluteResolution;
  }

  /** Do not place a buffer around quad polygons. */
  public boolean noBuffer;

  /** The counting mode. */
  public @NotNull ClusteringQuadBin.CountMode countMode = CountMode.MIXED;

  public enum CountMode {
    /** Real feature counts. Best accuracy, but slow. Not recommended for big result sets. */
    REAL("real"),
    /** Estimated feature counts. Low accuracy, but fast. Recommended for big result sets. */
    ESTIMATED("estimated"),
    /**
     * Estimated feature counts combined with real ones, if the estimation is low a real count gets
     * applied. Fits to the most use cases. This is the default.
     */
    MIXED("mixed");

    CountMode(@NotNull String text) {
      this.text = text;
    }

    /**
     * Returns the count mode for the given text.
     *
     * @param text The text.
     * @param alt The alternative to return, when no matching value found.
     * @return The count mode.
     */
    @JsonCreator
    public static ClusteringQuadBin.CountMode forText(String text, ClusteringQuadBin.CountMode alt) {
      if (text == null || text.length() < 3) {
        return alt;
      }
      for (final ClusteringQuadBin.CountMode countMode : values()) {
        if (countMode.text.equalsIgnoreCase(text)) {
          return countMode;
        }
      }
      return alt;
    }

    /** The textual representation. */
    public final @NotNull String text;

    @JsonValue
    @Override
    public @NotNull String toString() {
      return text;
    }
  }
}
