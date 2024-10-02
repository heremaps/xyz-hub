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

package com.here.xyz.models.hub;

import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.List;

public class FeatureModificationList extends Payload {
  private List<FeatureModification> modifications;

  public List<FeatureModification> getModifications() {
    return modifications;
  }

  public void setModifications(List<FeatureModification> modifications) {
    this.modifications = modifications;
  }

  public FeatureModificationList withModifications(List<FeatureModification> modifications) {
    setModifications(modifications);
    return this;
  }

  public static class FeatureModification implements Typed {
    private IfNotExists onFeatureNotExists;
    private IfExists onFeatureExists;
    private ConflictResolution onMergeConflict;
    private FeatureCollection featureData;

    public IfNotExists getOnFeatureNotExists() {
      return onFeatureNotExists;
    }

    public void setOnFeatureNotExists(IfNotExists onFeatureNotExists) {
      this.onFeatureNotExists = onFeatureNotExists;
    }

    public FeatureModification withOnFeatureNotExists(IfNotExists onFeatureNotExists) {
      setOnFeatureNotExists(onFeatureNotExists);
      return this;
    }

    public IfExists getOnFeatureExists() {
      return onFeatureExists;
    }

    public void setOnFeatureExists(IfExists onFeatureExists) {
      this.onFeatureExists = onFeatureExists;
    }

    public FeatureModification withOnFeatureExists(IfExists onFeatureExists) {
      setOnFeatureExists(onFeatureExists);
      return this;
    }

    public ConflictResolution getOnMergeConflict() {
      return onMergeConflict;
    }

    public void setOnMergeConflict(ConflictResolution onMergeConflict) {
      this.onMergeConflict = onMergeConflict;
    }

    public FeatureModification withOnMergeConflict(ConflictResolution onMergeConflict) {
      setOnMergeConflict(onMergeConflict);
      return this;
    }

    public FeatureCollection getFeatureData() {
      return featureData;
    }

    public void setFeatureData(FeatureCollection featureData) {
      this.featureData = featureData;
    }

    public FeatureModification withFeatureData(FeatureCollection featureData) {
      setFeatureData(featureData);
      return this;
    }
  }

  public enum IfNotExists {
    RETAIN,
    ERROR,
    CREATE;

    public static IfNotExists of(String value) {
      if (value == null)
        return null;

      try {
        return valueOf(value.toUpperCase());
      }
      catch (IllegalArgumentException e) {
        return null;
      }
    }
  }

  public enum IfExists {
    RETAIN,
    ERROR,
    DELETE,
    REPLACE,
    PATCH,
    MERGE;

    public static IfExists of(String value) {
      if (value == null)
        return null;

      try {
        return valueOf(value.toUpperCase());
      }
      catch (IllegalArgumentException e) {
        return null;
      }
    }
  }

  public enum ConflictResolution {
    ERROR,
    RETAIN,
    REPLACE;

    public static ConflictResolution of(String value) {
      if (value == null)
        return null;

      try {
        return valueOf(value.toUpperCase());
      }
      catch (IllegalArgumentException e) {
        return null;
      }
    }
  }
}
