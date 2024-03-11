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

package com.here.xyz.jobs.datasets.files;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import com.here.xyz.jobs.datasets.files.Partitioning.FeatureKey;
import com.here.xyz.jobs.datasets.files.Partitioning.Tiles;

@JsonSubTypes({
    @JsonSubTypes.Type(value = Tiles.class, name = "Tiles"),
    @JsonSubTypes.Type(value = FeatureKey.class, name = "FeatureKey")
})
public abstract class Partitioning implements Typed {

  public abstract String toBWCPartitionKey();

  public static class Tiles extends Partitioning {
    private int level = 12;
    private boolean clip;

    public int getLevel() {
      return level;
    }

    public void setLevel(int level) {
      this.level = level;
    }

    public Tiles withLevel(int level) {
      setLevel(level);
      return this;
    }

    public boolean isClip() {
      return clip;
    }

    public void setClip(boolean clip) {
      this.clip = clip;
    }

    public Tiles withClip(boolean clip) {
      setClip(clip);
      return this;
    }

    @Override
    public String toBWCPartitionKey() {
      return "tileid";
    }
  }

  public static class FeatureKey extends Partitioning {
    private String key = "id";

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public FeatureKey withKey(String key) {
      setKey(key);
      return this;
    }

    @Override
    public String toBWCPartitionKey() {
      return getKey();
    }
  }
}
