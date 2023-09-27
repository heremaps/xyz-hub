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

package com.here.xyz.httpconnector.util.jobs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.here.xyz.Typed;
import com.here.xyz.httpconnector.util.jobs.DatasetDescription.Files;
import com.here.xyz.httpconnector.util.jobs.DatasetDescription.Map;
import com.here.xyz.httpconnector.util.jobs.DatasetDescription.Space;
import com.here.xyz.httpconnector.util.jobs.DatasetDescription.Spaces;
import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Map.class, name = "Map"),
    @JsonSubTypes.Type(value = Space.class, name = "Space"),
    @JsonSubTypes.Type(value = Spaces.class, name = "Spaces"),
    @JsonSubTypes.Type(value = Files.class, name = "Files")
})
public abstract class DatasetDescription implements Typed {

  /**
   * @return the (primary) key of this DatasetDescription in order to search for it in the persistence layer.
   *  Returning null means, that any other key is matched.
   */
  @JsonIgnore
  public abstract String getKey();

  public abstract static class Identifiable extends DatasetDescription {

    private String id;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public <T extends Identifiable> T withId(String id) {
      setId(id);
      return (T) this;
    }

    public String getKey() {
      return getId();
    }
  }

  public static class Files extends DatasetDescription {
    CSVFormat format;
    int tileLevel = 12;
    boolean clipped = false;

    public CSVFormat getFormat() {
      return format;
    }

    public void setFormat(CSVFormat format) {
      this.format = format;
    }

    public Files withFormat(CSVFormat format) {
      setFormat(format);
      return this;
    }

    public int getTileLevel() {
      return tileLevel;
    }

    public void setTileLevel(int tileLevel) {
      this.tileLevel = tileLevel;
    }

    public Files withTileLevel(int tileLevel) {
      setTileLevel(tileLevel);
      return this;
    }

    public boolean isClipped() {
      return clipped;
    }

    public void setClipped(boolean clipped) {
      this.clipped = clipped;
    }

    public Files withClipped(boolean clipped) {
      setClipped(clipped);
      return this;
    }

    @Override
    public String getKey() {
      //No specific key to search for.
      return null;
    }
  }

  public static class Map extends Identifiable {

  }

  public static class Space extends Identifiable {

  }

  public static class Spaces extends DatasetDescription {

    private List<String> spaceIds;

    public List<String> getSpaceIds() {
      return spaceIds;
    }

    public void setSpaceIds(List<String> spaceIds) {
      this.spaceIds = spaceIds;
    }

    public Spaces withSpaceIds(List<String> spaceIds) {
      setSpaceIds(spaceIds);
      return this;
    }

    public String getKey() {
      return String.join(",", spaceIds);
    }
  }
}
