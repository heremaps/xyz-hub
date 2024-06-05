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

package com.here.xyz.jobs.datasets;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.datasets.files.Csv;
import com.here.xyz.jobs.datasets.files.FileChunking;
import com.here.xyz.jobs.datasets.files.FileFormat;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.datasets.files.Partitioning;
import com.here.xyz.jobs.datasets.files.Partitioning.FeatureKey;
import java.util.Map;

@JsonInclude(Include.NON_DEFAULT)
public class FileOutputSettings {
  private FileFormat format = new GeoJson();
  private Partitioning partitioning = new FeatureKey();
  private FileChunking chunking = new FileChunking();

  //Legacy fields:
  private String partitionKey = "tileid";
  private int tileLevel = 12;
  private boolean clipped = false;
  private int maxTilesPerFile = 512;

  public FileFormat getFormat() {
    return format;
  }

  public void setFormat(Object format) {
    //TODO: Remove BWC hack after refactoring
    if(format instanceof  FileFormat fileFormat)
      this.format = fileFormat;
    else if(format instanceof Map map && map.containsKey("type"))
      this.format = XyzSerializable.fromMap(map, FileFormat.class);
    else if (format instanceof String formatString) {
      if ("GEOJSON".equals(formatString))
        this.format = new GeoJson();
      else if ("JSON_WKB".equals(formatString))
        this.format = new Csv();
      else
        this.format = new Csv().withAddPartitionKey(true);
    }
    else
      this.format = new Csv();
  }

  public FileOutputSettings withFormat(FileFormat format) {
    setFormat(format);
    return this;
  }

  public Partitioning getPartitioning() {
    return partitioning;
  }

  public void setPartitioning(Partitioning partitioning) {
    this.partitioning = partitioning;
  }

  public FileOutputSettings withPartitioning(Partitioning partitioning) {
    setPartitioning(partitioning);
    return this;
  }

  public FileChunking getChunking() {
    return chunking;
  }

  public void setChunking(FileChunking chunking) {
    this.chunking = chunking;
  }

  public FileOutputSettings withChunking(FileChunking chunking) {
    setChunking(chunking);
    return this;
  }

  //Legacy getters & setters:
  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public FileOutputSettings withPartitionKey(String partitionKey) {
    setPartitionKey(partitionKey);
    return this;
  }

  public int getTileLevel() {
    return tileLevel;
  }

  public void setTileLevel(int tileLevel) {
    this.tileLevel = tileLevel;
  }

  public FileOutputSettings withTileLevel(int tileLevel) {
    setTileLevel(tileLevel);
    return this;
  }

  public boolean isClipped() {
    return clipped;
  }

  public void setClipped(boolean clipped) {
    this.clipped = clipped;
  }

  public FileOutputSettings withClipped(boolean clipped) {
    setClipped(clipped);
    return this;
  }

  public int getMaxTilesPerFile() {
    return maxTilesPerFile;
  }

  public void setMaxTilesPerFile(int maxTilesPerFile) {
    this.maxTilesPerFile = maxTilesPerFile;
  }

  public FileOutputSettings withMaxTilesPerFile(int maxTilesPerFile) {
    setMaxTilesPerFile(maxTilesPerFile);
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return XyzSerializable.equals(this, obj);
  }
}
