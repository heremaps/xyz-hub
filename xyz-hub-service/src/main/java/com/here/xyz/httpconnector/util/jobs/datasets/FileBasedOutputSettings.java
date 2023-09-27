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

package com.here.xyz.httpconnector.util.jobs.datasets;

import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;

import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;

public class FileBasedOutputSettings {
  private CSVFormat format = JSON_WKB;
  private String partitionKey = "tileid";
  private int tileLevel = 12;
  private boolean clipped = false;
  private int maxTilesPerFile = 512;

  public CSVFormat getFormat() {
    return format;
  }

  public void setFormat(CSVFormat format) {
    this.format = format;
  }

  public FileBasedOutputSettings withFormat(CSVFormat format) {
    setFormat(format);
    return this;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public FileBasedOutputSettings withPartitionKey(String partitionKey) {
    setPartitionKey(partitionKey);
    return this;
  }

  public int getTileLevel() {
    return tileLevel;
  }

  public void setTileLevel(int tileLevel) {
    this.tileLevel = tileLevel;
  }

  public FileBasedOutputSettings withTileLevel(int tileLevel) {
    setTileLevel(tileLevel);
    return this;
  }

  public boolean isClipped() {
    return clipped;
  }

  public void setClipped(boolean clipped) {
    this.clipped = clipped;
  }

  public FileBasedOutputSettings withClipped(boolean clipped) {
    setClipped(clipped);
    return this;
  }

  public int getMaxTilesPerFile() {
    return maxTilesPerFile;
  }

  public void setMaxTilesPerFile(int maxTilesPerFile) {
    this.maxTilesPerFile = maxTilesPerFile;
  }

  public FileBasedOutputSettings withMaxTilesPerFile(int maxTilesPerFile) {
    setMaxTilesPerFile(maxTilesPerFile);
    return this;
  }
}
