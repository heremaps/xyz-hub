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

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;

/**
 * @deprecated This format should not be used in any public API. It's rather kept for some internal purposes to keep BWC.
 */
@Deprecated
public class Csv extends FileFormat {
  private EntityPerLine entityPerLine = Feature;
  private GeoColumnEncoding encoding = null;
  private boolean addPartitionKey;
  private boolean geometryAsExtraWkbColumn;

  public EntityPerLine getEntityPerLine() {
    return entityPerLine;
  }

  public void setEntityPerLine(EntityPerLine entityPerLine) {
    this.entityPerLine = entityPerLine;
  }

  public Csv withEntityPerLine(EntityPerLine entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public GeoColumnEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(GeoColumnEncoding encoding) {
    this.encoding = encoding;
  }

  public Csv withEncoding(GeoColumnEncoding encoding) {
    setEncoding(encoding);
    return this;
  }

  public boolean isAddPartitionKey() {
    return addPartitionKey;
  }

  public void setAddPartitionKey(boolean addPartitionKey) {
    this.addPartitionKey = addPartitionKey;
  }

  public Csv withAddPartitionKey(boolean addPartitionKey) {
    setAddPartitionKey(addPartitionKey);
    return this;
  }

  public boolean isGeometryAsExtraWkbColumn() {
    return geometryAsExtraWkbColumn;
  }

  public void setGeometryAsExtraWkbColumn(boolean geometryAsExtraWkbColumn) {
    this.geometryAsExtraWkbColumn = geometryAsExtraWkbColumn;
  }

  public Csv withGeometryAsExtraWkbColumn(boolean geometryAsExtraWkbColumn) {
    setGeometryAsExtraWkbColumn(geometryAsExtraWkbColumn);
    return this;
  }

  public enum GeoColumnEncoding {
    BASE64
  }
}
