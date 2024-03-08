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

import static com.here.xyz.jobs.datasets.files.GeoJson.JsonMultiLineStandard.NEW_LINE;

public class GeoJson extends FileFormat {
  private EntityPerLine entityPerLine = EntityPerLine.Feature;
  private JsonMultiLineStandard multiLineStandard = NEW_LINE;

  public EntityPerLine getEntityPerLine() {
    return entityPerLine;
  }

  public void setEntityPerLine(EntityPerLine entityPerLine) {
    this.entityPerLine = entityPerLine;
  }

  public GeoJson withEntityPerLine(EntityPerLine entityPerLine) {
    setEntityPerLine(entityPerLine);
    return this;
  }

  public JsonMultiLineStandard getMultiLineStandard() {
    return multiLineStandard;
  }

  public void setMultiLineStandard(JsonMultiLineStandard multiLineStandard) {
    this.multiLineStandard = multiLineStandard;
  }

  public GeoJson withMultiLineStandard(JsonMultiLineStandard multiLineStandard) {
    setMultiLineStandard(multiLineStandard);
    return this;
  }

  public enum JsonMultiLineStandard {
    RFC7464,
    NEW_LINE
  }
}
