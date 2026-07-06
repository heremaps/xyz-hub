/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
package com.here.xyz.jobs.steps.impl.transport.tasks.outputs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.here.xyz.Typed;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;

@JsonInclude(NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public record ImportOutput(String importStatistics, long fileBytes,
                           ImportProgress progress) implements TaskPayload {

  public ImportOutput(String importStatistics, long fileBytes) {
    this(importStatistics, fileBytes, null);
  }

  public ImportOutput(ImportProgress progress) {
    this(null, -1, progress);
  }

  //@TODO: shift this extraction to SQL function "perform_import_from_s3_task()"
  public long extractRowCount() {
    if (importStatistics == null) return 0;
    var matcher = java.util.regex.Pattern.compile("\\d+").matcher(importStatistics);
    return matcher.find() ? Long.parseLong(matcher.group()) : 0;
  }

  @JsonInclude(NON_DEFAULT)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record ImportProgress(boolean tmpTableLoaded, long startI, long endI)
          implements Typed {
    public ImportProgress(boolean tmpTableLoaded){
      this(tmpTableLoaded, -1,-1);
    }
  }
}

