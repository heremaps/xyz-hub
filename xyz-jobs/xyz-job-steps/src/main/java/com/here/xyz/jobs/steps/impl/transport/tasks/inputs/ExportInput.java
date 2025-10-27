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
package com.here.xyz.jobs.steps.impl.transport.tasks.inputs;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ExportOutput;

@JsonTypeName("ExportInput")
public record ExportInput(Integer threadId, String tileId) implements TaskPayload {
  public ExportInput(){
    this(null, null);
  }
  public ExportInput(Integer threadId) {
    this(threadId, null);
  }
  public ExportInput(String tileId) {
    this(null, tileId);
  }

  public static void main(String[] args) throws JsonProcessingException {
    TaskedSpaceBasedStep.SpaceBasedTaskUpdate<ExportOutput> serialize = XyzSerializable.deserialize("""
            {
              "type" : "TaskedSpaceBasedStep$SpaceBasedTaskUpdate",
              "taskId" : 1,
              "taskOutput" : {
                "rows" : 11,
                "bytes" : 3968,
                "files" : 1
              }
            }
            """, TaskedSpaceBasedStep.SpaceBasedTaskUpdate.class);
    System.out.println(serialize.taskOutput);

  }
}
