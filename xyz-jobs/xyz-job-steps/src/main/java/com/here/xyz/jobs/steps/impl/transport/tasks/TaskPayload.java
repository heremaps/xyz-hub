/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
package com.here.xyz.jobs.steps.impl.transport.tasks;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
import com.here.xyz.jobs.steps.impl.transport.ExtractJsonPathValues;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.CountInput;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.Empty;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ExportInput;
import com.here.xyz.jobs.steps.impl.transport.tasks.inputs.ImportInput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ExportOutput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput;

@JsonSubTypes({
        @JsonSubTypes.Type(value = CountInput.class, name = "CountInput"),
        @JsonSubTypes.Type(value = Empty.class, name = "Empty"),
        @JsonSubTypes.Type(value = ExportInput.class, name = "ExportInput"),
        @JsonSubTypes.Type(value = ImportInput.class, name = "ImportInput"),

        @JsonSubTypes.Type(value = ExportOutput.class, name = "ExportOutput"),
        @JsonSubTypes.Type(value = ImportOutput.class, name = "ImportOutput"),
        @JsonSubTypes.Type(value = ExtractJsonPathValues.ExtractTaskInput.class, name = "ExtractJsonPathValues$ExtractTaskInput"),
        @JsonSubTypes.Type(value = ExtractJsonPathValues.ExtractTaskOutput.class, name= "ExtractJsonPathValues$ExtractTaskOutput")
})
public interface TaskPayload extends Typed {
}
