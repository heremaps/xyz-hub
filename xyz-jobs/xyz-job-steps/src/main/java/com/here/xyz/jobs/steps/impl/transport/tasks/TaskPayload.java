package com.here.xyz.jobs.steps.impl.transport.tasks;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Typed;
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
        @JsonSubTypes.Type(value = ImportOutput.class, name = "ImportOutput")
})
public interface TaskPayload extends Typed {
}
