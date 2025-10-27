package com.here.xyz.jobs.steps.impl.transport.tasks.outputs;

import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;

public record ExportOutput(long rows, long bytes, int files) implements TaskPayload { }
