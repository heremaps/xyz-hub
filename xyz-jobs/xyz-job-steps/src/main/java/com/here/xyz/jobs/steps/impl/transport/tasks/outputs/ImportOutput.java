package com.here.xyz.jobs.steps.impl.transport.tasks.outputs;

import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;

public record ImportOutput(String rows) implements TaskPayload { }
