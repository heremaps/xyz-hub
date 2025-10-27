package com.here.xyz.jobs.steps.impl.transport.tasks.inputs;

import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;

public record ImportInput(String s3Bucket, String s3Key, String s3Region, long fileByteSize)
        implements TaskPayload {
}
