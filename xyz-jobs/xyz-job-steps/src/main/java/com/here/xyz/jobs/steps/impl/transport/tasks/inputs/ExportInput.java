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
