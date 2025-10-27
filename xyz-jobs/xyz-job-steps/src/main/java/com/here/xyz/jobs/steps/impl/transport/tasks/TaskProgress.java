package com.here.xyz.jobs.steps.impl.transport.tasks;

public class TaskProgress<I> {
  private int totalTasks;
  private int startedTasks;
  private int finalizedTasks;
  private Integer taskId;
  private I taskInput;

  public TaskProgress() {}

  public TaskProgress(int totalTasks, int startedTasks, int finalizedTasks, Integer taskId, I taskInput) {
    this.totalTasks = totalTasks;
    this.startedTasks = startedTasks;
    this.finalizedTasks = finalizedTasks;
    this.taskId = taskId;
    this.taskInput = taskInput;
  }

  public int getTotalTasks() {
    return totalTasks;
  }

  public void setTotalTasks(int totalTasks) {
    this.totalTasks = totalTasks;
  }

  public int getStartedTasks() {
    return startedTasks;
  }

  public void setStartedTasks(int startedTasks) {
    this.startedTasks = startedTasks;
  }

  public int getFinalizedTasks() {
    return finalizedTasks;
  }

  public void setFinalizedTasks(int finalizedTasks) {
    this.finalizedTasks = finalizedTasks;
  }

  public Integer getTaskId() {
    return taskId;
  }

  public void setTaskId(Integer taskId) {
    this.taskId = taskId;
  }

  public I getTaskInput() {
    return taskInput;
  }

  public void setTaskInput(I taskInput) {
    this.taskInput = taskInput;
  }

  public boolean isComplete() {
    return totalTasks == finalizedTasks;
  }

  @Override
  public String toString() {
    return "TaskProgress{" +
            "totalTasks=" + totalTasks +
            ", startedTasks=" + startedTasks +
            ", finalizedTasks=" + finalizedTasks +
            ", taskId=" + taskId +
            ", taskInput=" + taskInput +
            '}';
  }
}
