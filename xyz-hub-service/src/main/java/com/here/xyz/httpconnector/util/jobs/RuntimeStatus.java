package com.here.xyz.httpconnector.util.jobs;

import com.here.xyz.hub.Core;

public class RuntimeStatus {

  //TODO: Move statistics in here
  //TODO: Move job state in here
  private Action desiredAction;
  private long lastUpdatedAt;

  private long startedAt;

  public float getEstimatedProgress() {
    return 0.5f; //TODO: Deduct from statistics, start time & e.g. estimated row count
  }

  public long getEstimatedEndTime() {
    long executionTime = Core.currentTimeMillis() - getStartedAt();
    float estimatedProgress = getEstimatedProgress();
    if (estimatedProgress == 0)
      return -1;
    long estimatedDuration = (long) (executionTime / estimatedProgress);
    return getStartedAt() + estimatedDuration;
  }

  /**
   * The desired action can be set by the user to define the intent of executing some action on the status of
   * the job. If the action could not be executed, the value stays as it was defined by the user.
   * Once the execution has been successfully performed, that value will be unset.
   * @return
   */
  public Action getDesiredAction() {
    return desiredAction;
  }

  public void setDesiredAction(Action desiredAction) {
    this.desiredAction = desiredAction;
  }

  /**
   * Returns the time of the last status update of the job in milliseconds.
   * @return
   */
  public long getLastUpdatedAt() {
    return lastUpdatedAt;
  }

  public void setLastUpdatedAt(long lastUpdatedAt) {
    this.lastUpdatedAt = lastUpdatedAt;
  }

  public long getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(long startedAt) {
    this.startedAt = startedAt;
  }

  public enum Action {
    START,
    ABORT,
    RESUME
  }
}
