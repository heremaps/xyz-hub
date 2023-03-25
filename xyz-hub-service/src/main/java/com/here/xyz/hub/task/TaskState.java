package com.here.xyz.hub.task;

/**
 * The state can be read to know whether an action should still be performed or may be cancelled. E.g. when the task is in a final state
 * already, it doesn't make sense to send a(nother) response or fail with another exception.
 * <p>
 * Final states are: {@link TaskState#RESPONSE_SENT} The following is true for final states: No further action should be started and pending
 * actions should be cancelled / killed. If cancelling of some asynchronous action is not possible the action's handler should do nothing in
 * case it still gets called.
 */
public enum TaskState {

  /**
   * Init is the first state right after the task has been created. The execution of the task has not started yet.
   */
  INIT,

  /**
   * The execution of the task has been started by the {@link TaskPipeline}.
   */
  STARTED,

  /**
   * The main action of this task is in progress. This could be a running request the system is waiting for to succeed or fail.
   */
  IN_PROGRESS,

  /**
   * The task's actions have been performed and the response has been sent to the client. This is a final state.
   */
  RESPONSE_SENT,

  /**
   * The task's execution has been cancelled. E.g. due to the request has been cancelled by the client. This is a final state.
   */
  CANCELLED,

  /**
   * An error happened during the execution of one of this task's actions. This could happen either during the request- or response-phase.
   * This is a final state.
   */
  ERROR;

  public boolean isFinal() {
    return this == RESPONSE_SENT || this == CANCELLED || this == ERROR;
  }
}
