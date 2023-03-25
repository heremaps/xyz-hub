package com.here.xyz.hub.task;

import com.here.xyz.events.Event;
import org.jetbrains.annotations.NotNull;

/**
 * The handler to be invoked when the pipeline failed with an exception.
 *
 * @param <EVENT> the event type.
 * @param <TASK> the task type.
 */
@FunctionalInterface
public interface IErrorHandler<EVENT extends Event, TASK extends Task<EVENT, TASK>> {

  /**
   * The handler invoked last, when the pipeline failed with an error (exception).
   *
   * @param task      the task.
   * @param throwable the exception reported.
   */
  void onError(@NotNull TASK task, @NotNull Throwable throwable);
}
