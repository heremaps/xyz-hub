package com.here.xyz.hub.task;

import com.here.xyz.events.Event;
import org.jetbrains.annotations.NotNull;

/**
 * The handler to be invoked when the pipeline finished without any error.
 *
 * @param <EVENT> the event type.
 * @param <TASK> the task type.
 */
@FunctionalInterface
public interface ISuccessHandler<EVENT extends Event, TASK extends AbstractEventTask<EVENT, TASK>> {

  /**
   * The handler invoked last, when the pipeline finished without any error.
   *
   * @param task the task.
   * @throws Throwable if any error occurred.
   */
  void onSuccess(@NotNull TASK task) throws Throwable;
}
