package com.here.xyz.hub.task;

import com.here.xyz.events.Event;
import org.jetbrains.annotations.NotNull;

/**
 * A processor called by the pipeline to handle the task.
 *
 * @param <EVENT> the even type.
 * @param <TASK> the task type.
 */
@FunctionalInterface
public interface ITaskStep<EVENT extends Event, TASK extends AbstractEventTask<EVENT, TASK>> {

  /**
   * The method invoked by the pipeline to process the task.
   *
   * @param task     the task to process.
   * @param callback the callback to report the result to.
   * @throws Throwable if any error occurred.
   */
  void process(@NotNull TASK task, @NotNull ICallback callback) throws Throwable;
}