package com.here.xyz.hub.task;

import org.jetbrains.annotations.NotNull;

/**
 * The callback handler.
 */
public interface ICallback {

  /**
   * Report an exception, the current chain value stays what it is and the finishing handler is invoked.
   *
   * @param e the exception to report.
   */
  void throwException(@NotNull Throwable e);

  /**
   * Report a success, continue the pipeline with the next step using the given task.
   */
  void success();
}
