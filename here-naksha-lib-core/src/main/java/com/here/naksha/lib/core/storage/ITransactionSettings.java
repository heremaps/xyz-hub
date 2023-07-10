package com.here.naksha.lib.core.storage;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * The transaction settings.
 */
public interface ITransactionSettings {

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  long getStatementTimeout(@NotNull TimeUnit timeUnit);

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   * @return this.
   * @throws Exception If any error occurred.
   */
  @NotNull ITransactionSettings withStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) throws Exception;

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  long getLockTimeout(@NotNull TimeUnit timeUnit);

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   * @return this.
   * @throws Exception If any error occurred.
   */
  @NotNull ITransactionSettings withLockTimeout(long timeout, @NotNull TimeUnit timeUnit) throws Exception;

}
