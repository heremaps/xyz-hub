package com.here.xyz;

import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Transaction;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The Naksha host.
 */
public interface INaksha {

  /**
   * The reference to the Naksha implementation provided by the host.
   */
  AtomicReference<@NotNull INaksha> instance = new AtomicReference<>();

  /**
   * If an exception happens, all functions of this class will set a thread local error.
   *
   * @return the last error.
   */
  @Nullable Throwable getLastError();

  /**
   * Sets the thread local error and returns null.
   *
   * @param error the error or {@code null}, if no error.
   * @return {@code null}.
   */
  <V> @Nullable V Return(@Nullable Throwable error);

  /**
   * Returns the given value and clears thread local error.
   *
   * @param value the value to return.
   * @return the given value.
   */
  <V> @Nullable V Return(@Nullable V value);

  /**
   * Tests if the given value is {@code null} and if so, throws either the {@link #getLastError() last error} or an
   * {@link NullPointerException}, if no error is available.
   *
   * @param value The value to test.
   * @param <V>   The value-type.
   * @return the value.
   * @throws Throwable If the value is null.
   */
  default <V> @NotNull V notNull(@Nullable V value) throws Throwable {
    if (value != null) {
      return value;
    }
    final Throwable lastError = getLastError();
    throw lastError != null ? lastError : new NullPointerException();
  }

  /**
   * Tests if the given value is {@code null} and if so, queries {@link #getLastError() last error} and throws it. If the value is correctly
   * {@code null}, then returns null.
   *
   * @param value The value to test.
   * @param <V>   The value-type.
   * @return the value.
   * @throws Throwable If an error occurred.
   */
  default <V> @Nullable V notError(@Nullable V value) throws Throwable {
    if (value != null) {
      return value;
    }
    final Throwable lastError = getLastError();
    if (lastError != null) {
      throw lastError;
    }
    return null;
  }

  /**
   * Tests if the given value is {@code null} and if so, queries {@link #getLastError() last error} and throws it. If the value is correctly
   * {@code null}, then returns the given alternative, that is not {@code null}.
   *
   * @param value       The value to test.
   * @param alternative The value to return, when the value is {@code null}.
   * @param <V>         The value-type.
   * @return the value.
   * @throws Throwable If an error occurred.
   */
  default <V> @NotNull V notError(@Nullable V value, @NotNull V alternative) throws Throwable {
    if (value != null) {
      return value;
    }
    final Throwable lastError = getLastError();
    if (lastError != null) {
      throw lastError;
    }
    return alternative;
  }

  /**
   * Returns the space with the given identifier or {@code null}, if no such space exists.
   *
   * @param id The space identifier.
   * @return The space or {@code null}, if no such space exists.
   */
  @Nullable Space getSpaceById(@NotNull String id);

  /**
   * Returns the space with the given identifier or {@code null}, if no such space exists.
   *
   * @param id The space identifier.
   * @return The space or {@code null}, if no such space exists.
   */
  @Nullable Connector getConnectorById(@NotNull String id);
}