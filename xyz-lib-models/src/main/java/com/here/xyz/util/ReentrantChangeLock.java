package com.here.xyz.util;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import org.jetbrains.annotations.NotNull;

/**
 * A change lock used to synchronize access and to make consistent concurrent reads and writes. The usage is rather simple, for mutators
 * do:
 * <pre>{@code
 * try (final var op = change.write()) {
 *   // No other write can be done concurrently, not even using the same thread!
 *   // Beware that no method is called, that tries to acquire write() too, it will deadlock!
 * }
 * }</pre>
 * <p>
 * For readers to perform consistent concurrent reads: <pre>{@code
 * long id;
 * do {
 *   id = change.read();
 *   // Read what you need.
 * } while (!change.isConsistent(id));
 * // Process the consistently read result.
 * }</pre>
 *
 * <p><b>Note:</b> Writers should keep the write-phase as short as possible, they may acquire the lock before they actually start writing, so
 * this is feasible: <pre>{@code
 * change.lock();
 * try {
 *   // prepare the write
 *   try (final var op = change.write()) {
 *     // No other write can be done concurrently, not even using the same thread!
 *     // Beware that no method is called, that tries to acquire write() too, it will deadlock!
 *   }
 * } finally {
 *   change.unlock;
 * }
 * }</pre>
 */
public class ReentrantChangeLock extends ReentrantLockable implements Closeable {

  /**
   * The change count.
   */
  long changeCount;

  /**
   * Starts a new read.
   *
   * @return the read identifier.
   */
  public long read() {
    long changeCount = this.changeCount;
    while ((changeCount & 1) == 1) {
      Thread.yield();
      changeCount = (long) CHANGE_COUNT.getVolatile(this);
    }
    return changeCount;
  }

  /**
   * Tests if the read was consistent.
   *
   * @param readId the read identifier.
   * @return {@code true} if the read was consistent; {@code false} otherwise.
   */
  public boolean isConsistent(long readId) {
    return (long) CHANGE_COUNT.getVolatile(this) == readId;
  }

  /**
   * Lock this instance and then return it for modification. The method guarantees to start a new inconsistent state. If the change lock is
   * currently in an inconsistent state, the method blocks until a consistent state reached.
   *
   * @return this.
   */
  public @NotNull ReentrantChangeLock write() {
    lock();
    while (true) {
      long changeCount = this.changeCount;
      while ((changeCount & 1) == 1) {
        Thread.yield();
        changeCount = (long) CHANGE_COUNT.getVolatile(this);
      }
      if (CHANGE_COUNT.compareAndSet(this, changeCount, changeCount + 1)) {
        return this;
      }
    }
  }

  static final @NotNull VarHandle CHANGE_COUNT;

  static {
    try {
      CHANGE_COUNT = MethodHandles.lookup().in(ReentrantChangeLock.class)
          .findVarHandle(ReentrantChangeLock.class, "changeCount", long.class);
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }

  @Override
  public void close() {
    CHANGE_COUNT.getAndAdd(this, 1);
    unlock();
  }
}
