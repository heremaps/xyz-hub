package com.here.xyz.util;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import org.jetbrains.annotations.NotNull;

/**
 * A totally unfair lock, implemented purely in Java. This lock is as cheap as possible in memory
 * consumption and in CPU cost, as long as only used by a small amount of threads for tiny time
 * windows. If a class extends this lock, then no allocation is done, except for 12-16 byte of
 * additional memory consumed per instance. The most simply usage is:
 *
 * <pre>{@code
 * o.lock();
 * try {
 *   ...
 * } finally {
 *   o.unlock();
 * }
 * }</pre>
 *
 * <p>This implementation is optimal when the lock only held for a short time exclusively within
 * Java threads. Do <b>NOT</b> use this lock, when an IO operation should be synchronized, for this
 * it is highly recommended using a lock provided by the operating system, so use the language
 * syntax “synchronized”. The native “synchronized” is translated into a <a
 * href="https://en.wikipedia.org/wiki/List_of_Java_bytecode_instructions">byte code instruction</a>
 * (“monitorenter” and “monitorexit”), which allows the JVM to allocate an OS mutex, that can
 * prevent all kind of issues, for example a <a
 * href="https://en.wikipedia.org/wiki/Priority_inversion">priority inversion</a>.
 *
 * <p>This implementation allows the same thread to re-enter the same lock as often as it wants.
 */
@SuppressWarnings({"DuplicatedCode", "unused"})
public class ReentrantLockable implements Lock {

  /** The thread that holds the lock. */
  Thread owner;

  /** The state of the lock. */
  int count;

  /** Acquires the lock. If the lock is not available then the current thread enters a spin-loop. */
  @Override
  public void lock() {
    final Thread current = Thread.currentThread();
    while (true) {
      final Object owner = this.owner;
      if (owner == current) {
        break;
      }
      if (owner == null && OWNER.compareAndSet(this, null, current)) {
        break;
      }
      Thread.yield();
    }
    COUNT.getAndAdd(this, 1);
  }

  /** Acquires the lock. If the lock is not available then the current thread enters a spin-loop. */
  @Override
  public void lockInterruptibly() throws InterruptedException {
    final Thread current = Thread.currentThread();
    while (true) {
      final Object owner = this.owner;
      if (owner == current) {
        break;
      }
      if (owner == null && OWNER.compareAndSet(this, null, current)) {
        break;
      }
      if (current.isInterrupted()) {
        throw new InterruptedException();
      }
      Thread.yield();
    }
    COUNT.getAndAdd(this, 1);
  }

  @Override
  public boolean tryLock() {
    final Thread current = Thread.currentThread();
    final Object owner = this.owner;
    if (owner != current) {
      if (owner != null || !OWNER.compareAndSet(this, null, current)) {
        return false;
      }
    }
    COUNT.getAndAdd(this, 1);
    return true;
  }

  @Override
  public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
    final Thread current = Thread.currentThread();
    final long END = System.nanoTime() + unit.toNanos(time);
    while (true) {
      final Object owner = this.owner;
      if (owner == current) {
        break;
      }
      if (owner == null && OWNER.compareAndSet(this, null, current)) {
        break;
      }
      if (current.isInterrupted()) {
        throw new InterruptedException();
      }
      if (System.nanoTime() > END) {
        return false;
      }
      Thread.yield();
    }
    COUNT.getAndAdd(this, 1);
    return true;
  }

  /**
   * Releases the lock.
   *
   * @throws IllegalStateException if the lock is in a broken state or not held by the current
   *     thread.
   */
  @Override
  public void unlock() {
    if (!tryUnlock()) {
      throw new IllegalStateException("The lock is not held by the current thread");
    }
  }

  /**
   * Tries to release the lock.
   *
   * @return true if the lock was released; false otherwise (the current thread does not own the
   *     lock or any other error).
   */
  public boolean tryUnlock() {
    final Thread current = Thread.currentThread();
    final Thread owner = this.owner;
    if (owner != current) {
      return false;
    }
    int count = this.count - 1;
    if (count < 0) {
      return false;
    }
    // We now know that we hold the lock.
    if (count == 0) {
      this.count = 0;
      OWNER.setVolatile(
          this, null); // This guarantees that both are visible in order count, then owner change!
    } else {
      COUNT.setRelease(this, count);
    }
    return true;
  }

  /**
   * Returns true if the current thread holds the lock.
   *
   * @return true if the current thread holds the lock; false otherwise.
   */
  public boolean isHeldByCurrentThread() {
    return Thread.currentThread() == owner;
  }

  /**
   * Returns the current owner of the lock.
   *
   * @return the current owner of the lock.
   */
  public Thread getLockOwner() {
    return owner;
  }

  /**
   * This operation is not supported.
   *
   * @throws UnsupportedOperationException When called.
   */
  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }

  static final @NotNull VarHandle OWNER;
  static final @NotNull VarHandle COUNT;

  static {
    try {
      OWNER =
          MethodHandles.lookup()
              .in(ReentrantLockable.class)
              .findVarHandle(ReentrantLockable.class, "owner", Thread.class);
      COUNT =
          MethodHandles.lookup()
              .in(ReentrantLockable.class)
              .findVarHandle(ReentrantLockable.class, "count", int.class);
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }
}
