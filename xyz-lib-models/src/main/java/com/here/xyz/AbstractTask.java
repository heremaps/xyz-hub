package com.here.xyz;

import com.here.xyz.events.Event;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A configurable event task, that executes one or more events in an own dedicated thread. Basically a task is
 * {@link #attachToCurrentThread() attached} to a dedicated thread and sends events through a private pipeline with handlers added. All
 * handlers added to the pipeline of the task can query the current task simply via {@link #currentTask()}.
 * <p>
 * A task may send multiple events through the attached pipeline and modify the pipeline between these events. For example to modify
 * features at least a {@link LoadFeaturesEvent} is needed to fetch the current state of the features and then to (optionally) perform a
 * merge and execute the {@link ModifyFeaturesEvent}. Other combinations are possible.
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public abstract class AbstractTask<EVENT extends Event> implements UncaughtExceptionHandler {

  /**
   * The constructor to use, when creating new task instances on demand.
   */
  public static final AtomicReference<@Nullable Supplier<@NotNull AbstractTask<?>>> FACTORY = new AtomicReference<>();

  /**
   * The soft-limit of tasks to run concurrently.
   */
  public static final AtomicLong SOFT_LIMIT = new AtomicLong(Math.max(1000, Runtime.getRuntime().availableProcessors() * 50L));

  /**
   * A thread pool to execute tasks.
   */
  private static final ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();

  /**
   * Creates a new task. If no stream-id is given, and the thread creating this task does have a task attached, then this task will derive
   * the stream-id. If no stream-id is given, and the thread creating this task does <b>NOT</b> have an own task bound, then a new random
   * stream-id is generated.
   *
   * @param event The event.
   */
  public AbstractTask(@NotNull EVENT event) {
    this.startNanos = event.startNanos();
    this.streamId = event.getStreamId();
    this.event = event;
    attachments = new ConcurrentHashMap<>();
  }

  /**
   * Returns the thread local logger configured to this task.
   *
   * @return the thread local logger configured to this task.
   */
  public @NotNull NakshaLogger logger() {
    return NakshaLogger.currentLogger().with(streamId, startNanos);
  }

  /**
   * Returns the start time of the task in nanoseconds.
   *
   * @return The start time of the task in nanoseconds.
   */
  public long startNanos() {
    return startNanos;
  }

  /**
   * The uncaught exception handler for the thread that executes.
   *
   * @param thread the thread.
   * @param t      the exception.
   */
  public void uncaughtException(Thread thread, Throwable t) {
    logger().error("Uncaught exception", t);
  }

  /**
   * Returns all task attachments.
   *
   * @return The task attachments.
   */
  public @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments() {
    return attachments;
  }

  /**
   * Returns the value for the give type; if it exists.
   *
   * @param valueClass the class of the value type.
   * @param <T>        the value type.
   * @return the value or {@code null}.
   */
  public <T> @Nullable T getAttachment(@NotNull Class<T> valueClass) {
    final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments = attachments();
    final Object value = attachments.get(valueClass);
    return valueClass.isInstance(value) ? valueClass.cast(value) : null;
  }

  /**
   * Returns the value for the give type. This method simply uses the given class as key in the {@link #attachments()} and expects that the
   * value is of the same type. If the value is {@code null} or of a wrong type, the method will create a new instance of the given value
   * class and store it in the attachments, returning the new instance.
   *
   * @param valueClass the class of the value type.
   * @param <T>        the value type.
   * @return the value.
   * @throws NullPointerException if creating a new value instance failed.
   */
  public <T> @NotNull T getOrCreateAttachment(@NotNull Class<T> valueClass) {
    final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments = attachments();
    while (true) {
      final Object o = attachments.get(valueClass);
      if (valueClass.isInstance(o)) {
        return valueClass.cast(o);
      }
      final T newValue;
      try {
        newValue = valueClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new NullPointerException();
      }
      final Object existingValue = attachments.putIfAbsent(valueClass, newValue);
      if (existingValue == null) {
        return newValue;
      }
      if (valueClass.isInstance(existingValue)) {
        return valueClass.cast(existingValue);
      }
      // Overwrite the existing value, because it is of the wrong type.
      if (attachments.replace(valueClass, existingValue, newValue)) {
        return newValue;
      }
      // Conflict, two threads seem to want to update the same key the same time!
    }
  }

  /**
   * Sets the given value in the {@link #attachments()} using the class of the value as key.
   *
   * @param value the value to set.
   * @return the key.
   * @throws NullPointerException if the given value is null.
   */
  @SuppressWarnings("unchecked")
  public <T> @NotNull Class<T> setAttachment(@NotNull T value) {
    //noinspection ConstantConditions
    if (value == null) {
      throw new NullPointerException();
    }
    attachments().put(value.getClass(), value);
    return (Class<T>) value.getClass();
  }

  /**
   * The attachments of this context.
   */
  private final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments;

  /**
   * The steam-id of this context.
   */
  protected final @NotNull String streamId;

  /**
   * The nano-time when creating the context.
   */
  protected final long startNanos;

  /**
   * The main event to be processed by the task, created by the constructor.
   */
  private @NotNull EVENT event;

  /**
   * A flag to signal that this task is internal.
   */
  protected boolean internal;

  /**
   * Flag this task as internal, so when starting the task, the maximum amount of parallel tasks limit should be ignored.
   *
   * @param internal {@code true} if this task is internal and therefore bypassing the maximum parallel tasks limit.
   * @throws IllegalStateException If the task is not in the state {@link State#NEW}.
   */
  public void setInternal(boolean internal) throws IllegalStateException {
    lock();
    try {
      this.internal = internal;
    } finally {
      unlock();
    }
  }

  /**
   * Tests whether this task flagged as internal.
   *
   * @return {@code true} if this task flagged as internal; {@code false} otherwise.
   */
  public boolean isInternal() {
    return internal;
  }

  /**
   * Returns the stream-id.
   *
   * @return The stream-id.
   */
  public @NotNull String streamId() {
    return streamId;
  }

  /**
   * Returns the main event of this task. Internally a task may generate multiple sub-events and send them through the pipeline. For example
   * a {@link ModifyFeaturesEvent} requires a {@link LoadFeaturesEvent} pre-flight event, some other events may require other pre-flight
   * request.
   *
   * @return the main event of this task.
   */
  public final @NotNull EVENT getEvent() {
    return event;
  }

  /**
   * Set the event of this task.
   * @param event The event to set.
   * @return the previously set event.
   * @throws IllegalStateException If the task has been started already.
   */
  public final @NotNull EVENT setEvent(@NotNull EVENT event) {
    final EVENT old = this.event;
    lock();
    this.event = event;
    unlock();
    return old;
  }

  /**
   * The thread to which this context is currently bound; if any.
   */
  @Nullable Thread thread;

  /**
   * The previously set uncaught exception handler.
   */
  @Nullable Thread.UncaughtExceptionHandler oldUncaughtExceptionHandler;

  @Nullable String oldName;

  /**
   * Returns the task attached to the current thread; if any.
   *
   * @return The task attached to the current thread or {@code null}, if the current thread has no task attached.
   * @throws ClassCastException if the task is not of the expected type.
   */
  @SuppressWarnings("unchecked")
  public static <T extends AbstractTask<?>> @Nullable T currentTask() {
    final Thread thread = Thread.currentThread();
    final UncaughtExceptionHandler uncaughtExceptionHandler = thread.getUncaughtExceptionHandler();
    if (uncaughtExceptionHandler instanceof AbstractTask) {
      return (T) uncaughtExceptionHandler;
    }
    return null;
  }

  /**
   * Returns the thread to which this task is currently bound.
   *
   * @return The thread to which this task is currently bound.
   */
  public @Nullable Thread getThread() {
    return thread;
  }

  /**
   * Binds this task to the current thread.
   *
   * @throws IllegalStateException if this task bound to another thread, or the current thread bound to another task.
   */
  public void attachToCurrentThread() {
    if (thread != null) {
      throw new IllegalStateException("Already bound to a thread");
    }
    final Thread thread = Thread.currentThread();
    final String threadName = thread.getName();
    final UncaughtExceptionHandler threadUncaughtExceptionHandler = thread.getUncaughtExceptionHandler();
    if (threadUncaughtExceptionHandler instanceof AbstractTask) {
      throw new IllegalStateException("The current thread is already bound to task " + threadName);
    }
    this.thread = thread;
    this.oldName = threadName;
    this.oldUncaughtExceptionHandler = threadUncaughtExceptionHandler;
    thread.setName(streamId);
    thread.setUncaughtExceptionHandler(this);
  }

  /**
   * Removes this task form the current thread. The call will be ignored, if the task is unbound.
   *
   * @throws IllegalStateException If called from a thread to which this task is not bound.
   */
  public void detachFromCurrentThread() {
    if (this.thread == null) {
      return;
    }
    final Thread thread = Thread.currentThread();
    if (this.thread != thread) {
      throw new IllegalStateException("Can't unbind from foreign thread");
    }
    assert oldName != null;
    thread.setName(oldName);
    thread.setUncaughtExceptionHandler(oldUncaughtExceptionHandler);
    this.thread = null;
    this.oldName = null;
    this.oldUncaughtExceptionHandler = null;
  }

  /**
   * The event pipeline of this task.
   */
  protected final EventPipeline pipeline = new EventPipeline();

  /**
   * A lock to be used to modify the task thread safe.
   */
  private final ReentrantLock mutex = new ReentrantLock();

  /**
   * Acquire a lock, but only if the {@link #state()} is {@link State#NEW}.
   *
   * @throws IllegalStateException If the current state is not the one expected.
   */
  protected final void lock() {
    mutex.lock();
    final State currentState = state.get();
    if (currentState != State.NEW) {
      mutex.unlock();
      throw new IllegalStateException("Found illegal state " + currentState.name() + ", expected NEW");
    }
  }

  /**
   * Unlocks a lock acquired previously via {@link #lock()}.
   */
  protected final void unlock() {
    mutex.unlock();
  }

  /**
   * Creates a new thread, attach this task to the new thread, then call {@link #init()} followed by an invocation of {@link #execute()} to
   * generate the response.
   *
   * @return The future to the result.
   * @throws IllegalStateException If the {@link #state()} is not {@link State#NEW}.
   * @throws XyzErrorException     If any error occurred, for example too many concurrent tasks.
   */
  public @NotNull Future<@NotNull XyzResponse> start() throws XyzErrorException {
    final long LIMIT = AbstractTask.SOFT_LIMIT.get();
    lock();
    try {
      do {
        final long threadCount = AbstractTask.threadCount.get();
        assert threadCount >= 0L;
        if (!internal && threadCount >= LIMIT) {
          throw new XyzErrorException(XyzError.TOO_MANY_REQUESTS, "Maximum number of concurrent requests (" + LIMIT + ") reached");
        }
        if (AbstractTask.threadCount.compareAndSet(threadCount, threadCount + 1)) {
          try {
            final Future<XyzResponse> future = threadPool.submit(this::run);
            state.set(State.START);
            return future;
          } catch (Throwable t) {
            AbstractTask.threadCount.decrementAndGet();
            logger().error("Unexpected exception while trying to fork a new thread", t);
            throw new XyzErrorException(XyzError.EXCEPTION, "Internal error while forking new worker thread");
          }
        }
        // Conflict, two threads concurrently try to fork.
      } while (true);
    } finally {
      unlock();
    }
  }

  private static final AtomicLong threadCount = new AtomicLong();

  private @NotNull XyzResponse run() {
    assert state.get() == State.START;
    state.set(State.EXECUTE);
    attachToCurrentThread();
    try {
      @NotNull XyzResponse response;
      try {
        init();
        response = execute();
      } catch (Throwable t) {
        response = errorResponse(t);
      }
      state.set(State.CALLING_LISTENER);
      for (final @NotNull Consumer<@NotNull XyzResponse> listener : listeners) {
        try {
          listener.accept(response);
        } catch (Throwable t) {
          logger().error("Uncaught exception in listener", t);
        }
      }
      return response;
    } finally {
      state.set(State.DONE);
      final long newValue = AbstractTask.threadCount.decrementAndGet();
      assert newValue >= 0L;
      detachFromCurrentThread();
    }
  }

  /**
   * Initializes this task.
   *
   * @throws Throwable The exception to throw.
   */
  abstract protected void init() throws Throwable;

  /**
   * Execute this task.
   *
   * @return the response.
   * @throws XyzErrorException If an expected error occurred (will not be logged).
   * @throws Throwable         If any unexpected error occurred (will be logged as error).
   */
  abstract protected @NotNull XyzResponse execute() throws Throwable;

  /**
   * Try to cancel the task.
   *
   * @return {@code true} if the task cancelled successfully; {@code false} otherwise.
   */
  public boolean cancel() {
    return false;
  }

  /**
   * The state of the task.
   */
  public enum State {
    /**
     * The task is new.
     */
    NEW,

    /**
     * The task is starting.
     */
    START,

    /**
     * The task is executing.
     */
    EXECUTE,

    /**
     * Done executing and notifying listener.
     */
    CALLING_LISTENER,

    /**
     * Fully done.
     */
    DONE
  }

  private final AtomicReference<@NotNull State> state = new AtomicReference<>(State.NEW);

  /**
   * Returns the current state of the task.
   *
   * @return The current state of the task.
   */
  public final @NotNull State state() {
    return state.get();
  }

  private final @NotNull List<@NotNull Consumer<@NotNull XyzResponse>> listeners = new ArrayList<>();

  /**
   * Adds the given response listener.
   *
   * @param listener The listener to add.
   * @return {@code true} if added the listener; {@code false} if the listener already added.
   * @throws IllegalStateException If called after {@link #start()}.
   */
  public final boolean addListener(@NotNull Consumer<@NotNull XyzResponse> listener) {
    lock();
    try {
      if (!listeners.contains(listener)) {
        listeners.add(listener);
        return true;
      }
      return false;
    } finally {
      state.set(State.NEW);
    }
  }

  /**
   * Remove the given response listener.
   *
   * @param listener The listener to remove.
   * @return {@code true} if removed the listener; {@code false} otherwise.
   * @throws IllegalStateException After {@link #start()} called.
   */
  public final boolean removeListener(@NotNull Consumer<@NotNull XyzResponse> listener) {
    lock();
    try {
      if (!listeners.contains(listener)) {
        listeners.remove(listener);
        return true;
      }
      return false;
    } finally {
      state.set(State.NEW);
    }
  }

  /**
   * Helper method to return a valid error response for the given exception.
   *
   * @param t The exception for which to return a valid error response.
   * @return The error response.
   */
  protected @NotNull XyzResponse errorResponse(@NotNull Throwable t) {
    final XyzResponse response;
    if (t instanceof XyzErrorException e) {
      response = e.toErrorResponse(streamId);
    } else if (t instanceof ParameterError e) {
      response = errorResponse(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
    } else {
      logger().error("Unexpected exception (not XyzErrorException or ParameterError): {}", t.getClass().getName(), t);
      response = errorResponse(XyzError.EXCEPTION, "Uncaught exception in task " + getClass().getSimpleName());
    }
    return response;
  }

  /**
   * Helper method to create a new error response.
   *
   * @param error   the XYZ error to return.
   * @param message the message to return.
   * @return the error response.
   */
  protected final @NotNull XyzResponse errorResponse(@NotNull XyzError error, @NotNull String message) {
    final ErrorResponse r = new ErrorResponse();
    r.setError(error);
    r.setStreamId(streamId);
    r.setErrorMessage(message);
    return r;
  }
}