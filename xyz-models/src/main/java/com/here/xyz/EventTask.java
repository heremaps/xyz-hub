package com.here.xyz;

import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A configurable event pipeline, that executes events in an own dedicated thread. Basically a task (and so the event pipeline) are
 * {@link #bind() bound} to a thread. All handlers added to the pipeline of the task can query the current task simply via
 * {@link #currentTask()}.
 * <p>
 * A task may send multiple events through the attached pipeline and modify the pipeline between the events.
 */
public abstract class EventTask extends EventPipelineWithLogger implements UncaughtExceptionHandler {

  /**
   * Internal default No Operation Task, just useful for testing or other edge cases. The NOP task binding is weak, that means all other
   * tasks are forcefully unbinding the NOP task. Its purpose is to ensure that {@link #currentTask()} is always able to return a task for
   * logging purpose.
   */
  static final class NopTask extends EventTask {

    NopTask() {
      // We know that NOP Tasks only created internally and only when currentTask() is invoked, therefore we set the stream-id to
      // the name of the current thread, so that the thread is not renamed.
      super(Thread.currentThread().getName());
    }

    @Override
    protected @NotNull XyzResponse execute() {
      return errorResponse(XyzError.NOT_IMPLEMENTED, "Not Implemented");
    }

    @Override
    protected void sendResponse(@NotNull XyzResponse response) {
    }
  }

  /**
   * The constructor to use, when creating new task instances on demand.
   */
  public static final AtomicReference<@Nullable Supplier<@NotNull EventTask>> FACTORY = new AtomicReference<>();

  /**
   * The soft-limit of tasks to run concurrently.
   */
  public static final AtomicLong SOFT_LIMIT = new AtomicLong(Math.max(1000, Runtime.getRuntime().availableProcessors() * 50L));

  /**
   * A thread pool to execute tasks.
   */
  private static final ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();

  /**
   * Creates a new task. If no stream-id is given, and the thread creating this task does have an own task bound, then the new task will
   * have the same stream-id. If no stream-id is given, and the thread creating this task does <b>NOT</b> have an own task bound, then a new
   * random stream-id is generated.
   *
   * @param streamId The stream-id to use; if any.
   */
  public EventTask(@Nullable String streamId) {
    if (streamId == null) {
      final EventTask currentTask = EventTask.currentTaskOrNull();
      if (currentTask != null) {
        streamId = currentTask.streamId;
      } else {
        streamId = RandomStringUtils.randomAlphanumeric(12);
      }
    }
    this.streamId = streamId;
    attachments = new ConcurrentHashMap<>();
  }

  /**
   * The uncaught exception handler for the thread that executes.
   *
   * @param thread the thread.
   * @param t      the exception.
   */
  public void uncaughtException(Thread thread, Throwable t) {
    error("Uncaught exception", t);
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
  public <T> @Nullable T get(@NotNull Class<T> valueClass) {
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
  public <T> @NotNull T getOrCreate(@NotNull Class<T> valueClass) {
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
  public <T> @NotNull Class<T> set(@NotNull T value) {
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
  protected final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments;

  /**
   * The steam-id of this context.
   */
  protected @NotNull String streamId;

  /**
   * Returns the stream-id.
   *
   * @return The stream-id.
   */
  public @NotNull String streamId() {
    return streamId;
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
   * Returns the task bound to the current thread. If no task bound, creating a new one.
   *
   * @return the task of the current thread.
   * @throws ClassCastException if the current task is not of the expected type.
   */
  @SuppressWarnings("unchecked")
  public static <T extends EventTask> @NotNull T currentTask() {
    final Thread thread = Thread.currentThread();
    final UncaughtExceptionHandler uncaughtExceptionHandler = thread.getUncaughtExceptionHandler();
    if (uncaughtExceptionHandler instanceof EventTask) {
      return (T) uncaughtExceptionHandler;
    }

    final Supplier<@NotNull EventTask> eventTaskSupplier = FACTORY.get();
    final @NotNull EventTask newTask = eventTaskSupplier != null ? eventTaskSupplier.get() : new NopTask();
    newTask.thread = thread;
    newTask.oldName = thread.getName();
    newTask.oldUncaughtExceptionHandler = uncaughtExceptionHandler;
    thread.setName(newTask.streamId);
    thread.setUncaughtExceptionHandler(newTask);
    return (T) newTask;
  }

  /**
   * Returns the task bound to the current thread; if any.
   *
   * @return The task of the current thread or {@code null}, if the current thread has no task bound.
   * @throws ClassCastException if the task is not of the expected type.
   */
  @SuppressWarnings("unchecked")
  public static <T extends EventTask> @Nullable T currentTaskOrNull() {
    final Thread thread = Thread.currentThread();
    final UncaughtExceptionHandler uncaughtExceptionHandler = thread.getUncaughtExceptionHandler();
    if (uncaughtExceptionHandler instanceof EventTask) {
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
  public void bind() {
    if (thread != null) {
      throw new IllegalStateException("Already bound to a thread");
    }
    final Thread thread = Thread.currentThread();
    final String threadName = thread.getName();
    final UncaughtExceptionHandler threadUncaughtExceptionHandler = thread.getUncaughtExceptionHandler();
    if (threadUncaughtExceptionHandler instanceof EventTask) {
      if (threadUncaughtExceptionHandler.getClass() == NopTask.class) {
        // Force unbind of NOP task.
        final NopTask nopTask = (NopTask) threadUncaughtExceptionHandler;
        nopTask.thread = null;
      } else {
        throw new IllegalStateException("The current thread is already bound to task " + threadName);
      }
    }
    this.thread = thread;
    this.oldName = threadName;
    this.oldUncaughtExceptionHandler = threadUncaughtExceptionHandler;
    thread.setName(streamId);
    thread.setUncaughtExceptionHandler(this);
  }

  /**
   * Removes this task form the current thread. The call will be ignored, if the task is already unbound.
   *
   * @throws IllegalStateException If called from a thread to which this task is not bound.
   */
  public void unbind() {
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
   * Creates a new thread and binds this task to the new thread, executing the given main method.
   *
   * @throws XyzErrorException Too many concurrent threads.
   */
  public @NotNull Future<@NotNull XyzResponse> start() throws XyzErrorException {
    final long LIMIT = EventTask.SOFT_LIMIT.get();
    do {
      final long threadCount = EventTask.threadCount.get();
      assert threadCount >= 0L;
      if (threadCount >= LIMIT) {
        throw new XyzErrorException(XyzError.TOO_MANY_REQUESTS, "Maximum number of concurrent requests (" + LIMIT + ") reached");
      }
      if (EventTask.threadCount.compareAndSet(threadCount, threadCount + 1)) {
        try {
          return threadPool.submit(this::run);
        } catch (Throwable t) {
          EventTask.threadCount.decrementAndGet();
          error("Unexpected exception while trying to fork a new thread", t);
          throw new XyzErrorException(XyzError.EXCEPTION, "Internal error while forking new worker thread");
        }
      }
      // Conflict, two threads concurrently try to fork.
    } while (true);
  }

  /**
   * Creates a new thread and binds this task to the thread, executing the given method, ignoring the thread limit. This should only be used
   * internally (we do not want a running request to be aborted just because of some arbitrary thread limit, rather reject new requests).
   */
  public @NotNull Future<@NotNull XyzResponse> startWithoutLimit() {
    try {
      EventTask.threadCount.incrementAndGet();
      return threadPool.submit(this::run);
    } catch (Throwable t) {
      EventTask.threadCount.decrementAndGet();
      error("Unexpected exception while trying to fork a new thread, ignoring the soft-limit", t);
      throw t;
    }
  }

  private static final AtomicLong threadCount = new AtomicLong();

  private @NotNull XyzResponse run() {
    bind();
    try {
      final @NotNull XyzResponse response = execute();
      if (callback != null) {
        try {
          callback.accept(response);
        } catch (Throwable t) {
          error("Uncaught exception in event pipeline callback", t);
        }
      }
      try {
        sendResponse(response);
      } catch (Throwable t) {
        error("Uncaught exception while post processing event", t);
      }
      return response;
    } catch (Throwable t) {
      error("Uncaught exception in task execute", t);
      return errorResponse(XyzError.EXCEPTION, "Uncaught exception in task " + getClass().getSimpleName());
    } finally {
      final long newValue = EventTask.threadCount.decrementAndGet();
      assert newValue >= 0L;
      unbind();
    }
  }

  // We need to capture the callback handling.

  private @Nullable Consumer<XyzResponse> callback;

  /**
   * It should not be necessary to use this.
   *
   * @param callback The callback to invoke, when the response is available.
   * @return this.
   */
  @Deprecated
  @Override
  public @NotNull EventTask setCallback(@Nullable Consumer<XyzResponse> callback) {
    this.callback = callback;
    return this;
  }

  @Override
  public @Nullable Consumer<XyzResponse> getCallback() {
    return callback;
  }

  /**
   * Execute this task.
   *
   * @return the response.
   * @throws XyzErrorException If an expected error occurred (will not be logged).
   * @throws Throwable         If any unexpected error occurred (will be logged as error).
   */
  abstract protected @NotNull XyzResponse execute() throws Throwable;

  /**
   * Called before returning the response and after calling the callback. Can be used to serialize the response and send it to a socket.
   *
   * <p><b>Note</b>: This method should not modify the response.
   *
   * @param response the response.
   */
  abstract protected void sendResponse(@NotNull XyzResponse response);

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