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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

/**
 * A helper class manage a task in an own thread.
 */
public abstract class EventTask extends EventPipeline implements UncaughtExceptionHandler, Logger {

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
   * Creates a new task.
   */
  public EventTask() {
    this(null);
  }

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
    startNanos = NanoTime.now();
    attachments = new ConcurrentHashMap<>();
    sb = new StringBuilder();
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
   * Returns the value for the give type. This method simply uses the given class as key in the {@link #attachments()} and expects that the
   * value is of the same type. If the value is {@code null} or of a wrong type, the method will create a new instance of the given value
   * class and store it in the attachments, returning the new instance.
   *
   * @param valueClass the class of the value type.
   * @param <T>        the value type.
   * @return the value.
   * @throws NullPointerException if creating a new value instance failed.
   */
  @SuppressWarnings("unchecked")
  public <T> @NotNull T get(@NotNull Class<T> valueClass) {
    final @NotNull ConcurrentHashMap<@NotNull Object, Object> attachments = attachments();
    while (true) {
      final Object o = attachments.get(valueClass);
      if (valueClass.isInstance(o)) {
        return (T) o;
      }
      final T value;
      try {
        value = valueClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new NullPointerException();
      }
      final Object oldValue = attachments.putIfAbsent(valueClass, value);
      if (oldValue == null) {
        return value;
      }
      if (valueClass.isInstance(oldValue)) {
        return (T) oldValue;
      }
      // Overwrite the existing value, because it is of the wrong type.
      if (attachments.replace(valueClass, oldValue, value)) {
        return value;
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
   * The nano-time when creating the context.
   */
  public final long startNanos;
  private final @NotNull StringBuilder sb;
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
      return execute();
    } finally {
      final long newValue = EventTask.threadCount.decrementAndGet();
      assert newValue >= 0L;
      unbind();
    }
  }

  /**
   * The method invoked, when executing the task.
   *
   * @return The generated response.
   */
  abstract protected @NotNull XyzResponse execute();

  /**
   * Helper method to create a new error response from an exception.
   *
   * @param t the exception for which to return an error message.
   * @return the error response.
   */
  protected @NotNull XyzResponse errorResponse(@NotNull Throwable t) {
    // TODO: Improve this!
    return errorResponse(XyzError.EXCEPTION, t.getMessage());
  }

  /**
   * Helper method to create a new error response.
   *
   * @param error   the XYZ error to return.
   * @param message the message to return.
   * @return the error response.
   */
  protected @NotNull XyzResponse errorResponse(@NotNull XyzError error, @NotNull String message) {
    final ErrorResponse r = new ErrorResponse();
    r.setError(error);
    r.setStreamId(streamId);
    r.setErrorMessage(message);
    return r;
  }

  private static final Logger logger = LoggerFactory.getLogger(EventTask.class);

  private @NotNull String prefix(@NotNull String message) {
    sb.append(streamId);
    sb.append(':');
    sb.append(NanoTime.timeSince(startNanos, TimeUnit.MICROSECONDS));
    sb.append("us - ");
    sb.append(message);
    return sb.toString();
  }

  // ------------------------------------------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------------------------

  @Override
  public String getName() {
    return streamId;
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  @Override
  public void trace(String msg) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(msg));
    }
  }

  @Override
  public void trace(String format, Object arg) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(format), arg);
    }
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(format), arg1, arg2);
    }
  }

  @Override
  public void trace(String format, Object... arguments) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(format), arguments);
    }
  }

  @Override
  public void trace(String msg, Throwable t) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(msg), t);
    }
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return logger.isTraceEnabled();
  }

  @Override
  public void trace(Marker marker, String msg) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(msg));
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(format), arg);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(format), arg1, arg2);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(format), argArray);
    }
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(msg), t);
    }
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  public void debug(@NotNull String message) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(message));
    }
  }

  @Override
  public void debug(String format, Object arg) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(format), arg);
    }
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(format), arg1, arg2);
    }
  }

  @Override
  public void debug(String format, Object... arguments) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(format), arguments);
    }
  }

  public void debug(@NotNull String message, @NotNull Throwable t) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(message), t);
    }
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return logger.isDebugEnabled(marker);
  }

  @Override
  public void debug(Marker marker, String msg) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(msg));
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(format), arg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(format), arg1, arg2);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(format), arguments);
    }
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(msg), t);
    }
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  public void info(@NotNull String message) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(message));
    }
  }

  @Override
  public void info(String format, Object arg) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(format), arg);
    }
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(format), arg1, arg2);
    }
  }

  @Override
  public void info(String format, Object... arguments) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(format), arguments);
    }
  }

  @Override
  public void info(String msg, Throwable t) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(msg), t);
    }
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return logger.isInfoEnabled(marker);
  }

  @Override
  public void info(Marker marker, String msg) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(msg));
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(format), arg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(format), arg1, arg2);
    }
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(format), arguments);
    }
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(msg));
    }
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  @Override
  public void warn(String msg) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(msg));
    }
  }

  @Override
  public void warn(String format, Object arg) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(format), arg);
    }
  }

  @Override
  public void warn(String format, Object... arguments) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(format), arguments);
    }
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return logger.isWarnEnabled(marker);
  }

  @Override
  public void warn(Marker marker, String msg) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(msg));
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(format), arg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(format), arg1, arg2);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(format), arguments);
    }
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(msg), t);
    }
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(format), arg1, arg2);
    }
  }

  @Override
  public void warn(String msg, Throwable t) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(msg), t);
    }
  }

  @Override
  public boolean isErrorEnabled() {
    return isErrorEnabled();
  }

  @Override
  public void error(String msg) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(msg));
    }
  }

  @Override
  public void error(String format, Object arg) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(format), arg);
    }
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(format), arg1, arg2);
    }
  }

  @Override
  public void error(String format, Object... arguments) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(format), arguments);
    }
  }

  @Override
  public void error(String msg, Throwable t) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(msg), t);
    }
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return isErrorEnabled(marker);
  }

  @Override
  public void error(Marker marker, String msg) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(msg));
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(format), arg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(format), arg1, arg2);
    }
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(format), arguments);
    }
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(msg), t);
    }
  }

}