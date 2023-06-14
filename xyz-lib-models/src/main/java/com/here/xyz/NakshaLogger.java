package com.here.xyz;

import com.here.xyz.lambdas.F0;
import com.here.xyz.util.NanoTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

/**
 * A class implementing the logger interface, bound to a thread.
 */
public class NakshaLogger implements Logger {

  // TODO: Add the args as supported by Wikvaya/SFW and make a new package to allow binding the Wikvaya/SFW logger.

  /**
   * A supplied that can be modified by an extending class, all instances of the logger then will be turned into the application class. This
   * can be used to redirect all logging. The default implementation will redirect all logging to SLF4j.
   */
  protected static final AtomicReference<F0<NakshaLogger>> constructor = new AtomicReference<>(NakshaLogger::new);

  /**
   * The thread local instance.
   */
  protected static final ThreadLocal<NakshaLogger> instance = ThreadLocal.withInitial(() -> constructor.get().call());

  /**
   * Returns the current thread local logger.
   *
   * @return The current thread local logger.
   */
  public static @NotNull NakshaLogger currentLogger() {
    final NakshaLogger logger = instance.get();
    final AbstractTask<?> task = AbstractTask.currentTask();
    if (task != null) {
      logger.with(task.streamId(), task.startNanos());
    } else {
      final String threadName = Thread.currentThread().getName();
      //noinspection StringEquality
      if (logger.streamId() != threadName) {
        logger.with(threadName, NanoTime.now());
      }
    }
    return logger;
  }

  /**
   * A special string that, when used as stream-id by the constructor, is replaced by the getter with a random string.
   */
  protected static final String CREATE_STREAM_ID = "";

  /**
   * Create a new thread local logger.
   */
  protected NakshaLogger() {
    streamId = CREATE_STREAM_ID;
    startNanos = NanoTime.now();
  }

  /**
   * Create a new thread local logger.
   *
   * @param streamId   The initial stream-id.
   * @param startNanos The start nanos to use.
   */
  protected NakshaLogger(@NotNull String streamId, long startNanos) {
    this.streamId = streamId;
    this.startNanos = startNanos;
  }

  /**
   * The stream-id.
   */
  private @NotNull String streamId;

  /**
   * The start nano time for time measurements.
   */
  private long startNanos;

  /**
   * Returns the stream-id.
   *
   * @return The stream-id.
   */
  public @NotNull String streamId() {
    //noinspection StringEquality
    if (streamId == CREATE_STREAM_ID) {
      streamId = RandomStringUtils.randomAlphanumeric(12);
    }
    return streamId;
  }

  /**
   * The start nanoseconds. This can be used to calculate time differences, for example via: <pre>{@code
   * final long millis = NanoTime.timeSince(XyzLogger.currentLogger().startNanos(), TimeUnit.MILLIS);
   * }</pre>
   *
   * @return The start nanoseconds.
   */
  public long startNanos() {
    return startNanos;
  }

  /**
   * Binds the thread local logger to the given stream and start time.
   *
   * @param streamId   The stream.
   * @param startNanos The start-time.
   * @return this.
   */
  public @NotNull NakshaLogger with(@NotNull String streamId, long startNanos) {
    this.streamId = streamId;
    this.startNanos = startNanos;
    return this;
  }

  /**
   * The string builder used by this thread local logger.
   */
  protected final @NotNull StringBuilder sb = new StringBuilder();

  private @NotNull String prefix(@NotNull String message) {
    final StringBuilder sb = this.sb;
    sb.setLength(0);
    sb.append(streamId());
    sb.append(':');
    sb.append(NanoTime.timeSince(startNanos(), TimeUnit.MICROSECONDS));
    sb.append("us - ");
    sb.append(message);
    return sb.toString();
  }

  protected static final Logger logger = LoggerFactory.getLogger(AbstractTask.class);

  @Override
  public String getName() {
    return streamId();
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
    return logger.isErrorEnabled();
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
    return logger.isErrorEnabled(marker);
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
