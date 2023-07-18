/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.core;

import static com.here.naksha.lib.core.NakshaContext.currentContext;

import com.here.naksha.lib.core.util.NanoTime;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.helpers.CheckReturnValue;

/**
 * Improved logger interface for use within Naksha.
 */
@SuppressWarnings("unused")
public final class NakshaLogger implements Logger {

  NakshaLogger(@NotNull NakshaContext context) {
    this.context = context;
  }

  /**
   * Returns the current thread local logger.
   *
   * @return The current thread local logger.
   */
  public static @NotNull NakshaLogger currentLogger() {
    return currentContext().logger;
  }

  /**
   * The Naksha context.
   */
  public final @NotNull NakshaContext context;

  private final @NotNull NakshaLoggingEventBuilder eventBuilder = new NakshaLoggingEventBuilder();

  // -----------------------------------------------------------------------------------------------------------------
  // --------------------< SLF4J Logger implementation >--------------------------------------------------------------
  // -----------------------------------------------------------------------------------------------------------------

  /**
   * Reference to the real logger.
   */
  private static final Logger logger = LoggerFactory.getLogger(NakshaLogger.class);

  /**
   * The string builder used by this thread local logger.
   */
  private final @NotNull StringBuilder sb = new StringBuilder();

  private @NotNull String prefix(@NotNull String message) {
    final StringBuilder sb = this.sb;
    sb.setLength(0);
    sb.append(context.streamId());
    sb.append(':');
    sb.append(NanoTime.timeSince(context.startNanos(), TimeUnit.MICROSECONDS));
    sb.append("us - ");
    sb.append(message);
    return sb.toString();
  }

  @Override
  public String getName() {
    return context.streamId();
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

  @Deprecated
  @Override
  public void trace(String format, Object arg) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void trace(String format, Object arg1, Object arg2) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void trace(String format, Object... arguments) {
    if (logger.isTraceEnabled()) {
      logger.trace(prefix(format), arguments);
    }
  }

  @Deprecated
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

  @Deprecated
  @Override
  public void trace(Marker marker, String msg) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(msg));
    }
  }

  @Deprecated
  @Override
  public void trace(Marker marker, String format, Object arg) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(format), argArray);
    }
  }

  @Deprecated
  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    if (logger.isTraceEnabled(marker)) {
      logger.trace(marker, prefix(msg), t);
    }
  }

  @CheckReturnValue
  public @NotNull NakshaLoggingEventBuilder atTrace(@NotNull String message) {
    eventBuilder.eventBuilder = atTrace().setMessage(message);
    return eventBuilder;
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public void debug(@NotNull String message) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(message));
    }
  }

  @Deprecated
  @Override
  public void debug(String format, Object arg) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void debug(String format, Object arg1, Object arg2) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void debug(String format, Object... arguments) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(format), arguments);
    }
  }

  @Deprecated
  @Override
  public void debug(@NotNull String message, @NotNull Throwable t) {
    if (logger.isDebugEnabled()) {
      logger.debug(prefix(message), t);
    }
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return logger.isDebugEnabled(marker);
  }

  @Deprecated
  @Override
  public void debug(Marker marker, String msg) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(msg));
    }
  }

  @Deprecated
  @Override
  public void debug(Marker marker, String format, Object arg) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(format), arguments);
    }
  }

  @Deprecated
  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    if (logger.isDebugEnabled(marker)) {
      logger.debug(marker, prefix(msg), t);
    }
  }

  @CheckReturnValue
  public @NotNull NakshaLoggingEventBuilder atDebug(@NotNull String message) {
    eventBuilder.eventBuilder = atDebug().setMessage(message);
    return eventBuilder;
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public void info(@NotNull String message) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(message));
    }
  }

  @Deprecated
  @Override
  public void info(String format, Object arg) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void info(String format, Object arg1, Object arg2) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void info(String format, Object... arguments) {
    if (logger.isInfoEnabled()) {
      logger.info(prefix(format), arguments);
    }
  }

  @Deprecated
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

  @Deprecated
  @Override
  public void info(Marker marker, String msg) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(msg));
    }
  }

  @Deprecated
  @Override
  public void info(Marker marker, String format, Object arg) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void info(Marker marker, String format, Object... arguments) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(format), arguments);
    }
  }

  @Deprecated
  @Override
  public void info(Marker marker, String msg, Throwable t) {
    if (logger.isInfoEnabled(marker)) {
      logger.info(marker, prefix(msg));
    }
  }

  @CheckReturnValue
  public @NotNull NakshaLoggingEventBuilder atInfo(@NotNull String message) {
    eventBuilder.eventBuilder = atInfo().setMessage(message);
    return eventBuilder;
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

  @Deprecated
  @Override
  public void warn(String format, Object arg) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(format), arg);
    }
  }

  @Deprecated
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

  @Deprecated
  @Override
  public void warn(Marker marker, String msg) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(msg));
    }
  }

  @Deprecated
  @Override
  public void warn(Marker marker, String format, Object arg) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(format), arguments);
    }
  }

  @Deprecated
  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    if (logger.isWarnEnabled(marker)) {
      logger.warn(marker, prefix(msg), t);
    }
  }

  @Deprecated
  @Override
  public void warn(String format, Object arg1, Object arg2) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void warn(String msg, Throwable t) {
    if (logger.isWarnEnabled()) {
      logger.warn(prefix(msg), t);
    }
  }

  @CheckReturnValue
  public @NotNull NakshaLoggingEventBuilder atWarn(@NotNull String message) {
    eventBuilder.eventBuilder = atWarn().setMessage(message);
    return eventBuilder;
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

  @Deprecated
  @Override
  public void error(String format, Object arg) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void error(String format, Object arg1, Object arg2) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void error(String format, Object... arguments) {
    if (logger.isErrorEnabled()) {
      logger.error(prefix(format), arguments);
    }
  }

  @Deprecated
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

  @Deprecated
  @Override
  public void error(Marker marker, String msg) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(msg));
    }
  }

  @Deprecated
  @Override
  public void error(Marker marker, String format, Object arg) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(format), arg);
    }
  }

  @Deprecated
  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(format), arg1, arg2);
    }
  }

  @Deprecated
  @Override
  public void error(Marker marker, String format, Object... arguments) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(format), arguments);
    }
  }

  @Deprecated
  @Override
  public void error(Marker marker, String msg, Throwable t) {
    if (logger.isErrorEnabled(marker)) {
      logger.error(marker, prefix(msg), t);
    }
  }

  @CheckReturnValue
  public @NotNull NakshaLoggingEventBuilder atError(@NotNull String message) {
    eventBuilder.eventBuilder = atError().setMessage(message);
    return eventBuilder;
  }
}
