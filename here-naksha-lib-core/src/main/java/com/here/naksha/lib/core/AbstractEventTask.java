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

import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.feature.LoadFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A task processing an {@link Event} and producing an {@link XyzResponse}. The task may send multiple events through the attached
 * event-pipeline and modify the pipeline while processing the events. For example to modify features at least a
 * {@link LoadFeaturesEvent} is needed to fetch the current state of the features and then (optionally) performing a merge, and executing
 * eventually the {@link ModifyFeaturesEvent}. Other combinations are possible.
 */
public abstract class AbstractEventTask<EVENT extends Event> extends AbstractTask<XyzResponse> {

  /**
   * The constructor to use, when creating new event-task instances.
   */
  public static final AtomicReference<@Nullable Supplier<@NotNull AbstractEventTask<?>>> eventTaskFactory =
      new AtomicReference<>();

  /**
   * Creates a new even-task.
   *
   * @param streamId The stream-id for which to create a task.
   */
  protected AbstractEventTask(@Nullable String streamId) {
    super(streamId);
  }

  /**
   * Creates a new even-task.
   *
   * @param streamId The stream-id for which to create a task.
   * @param startNanos The start time in nanoseconds, see {@link com.here.naksha.lib.core.util.NanoTime}.
   */
  protected AbstractEventTask(@Nullable String streamId, long startNanos) {
    super(streamId, startNanos);
  }

  /**
   * A method that creates an error-response from the given caught exception.
   *
   * @param throwable The exception caught.
   * @return The error-response.
   */
  @Override
  protected @NotNull XyzResponse errorResponse(@NotNull Throwable throwable) {
    return new ErrorResponse(throwable, streamId);
  }

  /**
   * The main event to be processed by the task, created by the constructor.
   */
  private EVENT event;

  /**
   * The event pipeline of this task.
   */
  protected final EventPipeline pipeline = new EventPipeline();

  /**
   * Returns the main event of this task. Internally a task may generate multiple sub-events and send them through the pipeline. For example
   * a {@link ModifyFeaturesEvent} requires a {@link LoadFeaturesEvent} pre-flight event, some other events may require other pre-flight
   * request.
   *
   * @return the main event of this task.
   * @throws IllegalStateException If the event is {@code null}.
   */
  protected final @NotNull EVENT getEvent() {
    if (event == null) throw new IllegalStateException();
    return event;
  }

  /**
   * Set the event of this task.
   *
   * @param event The event to set.
   * @return the previously set event.
   * @throws IllegalStateException If the task has been started already.
   */
  protected final @Nullable EVENT setEvent(@NotNull EVENT event) {
    final EVENT old = this.event;
    lock();
    this.event = event;
    unlock();
    return old;
  }
}
