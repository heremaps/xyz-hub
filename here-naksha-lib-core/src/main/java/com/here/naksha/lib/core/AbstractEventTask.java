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
import com.here.naksha.lib.core.models.payload.XyzResponseType;
import com.here.naksha.lib.core.models.payload.events.feature.LoadFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A task processing an {@link Event} and producing an {@link XyzResponse}. The task may send multiple events through the attached
 * event-pipeline and modify the pipeline while processing the events. For example to modify features at least a {@link LoadFeaturesEvent}
 * is needed to fetch the current state of the features and then (optionally) performing a merge, and executing eventually the
 * {@link ModifyFeaturesEvent}. Other combinations are possible.
 */
@SuppressWarnings("unused")
@Deprecated
public abstract class AbstractEventTask<EVENT extends Event>
    extends AbstractTask<XyzResponse, AbstractEventTask<EVENT>> {

  /**
   * Creates a new even-task.
   *
   * @param naksha The reference to the Naksha host.
   */
  @Deprecated
  protected AbstractEventTask(@NotNull INaksha naksha) {
    super(naksha, new NakshaContext());
    pipeline = new EventPipeline(naksha());
  }

  /**
   * A method that creates an error-response from the given caught exception.
   *
   * @param throwable The exception caught.
   * @return The error-response.
   */
  @Override
  @Deprecated
  protected @NotNull XyzResponse errorResponse(@NotNull Throwable throwable) {
    return new ErrorResponse(throwable, context().streamId());
  }

  /**
   * The main event to be processed by the task.
   */
  @Deprecated
  private EVENT event;

  /**
   * The response types.
   */
  @Deprecated
  private @NotNull List<@NotNull XyzResponseType> responseTypes = new ArrayList<>();

  /**
   * The event pipeline of this task.
   */
  @Deprecated
  private final @NotNull EventPipeline pipeline;

  /**
   * Returns the event-pipeline.
   *
   * @return The event-pipeline.
   */
  @Deprecated
  public @NotNull EventPipeline pipeline() {
    return pipeline;
  }

  /**
   * Returns the main event of this task. Internally a task may generate multiple sub-events and send them through the pipeline. For example
   * a {@link ModifyFeaturesEvent} requires a {@link LoadFeaturesEvent} pre-flight event, some other events may require other pre-flight
   * request.
   *
   * @return the main event of this task.
   * @throws IllegalStateException If the event is {@code null}.
   */
  @Deprecated
  public final @NotNull EVENT getEvent() {
    if (event == null) {
      throw new IllegalStateException();
    }
    return event;
  }

  /**
   * Set the event for this task.
   *
   * @param event The event to set.
   * @return the previously set event.
   * @throws IllegalStateException If the task has been started already.
   */
  @Deprecated
  public final @Nullable EVENT setEvent(@NotNull EVENT event) {
    final EVENT old = this.event;
    final Thread thread = Thread.currentThread();
    if (getThread() == thread) {
      this.event = event;
      return old;
    }
    lockAndRequireNew();
    this.event = event;
    unlock();
    return old;
  }

  /**
   * Returns the preferred response types.
   *
   * @return The preferred response types.
   */
  @Deprecated
  public @NotNull List<? extends XyzResponseType> responseTypes() {
    return responseTypes;
  }
}
