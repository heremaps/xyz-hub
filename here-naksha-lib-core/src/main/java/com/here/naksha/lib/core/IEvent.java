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

import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import org.jetbrains.annotations.NotNull;

/**
 * The event that wraps a {@link Request}.
 */
public interface IEvent {

  /**
   * Returns the request of the event.
   *
   * @return the request of the event.
   */
  @NotNull
  Request<?> getRequest();

  /**
   * Replace the request of the event, returning the old request.
   *
   * @param request the new request.
   * @return The previous request.
   * @throws IllegalStateException if the thread calling the method does not hold the pipeline lock.
   */
  @NotNull
  Request<?> setRequest(@NotNull Request<?> request);

  /**
   * Send the event upstream to the next event handler. If no further handler is available, the default implementation at the end of each
   * pipeline will return a not implemented error response.
   *
   * @return the generated result.
   * @throws IllegalStateException if the thread calling the method does not hold the pipeline lock.
   */
  @NotNull
  Result sendUpstream();

  /**
   * Create a new event and send it upstream to the next event handler. If no further handler is available, the default implementation at
   * the end of each pipeline will return a not implemented error response. When the method returns, the current request is the same it was
   * before calling the method.
   *
   * @param request the request to send upstream.
   * @return the generated result.
   * @throws IllegalStateException if the thread calling the method does not hold the pipeline lock.
   */
  default @NotNull Result sendUpstream(@NotNull Request<?> request) {
    final Request<?> backup = setRequest(request);
    try {
      return sendUpstream();
    } finally {
      setRequest(backup);
    }
  }
}
