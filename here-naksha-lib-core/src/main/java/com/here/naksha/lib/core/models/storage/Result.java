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
package com.here.naksha.lib.core.models.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.Typed;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All results must extend this abstract base class.
 */
public abstract class Result implements Typed {

  /**
   * Member variable that can be set to the cursor.
   */
  @JsonIgnore
  protected @Nullable ResultCursor<?> cursor;

  /**
   * Return the {@link ResultCursor}, if any is available. If no result-cursor is available, throws an exception to handle result. This is
   * to simplify result processing when needed: <pre>{@code
   * try (ResultCursor<XyzFeature> cursor = session.execute(request).cursor(XyzFeature.class)) {
   *   for (XyzFeature feature : cursor) {
   *     ...
   *   }
   * } catch (NoCursor e) {
   *   if (e.result instanceof SuccessResult) ...
   * }}</pre>
   *
   * @param featureClass The class of the feature-type to be expected from the cursor; {@code null} for auto-detect.
   * @param <T>          The feature-type the cursor should return.
   * @return the cursor.
   */
  @JsonIgnore
  public <T> @NotNull ResultCursor<T> cursor(@Nullable Class<T> featureClass) throws NoCursor {
    if (cursor != null) {
      return cursor.withType(featureClass);
    }
    throw new NoCursor(this);
  }

  /**
   * Return the {@link ResultCursor}, if any is available. If no result-cursor is available, throws an exception to handle result. This is
   * to simplify result processing when needed: <pre>{@code
   * try (ResultCursor<XyzFeature> cursor = session.execute(request).cursor()) {
   *   for (XyzFeature feature : cursor) {
   *     ...
   *   }
   * } catch (NoCursor e) {
   *   if (e.result instanceof SuccessResult) ...
   * }}</pre>
   *
   * @param <T> The feature-type the cursor should return.
   * @return the cursor.
   */
  @SuppressWarnings("unchecked")
  @JsonIgnore
  public <T> @NotNull ResultCursor<T> cursor() throws NoCursor {
    if (cursor != null) {
      return (ResultCursor<T>) cursor;
    }
    throw new NoCursor(this);
  }
}
