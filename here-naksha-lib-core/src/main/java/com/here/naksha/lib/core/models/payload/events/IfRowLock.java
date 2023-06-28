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
package com.here.naksha.lib.core.models.payload.events;

public enum IfRowLock {
  /**
   * WAIT - to indicate current DB operation should wait for other operation to release the row-level lock.
   */
  WAIT,
  /**
   * ABORT - to indicate current DB operation should abort immediately when other operation is found to have row-level lock.
   */
  ABORT;

  /**
   * Return enum based on passed value.
   *
   * @return The enum element, which matches the passed value.
   */
  public static IfRowLock of(String value) {
    if (value == null) {
      return null;
    }
    try {
      return valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
