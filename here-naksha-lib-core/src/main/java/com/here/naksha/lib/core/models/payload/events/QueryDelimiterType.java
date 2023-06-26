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

/**
 * The query delimiter type as defined in <a
 * href="https://datatracker.ietf.org/doc/html/rfc3986#section-2.2">RFC-3986</a>. Note that
 * technically all characters that are not explicitly unreserved, maybe used as delimiters, which
 * means all characters not being ALPHA / DIGIT / "-" / "." / "_" / "~". However, it is recommended
 * to only use the reserved characters as delimiters to guarantee that standard encoding algorithms
 * work as intended.
 */
public enum QueryDelimiterType {
  /** A general delimiter that is being used as delimiters of the generic URI components. */
  GENERAL,
  /** A sub-delimiter. */
  SUB,
  /**
   * An unsafe delimiter, characters not being unreserved, but neither reserved for the delimiter
   * purpose, for example ">".
   */
  UNSAFE;
}
