/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.execution;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ErrorCauseResolver {
  static final int MAX_CAUSE_LENGTH = 2_048;
  private static final Pattern ERROR_MESSAGE_PATTERN = Pattern.compile("\\bE\\d{6}\\b[^\\r\\n\\\\\"]*");

  private ErrorCauseResolver() {}

  /**
   * Resolves the most useful error cause from a wrapped exception. A coded application error is preferred over framework wrapper errors.
   * If there is no coded error in the chain, the existing one-level cause format is retained.
   */
  public static String resolve(Throwable error) {
    if (error == null)
      return null;

    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = error;
    String codedCause = null;
    while (current != null && visited.add(current)) {
      String codedMessage = extractCodedMessage(current.getMessage());
      if (codedMessage != null)
        codedCause = codedMessage;
      current = current.getCause();
    }

    if (codedCause != null)
      return truncate(codedCause);

    Throwable directCause = error.getCause();
    if (directCause == null)
      return null;

    String cause = directCause.getClass().getSimpleName()
        + (directCause.getMessage() == null ? "" : ": " + directCause.getMessage());
    return truncate(cause);
  }

  /**
   * Extracts a coded application error from a plain stack trace or a JSON-escaped Step Functions failure cause.
   */
  public static String extractCodedMessage(String cause) {
    if (cause == null || cause.isBlank())
      return null;

    Matcher matcher = ERROR_MESSAGE_PATTERN.matcher(cause);
    String codedMessage = null;
    while (matcher.find())
      codedMessage = matcher.group().trim();
    return codedMessage == null ? null : truncate(codedMessage);
  }

  public static String truncate(String cause) {
    if (cause == null || cause.length() <= MAX_CAUSE_LENGTH)
      return cause;
    return cause.substring(0, MAX_CAUSE_LENGTH - 4) + " ...";
  }
}
