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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ErrorCauseResolverTest {

  @Test
  void resolvesCodedRootCauseFromWrappedMpaFailure() {
    RuntimeException parseFailure = new RuntimeException("E316052 Cannot parse configuration.");
    RuntimeException workflowFailure = new RuntimeException("Failed to process a microbatch.", parseFailure);
    RuntimeException dispatchFailure = new RuntimeException("MicroBatch Failed.", workflowFailure);
    RuntimeException mpaFailure = new RuntimeException("Failed to execute MPA", dispatchFailure);

    assertEquals("E316052 Cannot parse configuration.", ErrorCauseResolver.resolve(mpaFailure));
  }

  @Test
  void preservesProfileNameInCodedRootCause() {
    RuntimeException profileFailure = new RuntimeException(
        "E316053 [customer-profile_42] Configuration not found for profile.");

    assertEquals("E316053 [customer-profile_42] Configuration not found for profile.",
        ErrorCauseResolver.resolve(new RuntimeException("Failed to execute MPA", profileFailure)));
  }

  @Test
  void retainsExistingImmediateCauseFormatWhenThereIsNoCodedError() {
    IllegalStateException cause = new IllegalStateException("ordinary failure");

    assertEquals("IllegalStateException: ordinary failure",
        ErrorCauseResolver.resolve(new RuntimeException("wrapper", cause)));
  }

  @Test
  void extractsCodedMessageFromJsonEscapedFailureCause() {
    String cause = "{\"errorMessage\":\"Failed to execute MPA\",\"stackTrace\":\"wrapper\\n"
        + "Caused by: ConfigurationParseException: E316052 Cannot parse configuration.\\nnext\"}";

    assertEquals("E316052 Cannot parse configuration.", ErrorCauseResolver.extractCodedMessage(cause));
  }

  @Test
  void handlesCyclesAndBoundsStoredCause() {
    SelfCausingException exception = new SelfCausingException("E316052 " + "x".repeat(3_000));

    String cause = ErrorCauseResolver.resolve(exception);

    assertEquals(ErrorCauseResolver.MAX_CAUSE_LENGTH, cause.length());
    assertTrue(cause.endsWith(" ..."));
  }

  @Test
  void returnsNullWithoutAnExceptionOrCause() {
    assertNull(ErrorCauseResolver.resolve(null));
    assertNull(ErrorCauseResolver.resolve(new RuntimeException("ordinary failure")));
  }

  private static class SelfCausingException extends RuntimeException {
    private SelfCausingException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }
}
