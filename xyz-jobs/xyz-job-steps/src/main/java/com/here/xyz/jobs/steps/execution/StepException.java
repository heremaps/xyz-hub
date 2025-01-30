/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;

public class StepException extends RuntimeException {
  private String code;
  private boolean retryable;

  public StepException(String message, Throwable cause) {
    super(message, cause);
    if (cause instanceof ErrorResponseException responseException)
      setCode(codeFromErrorErrorResponseException(responseException));
  }

  public StepException(String message) {
    super(message);
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public StepException withCode(String code) {
    setCode(code);
    return this;
  }

  public boolean isRetryable() {
    return retryable;
  }

  public void setRetryable(boolean retryable) {
    this.retryable = retryable;
  }

  public StepException withRetryable(boolean retryable) {
    setRetryable(retryable);
    return this;
  }

  static String codeFromErrorErrorResponseException(ErrorResponseException responseException) {
    return "HTTP-" + responseException.getErrorResponse().statusCode();
  }
}
