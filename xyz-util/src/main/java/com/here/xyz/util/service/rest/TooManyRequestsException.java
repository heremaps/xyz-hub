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

package com.here.xyz.util.service.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.here.xyz.util.service.HttpException;
import java.util.Map;

public class TooManyRequestsException extends HttpException {

  public final ThrottlingReason reason;

  public TooManyRequestsException(String errorText, ThrottlingReason reason) {
    super(TOO_MANY_REQUESTS, errorText);
    this.reason = reason;
  }

  public TooManyRequestsException(String errorText, ThrottlingReason reason, Map<String, Object> errorDetails) {
    super(TOO_MANY_REQUESTS, errorText, errorDetails);
    this.reason = reason;
  }

  public TooManyRequestsException(String errorText, ThrottlingReason reason, Throwable cause) {
    super(TOO_MANY_REQUESTS, errorText, cause);
    this.reason = reason;
  }

  public enum ThrottlingReason {
    MEMORY("M"),
    QUOTA("Q"),
    STORAGE_QUEUE_FULL("S"),
    CONNECTOR("C");

    public final String shortCut;

    ThrottlingReason(String shortCut) {
      this.shortCut = shortCut;
    }
  }
}
