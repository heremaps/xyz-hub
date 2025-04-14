/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util.service.errors;

import static com.here.xyz.util.service.errors.ErrorManager.getErrorDefinition;

import com.here.xyz.util.service.HttpException;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.Map;

public class DetailedHttpException extends HttpException {

  private static final long serialVersionUID = 1L;
  public final ErrorDefinition errorDefinition;
  public final Map<String, String> placeholders;

  public DetailedHttpException(String errorCode) {
    this(errorCode, null, null);
  }

  public DetailedHttpException(String errorCode, Map<String, String> placeholders) {
    this(errorCode, placeholders, null);
  }

  public DetailedHttpException(String errorCode, Throwable cause) {
    this(errorCode, null, cause);
  }

  public DetailedHttpException(String errorCode, Map<String, String> placeholders, Throwable cause) {
    super(HttpResponseStatus.valueOf(getErrorDefinition(errorCode).getStatus()), getErrorDefinition(errorCode).composeMessage(placeholders), cause);
    this.errorDefinition = getErrorDefinition(errorCode);
    this.placeholders = new HashMap<>() {{
      if (placeholders != null)
        putAll(placeholders);
      if (cause != null)
        put("cause", cause.getMessage());
    }};
  }
}