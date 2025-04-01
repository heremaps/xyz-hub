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

import static com.here.xyz.util.service.errors.ErrorManager.format;

import com.here.xyz.responses.DetailedErrorResponse;
import java.util.Map;

public class ErrorDefinition {

  private String title;
  private String code;
  private int status;
  private String cause;
  private String action;

  public String getTitle() {
    return title;
  }

  public String getFormattedTitle(Map<String, String> placeholders) {
    return format(getTitle(), placeholders);
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getCause() {
    return cause;
  }

  public String getFormattedCause(Map<String, String> placeholders) {
    return format(getCause(), placeholders);
  }

  public void setCause(String cause) {
    this.cause = cause;
  }

  public String getAction() {
    return action;
  }

  public String getFormattedAction(Map<String, String> placeholders) {
    return format(getAction(), placeholders);
  }

  public void setAction(String action) {
    this.action = action;
  }

  public String composeMessage(Map<String, String> placeholders) {
    return getFormattedTitle(placeholders);
  }

  public DetailedErrorResponse toErrorResponse(Map<String, String> placeholders) {
    return new DetailedErrorResponse()
        .withTitle(getFormattedTitle(placeholders))
        .withCode(getCode())
        .withCause(getFormattedCause(placeholders))
        .withAction(getFormattedAction(placeholders));
  }
}