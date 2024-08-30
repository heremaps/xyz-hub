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

package com.here.xyz.util.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Arrays;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseConfig {
  public static BaseConfig instance;

  {
    instance = this;
  }

  /**
   * The port of the HTTP server.
   */
  public int HTTP_PORT;

  /**
   * The maximum size of an event transiting between connector -> service -> client. Validation is only applied when
   * MAX_SERVICE_RESPONSE_SIZE is bigger than zero.
   *
   * @deprecated Use instead MAX_UNCOMPRESSED_RESPONSE_SIZE
   */
  public int MAX_SERVICE_RESPONSE_SIZE;

  /**
   * The maximum uncompressed request size in bytes supported on API calls. If uncompressed request size is bigger than
   * MAX_UNCOMPRESSED_REQUEST_SIZE, an error with status code 413 will be sent.
   */
  public long MAX_UNCOMPRESSED_REQUEST_SIZE;

  /**
   * The maximum uncompressed response size in bytes supported on API calls. If uncompressed response size is bigger than
   * MAX_UNCOMPRESSED_RESPONSE_SIZE, an error with status code 513 will be sent.
   */
  public long MAX_UNCOMPRESSED_RESPONSE_SIZE;

  /**
   * The maximum http response size in bytes supported on API calls. If response size is bigger than MAX_HTTP_RESPONSE_SIZE, an error with
   * status code 513 will be sent. Validation is only applied when MAX_HTTP_RESPONSE_SIZE is bigger than zero.
   *
   * @deprecated Use instead MAX_UNCOMPRESSED_RESPONSE_SIZE
   */
  public int MAX_HTTP_RESPONSE_SIZE;

  /**
   * The name of the upload limit header
   */
  public String UPLOAD_LIMIT_HEADER_NAME;

  /**
   * The message which gets returned if UPLOAD_LIMIT is reached
   */
  public String UPLOAD_LIMIT_REACHED_MESSAGE;

  /**
   * The name of the health check header to instruct for additional health status information.
   */
  public String HEALTH_CHECK_HEADER_NAME;

  /**
   * The value of the health check header to instruct for additional health status information.
   */
  public String HEALTH_CHECK_HEADER_VALUE;

  /**
   * An identifier for the service environment.
   */
  public String ENVIRONMENT_NAME;

  /**
   * The AWS region this service is running in. Value is <code>null</code> if not running in AWS.
   */
  public String AWS_REGION;

  /**
   * When set, modifies the Stream-Info header name to the value specified.
   */
  public String CUSTOM_STREAM_INFO_HEADER_NAME;

  /**
   * Flag indicating whether the author should be retrieved from the custom header Author.
   */
  public boolean USE_AUTHOR_FROM_HEADER = false;

  /**
   * Additional application name variable.
   */
  public String APP_NAME;

  /**
   * Helper method to split a string that is a comma separated list into a list of strings including
   * proper trimming of the elements in the list.
   * @param value The comma separated list as string
   * @return The actual list of strings
   */
  protected static final List<String> fromCommaSeparatedList(String value) {
    if (value == null)
      return List.of();
    return Arrays.asList(value.split(",")).stream().map(part -> part.trim()).toList();
  }
}
