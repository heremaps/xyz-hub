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
package com.here.naksha.app.common;

import static com.here.naksha.app.service.http.NakshaHttpHeaders.STREAM_ID;

import java.net.http.HttpResponse;
import java.util.Optional;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class ResponseAssertions {

  private final HttpResponse<String> subject;

  private ResponseAssertions(HttpResponse<String> subject) {
    this.subject = subject;
  }

  public static ResponseAssertions assertThat(HttpResponse<String> response) {
    Assertions.assertNotNull(response, "Can't run assertions on null HttpResponse");
    return new ResponseAssertions(response);
  }

  public ResponseAssertions hasStatus(int expectedStatus) {
    Assertions.assertEquals(expectedStatus, subject.statusCode(), "Response status mismatch");
    return this;
  }

  public ResponseAssertions hasStreamIdHeader(String expectedStreamId) {
    return hasHeader(STREAM_ID, expectedStreamId);
  }

  public ResponseAssertions hasHeader(String key, String expectedValue) {
    Optional<String> headerVal = subject.headers().firstValue(key);
    headerVal.ifPresentOrElse(
        headerValue -> Assertions.assertEquals(expectedValue, headerValue),
        () -> Assertions.fail("Response does not have header with key: " + key));
    return this;
  }

  public ResponseAssertions hasJsonBody(String expectedJsonBody) {
    return hasJsonBody(expectedJsonBody, "Actual and expected json body don't match");
  }

  public ResponseAssertions hasJsonBody(String expectedJsonBody, String failureMessage) {
    String actualBody = subject.body();
    Assertions.assertNotNull(actualBody, "Response body is null");
    try {
      JSONAssert.assertEquals(failureMessage, expectedJsonBody, actualBody, JSONCompareMode.LENIENT);
    } catch (JSONException e) {
      Assertions.fail("Unable to parse response body", e);
    }
    return this;
  }
}
