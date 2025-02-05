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

package com.here.xyz.jobs.util;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.web.XyzWebClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.time.Duration;

public class JobWebClient extends XyzWebClient {
  private static JobWebClient instance = new JobWebClient(Config.instance.JOB_API_ENDPOINT.toString());
  public static String userAgent = DEFAULT_USER_AGENT;

  public JobWebClient(String baseUrl) {
    super(baseUrl, userAgent);
  }

  @Override
  public boolean isServiceReachable() {
    try {
      request(HttpRequest.newBuilder()
          .uri(uri("/health"))
          .timeout(Duration.of(3, SECONDS)));
    }
    catch (WebClientException e) {
      return false;
    }
    return true;
  }

  public void postStepUpdate(Step<?> step) throws WebClientException {
    request(HttpRequest.newBuilder()
        .uri(uri("/admin/jobs/" + step.getJobId() + "/steps"))
        .header(CONTENT_TYPE, JSON_UTF_8.toString())
        .method("POST", BodyPublishers.ofByteArray(XyzSerializable.serialize(step).getBytes())));
  }

  public static JobWebClient getInstance() {
    return instance;
  }
}
