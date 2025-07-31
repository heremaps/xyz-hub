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

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.web.JobWebClient;

import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;

public class StepWebClient extends JobWebClient {
  private static Map<InstanceKey, StepWebClient> instances = new ConcurrentHashMap<>();

  public StepWebClient(String baseUrl) {
    super(baseUrl);
  }

  protected StepWebClient(String baseUrl, Map<String, String> extraHeaders) {
    super(baseUrl, extraHeaders);
  }

  public static StepWebClient getInstance(String baseUrl) {
    return getInstance(baseUrl, null);
  }

  public static StepWebClient getInstance(String baseUrl, Map<String, String> extraHeaders) {
    InstanceKey key = new InstanceKey(baseUrl, extraHeaders);
    if (!instances.containsKey(key))
      instances.put(key, new StepWebClient(baseUrl, extraHeaders));
    return instances.get(key);
  }

  public void postStepUpdate(Step<?> step) throws WebClientException {
    request(HttpRequest.newBuilder()
        .uri(uri("/admin/jobs/" + step.getJobId() + "/steps"))
        .header(CONTENT_TYPE, JSON_UTF_8.toString())
        .method("POST", BodyPublishers.ofByteArray(XyzSerializable.serialize(step).getBytes())));
  }
}
