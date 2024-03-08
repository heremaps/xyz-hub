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

package com.here.xyz.httpconnector.job.config;

import com.here.xyz.httpconnector.job.Job;
import com.here.xyz.httpconnector.job.RuntimeInfo.State;
import com.here.xyz.hub.config.Initializable;
import io.vertx.core.Future;
import java.util.List;

public abstract class JobConfigClient implements Initializable {

  public static JobConfigClient getInstance() {
    //TODO: Chose the right client impl using SPI here
    return new DynamoJobConfigClient(""); //TODO: Get table name from env vars
  }

  public abstract Future<Job> loadJob(String resourceKey, String jobId);

  /**
   * Load all jobs with a specified type.
   * @param state
   * @return
   */
  public abstract Future<List<Job>> loadJobs(State state);

  /**
   * Load all jobs related to a specified resourceKey (e.g., space ID).
   * @param resourceKey
   * @return
   */
  public abstract Future<List<Job>> loadJobs(String resourceKey);

  public abstract Future<Void> storeJob(String resourceKey, Job job);

  public abstract Future<Void> deleteJob(String resourceKey, String jobId);
}
