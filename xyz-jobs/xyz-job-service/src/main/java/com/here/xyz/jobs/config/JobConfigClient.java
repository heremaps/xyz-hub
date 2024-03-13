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

package com.here.xyz.jobs.config;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo.State;
import com.here.xyz.util.di.ImplementationProvider;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.List;

public abstract class JobConfigClient implements Initializable {

  public static JobConfigClient getInstance() {
    return Provider.provideInstance();
  }

  public static abstract class Provider implements ImplementationProvider {
    private static JobConfigClient client;
    public Provider() {}
    protected abstract JobConfigClient getInstance();
    private static JobConfigClient provideInstance() {
      if (client == null)
        client = ImplementationProvider.loadProvider(Provider.class).getInstance();
      return client;
    }
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
