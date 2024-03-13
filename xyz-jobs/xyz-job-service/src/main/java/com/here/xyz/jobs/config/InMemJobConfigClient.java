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
import com.here.xyz.jobs.RuntimeInfo;
import io.vertx.core.Future;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemJobConfigClient extends JobConfigClient {
    private Map<JobKey, Job> jobMap = new ConcurrentHashMap<>();

    public static class Provider extends JobConfigClient.Provider {
        @Override
        public boolean chooseMe() {
            return "test".equals(System.getProperty("scope"));
        }

        @Override
        protected JobConfigClient getInstance() {
            return new InMemJobConfigClient();
        }
    }


    @Override
    public Future<Job> loadJob(String resourceKey, String jobId) {
        return Future.succeededFuture(jobMap.get(new JobKey(resourceKey, jobId)));
    }

    @Override
    public Future<List<Job>> loadJobs(RuntimeInfo.State state) {
        List<Job> jobs = jobMap.values().stream().filter(job -> job.getStatus().getState() == state).collect(Collectors.toList());
        return Future.succeededFuture(jobs);
    }

    @Override
    public Future<List<Job>> loadJobs(String resourceKey) {
        List<Job> jobs = jobMap.entrySet()
                .stream()
                .filter(e -> e.getKey().resourceKey.equals(resourceKey))
                .map(e -> e.getValue())
                .collect(Collectors.toList());
        return Future.succeededFuture(jobs);
    }

    @Override
    public Future<Void> storeJob(String resourceKey, Job job) {
        jobMap.put(new JobKey(resourceKey, job.getId()), job);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> deleteJob(String resourceKey, String jobId) {
        jobMap.remove(new JobKey(resourceKey, jobId));
        return Future.succeededFuture();
    }
    private record JobKey(String resourceKey, String jobId) {}
}
