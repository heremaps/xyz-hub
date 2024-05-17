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
package com.here.xyz.jobs.steps.execution;

import com.here.xyz.jobs.config.DynamoJobConfigClient;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CleanUpExecutor {
    private static final Logger logger = LogManager.getLogger();
    private static final long CHECK_PERIOD_IN_MIN = 30;
    private static final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
    private static final CleanUpExecutor instance = new CleanUpExecutor();
    private static final int CLEANUP_JOBS_OLDER_THAN = 14 * 24 * 60 * 60 * 1000;        //14 Days
    private static List<String> AVAILABLE_STATE_MACHINES = new ArrayList<>();

    public static CleanUpExecutor getInstance() {
        return instance;
    }

    public void start(){
        exec.scheduleAtFixedRate(() -> cleanUp(), 0, CHECK_PERIOD_IN_MIN, SECONDS);
    }

    protected void cleanUp() {
        logger.info("Start cleanup of StateMachines!");
        JobExecutor.getInstance().list()
                .compose(stateMachineList -> retrieveAvailableStateMachines(stateMachineList))
                .compose(f -> deleteStateMachines());
    }

    protected Future<Void> retrieveAvailableStateMachines(List<String> stateMachineList) {
            AVAILABLE_STATE_MACHINES = new ArrayList<>(){{addAll(stateMachineList);}};
            return Future.succeededFuture();
    }

    protected Future<Void> deleteStateMachines() {
        DynamoJobConfigClient.getInstance().loadFailedAndSucceededJobsOlderThan(CLEANUP_JOBS_OLDER_THAN )
                .onSuccess(jobs -> jobs.stream().forEach(
                        job -> {
                            String stateMachineArn = job.getStateMachineArn();
                            if(AVAILABLE_STATE_MACHINES.contains(stateMachineArn)) {
                                JobExecutor.getInstance().delete(stateMachineArn)
                                        .onSuccess(f -> logger.info("[{}] Deleted state machine {}", job.getId(), job.getStateMachineArn()))
                                        .onFailure(e -> logger.info("[{}] Can`t delete state machine {}", job.getId(), job.getStateMachineArn(), e));
                            }
                        }));
        return Future.succeededFuture();
    }
}
