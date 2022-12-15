/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
package com.here.xyz.httpconnector.util.scheduler;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.Core;
import com.mchange.v3.decode.CannotDecodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.concurrent.*;

public abstract class JobQueue implements Runnable {
    protected static final Logger logger = LogManager.getLogger();

    private volatile static HashSet<Job> JOB_QUEUE = new HashSet<>();

    protected boolean commenced = false;
    protected ScheduledFuture<?> executionHandle;

    public static int CORE_POOL_SIZE = 30;

    protected static ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE, Core.newThreadFactory("jobqueue"));

    protected abstract void process() throws InterruptedException, CannotDecodeException;

    protected abstract void validateJob(Job j);

    protected abstract void prepareJob(Job j);

    protected abstract void executeJob(Job j);

    protected abstract void finalizeJob(Job j0);

    protected abstract void failJob(Job j);

    public static void addJob(Job job){
        logger.info("[{}] added to JobQueue!", job.getId());
        JOB_QUEUE.add(job);
    }

    public static void removeJob(Job job){
        logger.info("[{}] removed from JobQueue!", job.getId());
        JOB_QUEUE.remove(job);
    }

    public static boolean checkRunningImportJobsOnSpace(String targetSpaceId){
        for (Job j : JOB_QUEUE ) {
            if(targetSpaceId != null  && targetSpaceId != null
                && targetSpaceId.equalsIgnoreCase(j.getTargetSpaceId())){
                return true;
            }
        }
        return false;
    }

    public static HashSet<Job> getQueue(){
        return JOB_QUEUE;
    }

    public static void printQueue(){
        JOB_QUEUE.forEach(job -> logger.info(job.id));
    }

    protected static int queueSize(){
        return JOB_QUEUE.size();
    }

    /**
     * Begins executing the JobQueue processing - periodically and asynchronously.
     *
     * @return This check for chaining
     */
    public JobQueue commence() {
        if (!commenced) {
            logger.info("Start!");
            commenced = true;
            executionHandle = executorService.scheduleWithFixedDelay(this, 0, CService.configuration.JOB_CHECK_QUEUE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
        return this;
    }

    @Override
    public void run() {
        try {
            this.executorService.submit(() -> {
                try {
                    process();
                }
                catch (InterruptedException | CannotDecodeException ignored) {
                    //Nothing to do here.
                }
            });
        }catch (Exception e) {
            logger.error("{}: Error when executing Job", this.getClass().getSimpleName(), e);
        }
    }
}
