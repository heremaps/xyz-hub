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

package com.here.xyz.httpconnector.util.scheduler;

import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.VML;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.rest.HttpException;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Job-Queue for Export-Jobs (S3 -> RDS)
 */
public class ExportQueue extends JobQueue {
    private static final Logger logger = LogManager.getLogger();
    protected static ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE, Core.newThreadFactory("export-queue"));

    protected void process() throws InterruptedException, CannotDecodeException {

        for (Job job : getQueue()) {
            if (!(job instanceof Export))
                return;

            //Check Capacity
            isProcessingPossible(job)
                .compose(j -> loadCurrentConfig(job))
                .compose(currentJob -> {
                    /*
                    Job-Life-Cycle:
                    waiting -> (executing) -> executed -> executing_trigger -> trigger_executed -> collecting_trigger_status -> finalized
                    all stages can end up in failed
                     */
                    switch (currentJob.getStatus()) {
                        case finalized:
                            logger.info("job[{}] is finalized!", currentJob.getId());
                            break;
                        case failed:
                            logger.info("job[{}] has failed!", currentJob.getId());
                            break;
                        case waiting:
                            updateJobStatus(currentJob, Job.Status.queued);
                            break;
                        case queued:
                            updateJobStatus(currentJob, Job.Status.executing)
                                .onSuccess(f -> currentJob.execute());
                            break;
                        case executed:
                            updateJobStatus(currentJob, Job.Status.executing_trigger)
                                .onSuccess(f -> {
                                    if (((Export) currentJob).getExportTarget().getType().equals(VML)
                                        && ((Export) currentJob).getStatistic() != null
                                        && ((Export) currentJob).getStatistic().getFilesUploaded() > 0)
                                        //Only here we need a trigger
                                        postTrigger(currentJob);
                                    else
                                        currentJob.finalizeJob();
                                });
                            break;
                        case trigger_executed:
                            updateJobStatus(currentJob, Job.Status.collecting_trigger_status)
                                .onSuccess(f -> collectTriggerStatus(currentJob));
                            break;
                    }
                    return Future.succeededFuture();
                })
                .onFailure(e -> logError(e, job.getId()));
            }
    }

    @Override
    protected Export validateJob(Job j) {
        //Currently not needed
        return null;
    }

    @Override
    protected void prepareJob(Job j) {
        //Currently not needed
    }

    @Override
    protected boolean needRdsCheck(Job job) {
        //In next stage we need database resources
        if (job.getStatus().equals(Job.Status.queued))
            return true;
        return false;
    }

    protected Future<String> postTrigger(Job j) {
        return HubWebClient.executeHTTPTrigger((Export) j)
            .onSuccess(triggerId -> {
                //** Add import ID */
                ((Export) j).setTriggerId(triggerId);
                updateJobStatus(j, Job.Status.trigger_executed);
            })
            .onFailure(e -> {
                        if(e instanceof HttpException){
                            setJobFailed(j, Export.ERROR_TYPE_TARGET_ID_INVALID, Job.ERROR_TYPE_FINALIZATION_FAILED);
                        }else
                            setJobFailed(j, Export.ERROR_TYPE_HTTP_TRIGGER_FAILED, Job.ERROR_TYPE_FINALIZATION_FAILED);
                    }
            );
    }

    protected void collectTriggerStatus(Job j){
        //executeHttpTrigger
        if(((Export)j).getExportTarget().getType().equals(VML)) {
            HubWebClient.executeHTTPTriggerStatus((Export) j)
                    .onFailure(e -> {
                            if(e instanceof HttpException){
                                setJobFailed(j, Export.ERROR_TYPE_TARGET_ID_INVALID, Job.ERROR_TYPE_FINALIZATION_FAILED);
                            }else
                                setJobFailed(j, Export.ERROR_TYPE_HTTP_TRIGGER_STATUS_FAILED, Job.ERROR_TYPE_FINALIZATION_FAILED);
                        }
                    )
                    .onSuccess(status -> {
                        switch (status){
                            case "initialized":
                            case "submitted":
                                /** In other cases reset status for next status query */
                                updateJobStatus(j, Job.Status.trigger_executed);
                                return;
                            case "succeeded":
                                logger.info("job[{}] execution of '{}' succeeded ", j.getId(), ((Export) j).getTriggerId());
                                j.finalizeJob();
                                return;
                            case "cancelled":
                            case "failed":
                                logger.warn("job[{}] Trigger '{}' failed with state '{}'", j.getId(), ((Export) j).getTriggerId(), status);
                                setJobFailed(j, Export.ERROR_TYPE_HTTP_TRIGGER_FAILED, Job.ERROR_TYPE_FINALIZATION_FAILED);
                        }
                    });
        }else {
            /** skip collecting Trigger */
            j.finalizeJob();
        }
    }

    /**
     * Begins executing the ExportQueue processing - periodically and asynchronously.
     *
     * @return This check for chaining
     */
    public JobQueue commence() {
        if (!commenced) {
            logger.info("Start!");
            commenced = true;
            this.executionHandle = this.executorService.scheduleWithFixedDelay(this, 0, CService.configuration.JOB_CHECK_QUEUE_INTERVAL_MILLISECONDS, TimeUnit.MILLISECONDS);
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
            logger.error("Error when executing ExportQueue! ", e);
        }
    }
}
