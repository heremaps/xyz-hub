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

import static com.here.xyz.httpconnector.util.jobs.Export.ERROR_DESCRIPTION_HTTP_TRIGGER_FAILED;
import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.VML;
import static com.here.xyz.httpconnector.util.jobs.Job.ERROR_TYPE_FINALIZATION_FAILED;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.prepared;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.task.StatusHandler;
import com.here.xyz.httpconnector.util.jobs.CombinedJob;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.web.LegacyHubWebClient;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import java.util.ConcurrentModificationException;
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
        getQueue().stream().filter(job -> (job instanceof Export || job instanceof CombinedJob)).forEach(job -> processJob(job));
    }

    private void processJob(Job job) {
        try {
            //Check Capacity
            ((Future<Job>) job.isProcessingPossible())
                .compose(j -> loadCurrentConfig(job))
                .compose(currentJob -> {
                /*
                Job-Life-Cycle:
                waiting -> (executing) -> executed -> executing_trigger -> trigger_executed -> collecting_trigger_status -> finalized
                all stages can end up in failed
                 */
                    switch (currentJob.getStatus()) {
                        case aborted:
                            //Abort has happened on other node
                            removeJob(job);
                            StatusHandler.getInstance().abortJob(job);
                            break;
                        case waiting:
                            updateJobStatus(currentJob, Job.Status.queued);
                            break;
                        case queued:
                            updateJobStatus(currentJob, Job.Status.preparing)
                                .onSuccess(f -> prepareJob(currentJob));
                            break;
                        case prepared:
                            updateJobStatus(currentJob, Job.Status.executing)
                                .onSuccess(f -> currentJob.execute());
                            break;
                        case executed:
                            updateJobStatus(currentJob, Job.Status.executing_trigger)
                                .onSuccess(f -> {
                                    if (currentJob instanceof Export export
                                        && export.getExportTarget() != null
                                        && export.getExportTarget().getType() == VML
                                        && export.getStatistic() != null
                                        && export.getStatistic().getFilesUploaded() > 0
                                        && export.getStatistic().getBytesUploaded() > 0
                                        && !export.readParamSkipTrigger())
                                        //Only here we need a trigger
                                        postTrigger(currentJob);
                                    else
                                        currentJob.finalizeJob()
                                            .onFailure(t -> setFinalizationFailed(currentJob, t));
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
        catch (ConcurrentModificationException e) {
            /*
            Catch any ConcurrentModificationException from the actual job execution to not interfere with ConcurrentModificationExceptions
            being caught for the process-queue.
             */
            logger.error("[{}]", job.getId(), e);
        }
    }

    private static Future<Job> setFinalizationFailed(Job currentJob, Throwable t) {
        return setJobFailed(currentJob, t.getMessage(), ERROR_TYPE_FINALIZATION_FAILED);
    }

    @Override
    protected Export validateJob(Job j) {
        //Currently not needed
        return null;
    }

    @Override
    protected void prepareJob(Job job) {
        if (job instanceof Export export) {
            export.checkPersistentExports()
                    .onFailure(e -> {
                        logger.warn("job[{}] CheckPersistentExports has failed!", job.getId(), e);
                        setJobFailed(job, Export.ERROR_DESCRIPTION_PERSISTENT_EXPORT_FAILED, Export.ERROR_TYPE_EXECUTION_FAILED);
                    });
        }else
            updateJobStatus(job, prepared);
    }

    protected Future<String> postTrigger(Job job) {
        return LegacyHubWebClient.executeHTTPTrigger((Export) job)
            .onSuccess(triggerId -> {
                //Add import ID
                ((Export) job).setTriggerId(triggerId);
                updateJobStatus(job, Job.Status.trigger_executed);
            })
            .onFailure(e -> {
                if (e instanceof HttpException)
                    setJobFailed(job, Export.ERROR_DESCRIPTION_TARGET_ID_INVALID, ERROR_TYPE_FINALIZATION_FAILED);
                else
                    setJobFailed(job, ERROR_DESCRIPTION_HTTP_TRIGGER_FAILED, ERROR_TYPE_FINALIZATION_FAILED);
            });
    }

    protected void collectTriggerStatus(Job<?> job) {
        //executeHttpTrigger
        if (((Export) job).getExportTarget().getType() == VML) {
            LegacyHubWebClient.executeHTTPTriggerStatus((Export) job)
                .onFailure(e -> {
                    if (e instanceof HttpException)
                        setJobFailed(job, Export.ERROR_DESCRIPTION_TARGET_ID_INVALID, ERROR_TYPE_FINALIZATION_FAILED);
                    else
                        setJobFailed(job, Export.ERROR_DESCRIPTION_HTTP_TRIGGER_STATUS_FAILED, ERROR_TYPE_FINALIZATION_FAILED);
                })
                .onSuccess(status -> {
                    switch (status) {
                        case "initialized":
                        case "submitted":
                            //In other cases reset status for next status query
                            updateJobStatus(job, Job.Status.trigger_executed);
                            return;
                        case "succeeded":
                            logger.info("job[{}] execution of '{}' succeeded ", job.getId(), ((Export) job).getTriggerId());
                            job.finalizeJob().onFailure(t -> setFinalizationFailed(job, t));
                            return;
                        case "cancelled":
                        case "failed":
                            logger.warn("job[{}] Trigger '{}' failed with state '{}'", job.getId(), ((Export) job).getTriggerId(), status);
                            setJobFailed(job, ERROR_DESCRIPTION_HTTP_TRIGGER_FAILED, ERROR_TYPE_FINALIZATION_FAILED);
                    }
                });
        }
        else
            //Skip collecting Trigger
            job.finalizeJob().onFailure(t -> setFinalizationFailed(job, t));
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
                catch (ConcurrentModificationException e) {
                    /*
                    Some element of the queue has been removed or added during the runtime of the process method.
                    Execution can be stopped for this time, and we wait for the next process execution to ensure to not
                    process any elements that have been removed.
                     */
                }
                catch (InterruptedException | CannotDecodeException ignored) {
                    //Nothing to do here.
                }
                catch (Exception e) {
                    logger.error("Exception in queue:", e);
                }
            });
        }
        catch (Exception e) {
            logger.error("Error when executing ExportQueue! ", e);
        }
    }
}
