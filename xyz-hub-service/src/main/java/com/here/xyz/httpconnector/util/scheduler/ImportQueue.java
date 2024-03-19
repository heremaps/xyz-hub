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

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.task.StatusHandler;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.util.service.Core;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import java.sql.SQLException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Job-Queue for IMPORT-Jobs (S3 -> RDS)
 */
public class ImportQueue extends JobQueue {
    private static final Logger logger = LogManager.getLogger();
    public static volatile long NODE_EXECUTED_IMPORT_MEMORY;
    protected static ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE, Core.newThreadFactory("import-queue"));

    protected void process() throws InterruptedException, CannotDecodeException {

        getQueue().stream().filter(job -> job instanceof Import).forEach( job -> {
            ((Future<Job>) job.isProcessingPossible())
                    .compose(j -> loadCurrentConfig(job))
                    .compose(currentJob -> {
                        /**
                         * Job-Life-Cycle:
                         * waiting -> (validating) -> validated ->  queued -> (preparing) -> prepared -> (executing) -> executed -> (finalizing) -> finalized
                         * all stages can end up in failed
                         **/

                        switch (currentJob.getStatus()){
                            case aborted:
                                //Abort has happened on other node
                                removeJob(job);
                                StatusHandler.getInstance().abortJob(job);
                                break;
                            case waiting:
                                updateJobStatus(currentJob,Job.Status.validating)
                                        .compose(j -> {
                                            Import validatedJob = validateJob(j);
                                            //Set status of validation
                                            return updateJobStatus(validatedJob);
                                        });
                                break;
                            case validated:
                                //Reflect that the Job is loaded into job-queue
                                updateJobStatus(currentJob,Job.Status.queued);
                                break;
                            case queued:
                                updateJobStatus(currentJob, Job.Status.preparing)
                                        .onSuccess(j ->
                                                 addReadOnlyLockToSpace(j)
                                                        .onSuccess(f2 -> prepareJob(j))
                                                        .onFailure(f -> setJobFailed(job, Import.ERROR_DESCRIPTION_READONLY_MODE_FAILED, Job.ERROR_TYPE_PREPARATION_FAILED))
                                        );
                                break;
                            case prepared:
                                updateJobStatus(currentJob,Job.Status.executing)
                                        .onSuccess(j -> j.execute());
                                break;
                            case executed:
                                updateJobStatus(currentJob,Job.Status.finalizing)
                                        .onSuccess(j -> j.finalizeJob());
                                break;
                        }

                        return Future.succeededFuture();
                    })
                    .onFailure(e -> logError(e, job.getId()));
        });
    }

    @Override
    protected Import validateJob(Job job) {
        return Import.validateImportObjects((Import) job);
    }

    @Override
    protected void prepareJob(Job j) {
        JDBCImporter.getInstance().prepareImport((Import) j)
                .compose(
                        f2 ->  updateJobStatus(j ,Job.Status.prepared))
                .onFailure(f -> {
                    logger.warn("job[{}] preparation has failed!", j.getId(), f);

                    if (f instanceof SQLException sqlException && sqlException.getSQLState() != null) {
                        if (sqlException.getSQLState().equalsIgnoreCase("42P01")) {
                            logger.info("job[{}] TargetTable '{}' does not exist!", j.getId(), j.getTargetTable());
                            setJobFailed(j, Import.ERROR_DESCRIPTION_TARGET_TABLE_DOES_NOT_EXISTS, Job.ERROR_TYPE_PREPARATION_FAILED);
                        }
                    }else if (f.getMessage() != null && f.getMessage().equalsIgnoreCase("SequenceNot0"))
                        setJobFailed(j, Import.ERROR_DESCRIPTION_SEQUENCE_NOT_0, Job.ERROR_TYPE_PREPARATION_FAILED);
                    else
                        setJobFailed(j, Import.ERROR_DESCRIPTION_UNEXPECTED, Job.ERROR_TYPE_PREPARATION_FAILED);
                });
    }

    /**
     * Begins executing the ImportQueue processing - periodically and asynchronously.
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
                catch (Exception e) {
                    logger.error("Exception in queue:", e);
                }
            });
        }catch (Exception e) {
            logger.error("Error when executing ImportQueue! ", e);
        }
    }
}
