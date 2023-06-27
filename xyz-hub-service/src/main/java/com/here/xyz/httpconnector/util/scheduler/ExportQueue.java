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
import com.here.xyz.httpconnector.config.JDBCExporter;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.ExportObject;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.rest.HttpException;
import com.mchange.v3.decode.CannotDecodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Job-Queue for Export-Jobs (S3 -> RDS)
 */
public class ExportQueue extends JobQueue{
    private static final Logger logger = LogManager.getLogger();
    protected static ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE, Core.newThreadFactory("export-queue"));

    protected void process() throws InterruptedException, CannotDecodeException {

        for (int i = 0; i <  getQueue().size(); i++) {
            Job job = getQueue().get(i);

            if (!(job instanceof Export))
                return;

            /** Check Capacity */
            isProcessingOnRDSPossible(i, job)
                    .onSuccess(canProcess -> {

                        /** Execution is currently not possible */
                        if (!canProcess){
                            return;
                        }

                        /** Check if JDBC Client is available */
                        if (!isTargetJDBCClientLoaded(job))
                            return;

                        /** Run first Job Queue (FIFO) */
                        Export exportJob = (Export) job;

                        /**
                         * Job-Life-Cycle:
                         * waiting -> (validating) -> validated ->  queued -> (preparing) -> prepared -> (executing) -> executed -> (finalizing) -> finalized
                         * all stages can end up in failed
                         **/

                        switch (exportJob.getStatus()) {
                            case finalized:
                                logger.info("JOB[{}] is finalized!", exportJob.getId());
                                /** Remove Job from Queue */
                                removeJob(exportJob);
                                break;
                            case failed:
                                logger.info("JOB[{}] has failed!", exportJob.getId());
                                /** Remove Job from Queue - in some cases the user is able to retry */
                                removeJob(exportJob);
                                break;
                            case waiting:
                            case queued:
                                updateJobStatus(exportJob, Job.Status.executing)
                                        .onSuccess(f -> executeJob(exportJob));
                                break;
                            case executed:
                                updateJobStatus(exportJob, Job.Status.executing_trigger)
                                        .onSuccess(f -> postTrigger(exportJob));
                                break;
                            case trigger_executed:
                                updateJobStatus(exportJob, Job.Status.collectiong_trigger_status)
                                        .onSuccess(f -> collectTriggerStatus(exportJob));
                                break;
                            default: {
                                logger.info("JOB[{}] is currently '{}' - current Queue-size: {}", exportJob.getId(), exportJob.getStatus(), queueSize());
                            }
                        }
                    })
                    .onFailure(e -> logger.info(e.getMessage()));
            }
    }

    @Override
    protected void validateJob(Job j){
        //** Currently not needed */
    }

    @Override
    protected void prepareJob(Job j){
        //** Currently not needed */
    }

    @Override
    protected void executeJob(Job j){
        j.setExecutedAt(Core.currentTimeMillis() / 1000L);
        String defaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());

        String s3Path = CService.jobS3Client.getS3Path(j);

        boolean persistExport = j.getParams().containsKey("persistExport") ? (boolean) j.getParam("persistExport") : false;
        if(persistExport && ((Export) j).getExportTarget().getType() == Export.ExportTarget.Type.VML && ((Export) j).getFilters() == null ) {
            Export existingJob = CService.jobS3Client.readMetaFile((Export) j);
            if(existingJob != null) {
                if(existingJob.getExportObjects() == null || existingJob.getExportObjects().isEmpty()) {
                    String message = String.format("Another job already started for %s and targetLevel %s with status %s",
                            existingJob.getTargetSpaceId(), existingJob.getTargetLevel(), existingJob.getStatus());
                    failJob(j, message, Job.ERROR_TYPE_EXECUTION_FAILED);
                } else {
                     addFileData(existingJob);
                     ((Export) j).setExportObjects(existingJob.getExportObjects());
                     updateJobStatus(j, Job.Status.executed);
                }
                return;
            } else {
                addFileData(j);
            }
        }

        JDBCExporter.executeExport(((Export) j), defaultSchema, CService.configuration.JOBS_S3_BUCKET, s3Path,
                        CService.configuration.JOBS_REGION)
                .onSuccess(statistic -> {
                            /** Everything is processed */
                            logger.info("JOB[{}] Export of '{}' completely succeeded!", j.getId(), j.getTargetSpaceId());
                            ((Export)j).addStatistic(statistic);
                            updateJobStatus(j, Job.Status.executed);
                        }
                )
                .onFailure(f -> {
                            logger.warn("JOB[{}] Export of '{}' failed ", j.getId(), j.getTargetSpaceId(), f);
                            failJob(j, null , Job.ERROR_TYPE_EXECUTION_FAILED);
                        }
                );
    }

    @Override
    protected void finalizeJob(Job j){
        j.setFinalizedAt(Core.currentTimeMillis() / 1000L);
        updateJobStatus(j, Job.Status.finalized);
    }

    protected void addFileData(Job j){
        /** Add file statistics and downloadLinks */
        Map<String, ExportObject> exportObjects = CService.jobS3Client.scanExportPath(j, true);
        ((Export) j).setExportObjects(exportObjects);

        /** Write MetaFile to S3 */
        CService.jobS3Client.writeMetaFile((Export) j);
    }

    protected void postTrigger(Job j){
        addFileData(j);

        /** executeHttpTrigger - only on VML*/
        if(((Export)j).getExportTarget().getType().equals(Export.ExportTarget.Type.VML)) {
            HubWebClient.executeHTTPTrigger((Export) j)
                    .onFailure(e -> {
                                if(e instanceof HttpException){
                                    failJob(j, Export.ERROR_TYPE_TARGET_ID_INVALID, Job.ERROR_TYPE_FINALIZATION_FAILED);
                                }else
                                    failJob(j, Export.ERROR_TYPE_HTTP_TRIGGER_FAILED, Job.ERROR_TYPE_FINALIZATION_FAILED);
                            }
                    )
                    .onSuccess(triggerId -> {
                        //** Add import ID */
                        ((Export) j).setTriggerId(triggerId);
                        updateJobStatus(j, Job.Status.trigger_executed);
                    });
        }else {
            /** skip collecting Trigger */
            finalizeJob(j);
        }
    }

    protected void collectTriggerStatus(Job j){
        /** executeHttpTrigger */
        if(((Export)j).getExportTarget().getType().equals(Export.ExportTarget.Type.VML)) {
            HubWebClient.executeHTTPTriggerStatus((Export) j)
                    .onFailure(e -> {
                            if(e instanceof HttpException){
                                failJob(j, Export.ERROR_TYPE_TARGET_ID_INVALID, Job.ERROR_TYPE_FINALIZATION_FAILED);
                            }else
                                failJob(j, Export.ERROR_TYPE_HTTP_TRIGGER_STATUS_FAILED, Job.ERROR_TYPE_FINALIZATION_FAILED);
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
                                logger.info("JOB[{}] execution of '{}' succeeded ", j.getId(), ((Export) j).getTriggerId());
                                finalizeJob(j);
                                return;
                            case "cancelled":
                            case "failed":
                                logger.warn("JOB[{}] Trigger '{}' failed with state '{}'", j.getId(), ((Export) j).getTriggerId(), status);
                                failJob(j, Export.ERROR_TYPE_HTTP_TRIGGER_FAILED, Job.ERROR_TYPE_FINALIZATION_FAILED);
                        }
                    });
        }else {
            /** skip collecting Trigger */
            finalizeJob(j);
        }
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
            logger.error("{}: Error when executing Job", this.getClass().getSimpleName(), e);
        }
    }
}
