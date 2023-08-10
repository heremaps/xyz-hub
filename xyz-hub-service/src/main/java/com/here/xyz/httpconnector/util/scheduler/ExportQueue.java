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
import io.vertx.core.Future;
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

        for (Job job : getQueue()){
            if (!(job instanceof Export))
                return;

            /** Check Capacity */
            isProcessingPossible(job)
                    .compose(j -> loadCurrentConfig(job))
                    .compose( currentJob -> {
                        /**
                         * Job-Life-Cycle:
                         * waiting -> (executing) -> executed -> executing_trigger -> trigger_executed -> collecting_trigger_status -> finalized
                         * all stages can end up in failed
                         **/

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
                                        .onSuccess(f -> executeJob(currentJob));
                                break;
                            case executed:
                                updateJobStatus(currentJob, Job.Status.executing_trigger)
                                        .onSuccess(f -> {
                                            if(((Export)currentJob).getExportTarget().getType().equals(Export.ExportTarget.Type.VML)) {
                                                /** Only here we need a trigger */
                                                postTrigger(currentJob);
                                            }else
                                                finalizeJob(currentJob);
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
    protected Export validateJob(Job j){
        //** Currently not needed */
        return null;
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

        if(((Export) j).readParamPersistExport() ) {
            Export existingJob = CService.jobS3Client.readMetaFileFromJob((Export) j);
            if(existingJob != null) {
                if(existingJob.getExportObjects() == null || existingJob.getExportObjects().isEmpty()) {
                    String message = String.format("Another job already started for %s and targetLevel %s with status %s",
                            existingJob.getTargetSpaceId(), existingJob.getTargetLevel(), existingJob.getStatus());
                    setJobFailed(j, message, Job.ERROR_TYPE_EXECUTION_FAILED);
                } else {
                    addDownloadLinksAndWriteMetaFile(existingJob);
                    ((Export) j).setExportObjects(existingJob.getExportObjects());
                    updateJobStatus(j, Job.Status.executed);
                }
                return;
            } else {
                addDownloadLinksAndWriteMetaFile(j);
            }
        }

        JDBCExporter.executeExport(((Export) j), defaultSchema, CService.configuration.JOBS_S3_BUCKET, s3Path,
                        CService.configuration.JOBS_REGION)
                .onSuccess(statistic -> {
                            /** Everything is processed */
                            logger.info("job[{}] Export of '{}' completely succeeded!", j.getId(), j.getTargetSpaceId());
                            ((Export)j).addStatistic(statistic);
                            addDownloadLinksAndWriteMetaFile(j);
                            updateJobStatus(j, Job.Status.executed);
                        }
                )
                .onFailure(e -> {
                        logger.warn("job[{}] Export of '{}' failed ", j.getId(), j.getTargetSpaceId(), e);

                        if(e.getMessage() != null && e.getMessage().equalsIgnoreCase("Fail to read any response from the server, the underlying connection might get lost unexpectedly."))
                            setJobAborted(j);
                        else {
                            setJobFailed(j, null, Job.ERROR_TYPE_EXECUTION_FAILED);
                        }}
                );
    }

    @Override
    protected void finalizeJob(Job j){
        j.setFinalizedAt(Core.currentTimeMillis() / 1000L);
        updateJobStatus(j, Job.Status.finalized);
    }

    @Override
    protected boolean needRdsCheck(Job job) {
        /** In next stage we need database resources */
        if(job.getStatus().equals(Job.Status.queued))
            return true;
        return false;
    }

    protected void addDownloadLinksAndWriteMetaFile(Job j){
        /** Add file statistics and downloadLinks */
        Map<String, ExportObject> exportObjects = CService.jobS3Client.scanExportPath((Export)j, false, true);
        ((Export) j).setExportObjects(exportObjects);

        if(((Export)j).readParamSuperExportPath() != null) {
            /** Add exportObjects including fresh download links for persistent base exports */
            Map<String, ExportObject> superExportObjects = CService.jobS3Client.scanExportPath((Export) j, true, true);
            ((Export) j).setSuperExportObjects(superExportObjects);
        }

        /** Write MetaFile to S3 */
        CService.jobS3Client.writeMetaFile((Export) j);
    }

    protected Future<String> postTrigger(Job j){
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
        /** executeHttpTrigger */
        if(((Export)j).getExportTarget().getType().equals(Export.ExportTarget.Type.VML)) {
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
                                finalizeJob(j);
                                return;
                            case "cancelled":
                            case "failed":
                                logger.warn("job[{}] Trigger '{}' failed with state '{}'", j.getId(), ((Export) j).getTriggerId(), status);
                                setJobFailed(j, Export.ERROR_TYPE_HTTP_TRIGGER_FAILED, Job.ERROR_TYPE_FINALIZATION_FAILED);
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
            logger.error("{} {}", this.getClass().getSimpleName(), e);
        }
    }
}
