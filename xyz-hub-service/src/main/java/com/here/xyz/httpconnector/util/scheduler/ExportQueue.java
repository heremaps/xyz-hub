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
import com.here.xyz.hub.Core;
import com.mchange.v3.decode.CannotDecodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
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
        HashSet<Job> queue = getQueue();

        if (!queue.isEmpty()) {
            int i = 0;
            for (Job job : getQueue()) {
                if (!(job instanceof Export))
                    return;

                /** Check Capacity */
                isProcessingOnRDSPossible(i++, job)
                        .onSuccess(ff -> {
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
                                    updateJobStatus(exportJob, Job.Status.validating)
                                            .onSuccess(f -> validateJob(exportJob));
                                    break;
                                case validated:
                                    updateJobStatus(exportJob, Job.Status.queued);
                                    break;
                                case queued:
                                    updateJobStatus(exportJob, Job.Status.preparing)
                                            .onSuccess(f -> prepareJob(exportJob));
                                    break;
                                case prepared:
                                    updateJobStatus(exportJob, Job.Status.executing)
                                            .onSuccess(f -> executeJob(exportJob));
                                    break;
                                case executed:
                                    updateJobStatus(exportJob, Job.Status.finalizing)
                                            .onSuccess(f -> finalizeJob(exportJob));
                                    break;
                                default: {
                                    logger.info("JOB[{}] is currently '{}' - current Queue-size: {}", exportJob.getId(), exportJob.getStatus(), queueSize());
                                }
                            }
                        })
                        .onFailure(e -> logger.info(e.getMessage()));
            }
        }
    }

    @Override
    protected void validateJob(Job j){
        //** Currently not needed */
        j.setStatus(Job.Status.validated);
        updateJobStatus(j, null);
    }

    @Override
    protected void prepareJob(Job j){
        //** Currently not needed */
        j.setStatus(Job.Status.prepared);
        updateJobStatus(j, null);
    }

    @Override
    protected void executeJob(Job j){
        j.setExecutedAt(Core.currentTimeMillis() / 1000L);
        String defaultSchema = JDBCImporter.getDefaultSchema(j.getTargetConnector());

        String s3Path = CService.jobS3Client.EXPORT_DOWNLOAD_FOLDER+"/"+j.getId();

        JDBCExporter.executeExport(((Export) j), defaultSchema, CService.configuration.JOBS_S3_BUCKET, s3Path,
                        CService.configuration.JOBS_S3_BUCKET_REGION)
                .onSuccess(statistic -> {
//                            NODE_EXECUTED_IMPORT_MEMORY -= curFileSize;
                            logger.info("JOB[{}] Export of '{}' succeeded!", j.getId(), j.getTargetSpaceId());
                            ((Export)j).setStatistic(statistic);
                            updateJobStatus(j, Job.Status.executed);
                        }
                )
                .onFailure(f -> {
                            logger.warn("JOB[{}] Export of '{}' failed ", j.getId(), j.getTargetSpaceId(), f);
                            updateJobStatus(j, Job.Status.failed);
                        }
                );
    }

    @Override
    protected void finalizeJob(Job j){
        j.setFinalizedAt(Core.currentTimeMillis() / 1000L);

        //Add file statistics and downloadLinks
        Map<String, ExportObject> exportObjects = CService.jobS3Client.scanExportPath(j, true);
        ((Export)j).setExportObjects(exportObjects);

        //Write MetaFile to S3
        CService.jobS3Client.writeMetaFile((Export) j);

        updateJobStatus(j, Job.Status.finalized);
    }

    protected void abortJob(Job j){
        j.setErrorDescription("ABORTED");
        failJob(j);
    }

    @Override
    protected void failJob(Job j){
        //TODO: set TTL
        j.setStatus(Job.Status.failed);

        CService.jobConfigClient.update(null , j)
                .onFailure(t -> logger.warn("JOB[{}] update failed!", j.getId()));
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
            this.executionHandle = this.executorService.scheduleWithFixedDelay(this, 0, CService.configuration.JOB_CHECK_QUEUE_INTERVAL_SECONDS, TimeUnit.SECONDS);
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
