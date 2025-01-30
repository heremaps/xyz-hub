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
package com.here.xyz.httpconnector.rest;

import static com.here.xyz.XyzSerializable.Mappers.DEFAULT_MAPPER;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.task.JobHandler;
import com.here.xyz.httpconnector.task.StatusHandler;
import com.here.xyz.httpconnector.util.scheduler.ImportQueue;
import com.here.xyz.httpconnector.util.scheduler.JobQueue;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.util.service.logging.LogUtil;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobStatusApi {
    private static final Logger logger = LogManager.getLogger();
    private final String JOB_QUEUE_STATUS_ENDPOINT = "/psql/system/status";
    private final String JOB_QUEUE_QUEUE_ENDPOINT = "/psql/system/queue";
    private final ObjectNode system;

    public JobStatusApi(Router router) {
        this.system = DEFAULT_MAPPER.get().createObjectNode();
        ObjectNode poolSizes = DEFAULT_MAPPER.get().createObjectNode();

        poolSizes.put("DB_POOL_SIZE_PER_CLIENT", CService.configuration.JOB_DB_POOL_SIZE_PER_CLIENT);
        poolSizes.put("DB_POOL_SIZE_PER_STATUS_CLIENT", CService.configuration.JOB_DB_POOL_SIZE_PER_STATUS_CLIENT);
        poolSizes.put("DB_POOL_SIZE_PER_MAINTENANCE_CLIENT", CService.configuration.JOB_DB_POOL_SIZE_PER_MAINTENANCE_CLIENT);

        /* Add static configurations */
        this.system.put("MAX_RDS_INFLIGHT_IMPORT_BYTES", CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES);
        this.system.put("MAX_RDS_UTILIZATION_", CService.configuration.JOB_MAX_RDS_MAX_ACU_UTILIZATION);
        this.system.put("MAX_RUNNING_EXPORT_QUERIES", CService.configuration.JOB_MAX_RDS_MAX_RUNNING_EXPORT_QUERIES);
        this.system.put("MAX_RUNNING_IMPORT_QUERIES", CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES);
        this.system.set("POOL_SIZES", poolSizes);

        ArrayNode supportedConnectors = DEFAULT_MAPPER.get().valueToTree(CService.supportedConnectors);
        this.system.set("SUPPORTED_CONNECTORS", supportedConnectors);

        this.system.put("JOB_QUEUE_INTERVAL", CService.configuration.JOB_CHECK_QUEUE_INTERVAL_MILLISECONDS);
        this.system.put("HOST_ID", CService.HOST_ID);
        this.system.put("NODE_EXECUTED_IMPORT_MEMORY", ImportQueue.NODE_EXECUTED_IMPORT_MEMORY);

        router.route(HttpMethod.GET, JOB_QUEUE_STATUS_ENDPOINT)
                .handler(this::getSystemStatus);
        router.route(HttpMethod.DELETE, JOB_QUEUE_QUEUE_ENDPOINT)
                .handler(this::removeJobFromQueue);
        router.route(HttpMethod.POST, JOB_QUEUE_QUEUE_ENDPOINT)
                .handler(this::addJobToQueue);
    }

    private void addJobToQueue(final RoutingContext context){
        final String jobId = HApiParam.HQuery.getString(context, HApiParam.Path.JOB_ID, null);

        if(jobId == null){
            context.response().setStatusCode(BAD_REQUEST.code()).end();
            return;
        }

        HttpServerResponse httpResponse = context.response().setStatusCode(OK.code());
        httpResponse.putHeader(CONTENT_TYPE, APPLICATION_JSON);
        ObjectNode resp = DEFAULT_MAPPER.get().createObjectNode();

        JobHandler.loadJob(jobId, LogUtil.getMarker(context))
                .onFailure(e -> resp.put("STATUS", "does_not_exist"))
                .onSuccess(job -> {
                    if(JobQueue.hasJob(job) != null) {
                        resp.put("STATUS", "already_present");
                        httpResponse.end(resp.toString());
                    }
                    else {
                        try{
                            JobQueue.addJob(job);
                            logger.info("JOB[{}] got added manually to queue! Host-id: {}", job.getId(), CService.HOST_ID);
                            resp.put("STATUS", "job_added");
                        }
                        catch (Exception e){
                            logger.info("JOB[{}] state has no last state {}", job.getId(), CService.HOST_ID);
                            resp.put("STATUS", "not_added");
                        }
                        httpResponse.end(resp.toString());
                    }
                });
    }

    private void removeJobFromQueue(final RoutingContext context){
        String jobId = HApiParam.HQuery.getString(context, HApiParam.Path.JOB_ID, null);

        if(jobId == null){
            context.response().setStatusCode(BAD_REQUEST.code()).end();
            return;
        }

        HttpServerResponse httpResponse = context.response().setStatusCode(OK.code());
        httpResponse.putHeader(CONTENT_TYPE, APPLICATION_JSON);

        ObjectNode resp = DEFAULT_MAPPER.get().createObjectNode();

        JobHandler.loadJob(jobId, LogUtil.getMarker(context))
                .onFailure(e -> {
                    resp.put("STATUS", "does_not_exist");
                })
                .onSuccess(job -> {
                    JobQueue.removeJob(job);
                    logger.info("JOB[{}] got manually deleted from queue! Host-id: {}", job.getId(), CService.HOST_ID);
                    resp.put("STATUS", "job_removed");
                }).onComplete(job -> {
                    httpResponse.end(resp.toString());
                });
    }

    private void getSystemStatus(final RoutingContext context){

        HttpServerResponse httpResponse = context.response().setStatusCode(OK.code());
        httpResponse.putHeader(CONTENT_TYPE, APPLICATION_JSON);

        ObjectNode status = DEFAULT_MAPPER.get().createObjectNode();
        status.set("SYSTEM", this.system);
        ArrayNode runningJobs = DEFAULT_MAPPER.get().valueToTree(JobQueue.getQueue().stream().map(j ->{
            ObjectNode info = DEFAULT_MAPPER.get().createObjectNode();
            info.put("type", j.getClass().getSimpleName());
            info.put("id", j.getId());
            info.put("status", j.getStatus().toString());
            return info;
        }).toArray());
        status.set("RUNNING_JOBS", runningJobs);

        List<Future> statusFutures = new ArrayList<>();
        CService.supportedConnectors.forEach(connectorId -> statusFutures.add(StatusHandler.getInstance().getRDSStatus(connectorId)));

        CompositeFuture.join(statusFutures)
                .onComplete(f -> {
                    ObjectNode rdsStatusList = DEFAULT_MAPPER.get().createObjectNode();

                    statusFutures.forEach( f1 -> {
                        if(f1.succeeded()){
                            RDSStatus rdsStatus = (RDSStatus)f1.result();
                            rdsStatusList.set(rdsStatus.getConnectorId(), DEFAULT_MAPPER.get().valueToTree(rdsStatus));
                        }
                    });
                    status.set("RDS", rdsStatusList);

                    httpResponse.end(status.toString());

                })
                .onFailure(f -> httpResponse.end(status.toString()));
    }
}
