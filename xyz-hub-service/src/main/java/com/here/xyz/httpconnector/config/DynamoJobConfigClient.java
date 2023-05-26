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
package com.here.xyz.httpconnector.config;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.here.xyz.Payload;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.Status;
import com.here.xyz.httpconnector.util.jobs.Job.Type;
import com.here.xyz.hub.config.dynamo.DynamoClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A client for writing and editing JOBs on a DynamoDb
 */
public class DynamoJobConfigClient extends JobConfigClient {

    private static final Logger logger = LogManager.getLogger();

    private final Table jobs;
    private final DynamoClient dynamoClient;
    private Long expiration;

    private static final String IO_ATTR_NAME = "importObjects";

    public DynamoJobConfigClient(String tableArn) {
        dynamoClient = new DynamoClient(tableArn, null);
        logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
        jobs = dynamoClient.db.getTable(dynamoClient.tableName);
        if(CService.configuration != null && CService.configuration.JOB_DYNAMO_EXP_IN_DAYS != null)
            expiration = CService.configuration.JOB_DYNAMO_EXP_IN_DAYS;
    }

    @Override
    public void init(Handler<AsyncResult<Void>> onReady) {
        if (dynamoClient.isLocal()) {
            logger.info("DynamoDB running locally, initializing tables.");

            try {
                dynamoClient.createTable(jobs.getTableName(), "id:S,type:S,status:S", "id", "type,status", "exp");
            }
            catch (Exception e) {
                logger.error("Failure during creating tables on DynamoSpaceConfigClient init", e);
                onReady.handle(Future.failedFuture(e));
                return;
            }
        }
        onReady.handle(Future.succeededFuture());
    }

    @Override
    public Future<Job> getJob(Marker marker, String jobId) {
        if(jobId == null)
            return Future.succeededFuture(null);
        return DynamoClient.dynamoWorkers.executeBlocking(p -> {
            try {
                GetItemSpec spec = new GetItemSpec()
                        .withPrimaryKey("id", jobId)
                        .withConsistentRead(true);

                Item jobItem = jobs.getItem(spec);

                if (jobItem == null) {
                    logger.info(marker, "Getting job with ID: {} returned null", jobId);
                    p.complete();
                }
                else {
                    logger.info(marker, "Loaded Job width ID: {}", jobId);
                    p.complete(converItemToJob(jobItem));
                }
            }
            catch (Exception e) {
                p.fail(e);
            }
        });
    }

    @Override
    protected Future<List<Job>> getJobs(Marker marker, Type type, Status status, String targetSpaceId) {
        return DynamoClient.dynamoWorkers.executeBlocking(p -> {
            try {
                final List<Job> result = new ArrayList<>();
                List<ScanFilter> filterList = new ArrayList<>();

                if(type != null)
                    filterList.add(new ScanFilter("type").eq(type.toString()));
                if(status != null)
                    filterList.add(new ScanFilter("status").eq(status.toString()));
                if(targetSpaceId != null)
                    filterList.add(new ScanFilter("targetSpaceId").eq(targetSpaceId));

                jobs.scan(filterList.toArray(new ScanFilter[0])).pages().forEach(j -> j.forEach(i -> {
                    try{
                        final Job job = converItemToJob(i);
                        result.add(job);
                    }catch (DecodeException e){
                        logger.warn("Cant decode Job-Item - skip",e);
                    }
                }));
                p.complete(result);
            } catch (Exception e) {
                p.fail(e);
            }
        });
    }

    protected Future<String> findRunningJobOnSpace(Marker marker, String targetSpaceId, Type type) {
        return DynamoClient.dynamoWorkers.executeBlocking(p -> {
            try {
                List<ScanFilter> filterList = new ArrayList<>();

                filterList.add(new ScanFilter("type").eq(type.toString()));
                filterList.add(new ScanFilter("status").ne(Status.finalized.toString()));
                filterList.add(new ScanFilter("targetSpaceId").eq(targetSpaceId));

                jobs.scan(filterList.toArray(new ScanFilter[0])).pages().forEach(j -> j.forEach(i -> {
                    try{
                        final Job job = converItemToJob(i);

                        switch (job.getStatus()){
                            case waiting:
                            case failed:
                            case finalized:
                                break;
                            default: {
                                logger.info("{} is blocked!",targetSpaceId);
                                p.complete(job.getId());
                            }
                        }
                    }catch (DecodeException e){
                        logger.warn("Cant decode Job-Item - skip",e);
                    }
                }));

                p.complete(null);
            } catch (Exception e) {
                p.fail(e);
            }
        });
    }

    @Override
    protected Future<Job> deleteJob(Marker marker, Job job) {
        return DynamoClient.dynamoWorkers.executeBlocking(p -> {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey("id", job.getId())
                    .withReturnValues(ReturnValue.ALL_OLD);
            DeleteItemOutcome response = jobs.deleteItem(deleteItemSpec);

            if (response.getItem() != null)
                p.complete(converItemToJob(response.getItem()));
            else {
                p.complete();
            }
        });
    }

    @Override
    protected Future<Job> storeJob(Marker marker, Job job, boolean isUpdate) {
        if(!isUpdate && this.expiration != null){
            job.setExp(System.currentTimeMillis() / 1000L + expiration * 24 * 60 * 60);
        }
        return DynamoClient.dynamoWorkers.executeBlocking(p -> storeJobSync(job, p));
    }

    private void storeJobSync(Job job, Promise<Job> p){
        Item item = convertJobToItem(job);
        jobs.putItem(item);
        p.complete(job);
    }

    private static Item convertJobToItem(Job job) {
        JsonObject json = JsonObject.mapFrom(job);
        if( !json.containsKey(IO_ATTR_NAME) )
            return Item.fromJSON(json.toString());

        String str = json.getJsonObject(IO_ATTR_NAME).encode();
        json.remove(IO_ATTR_NAME);
        Item item = Item.fromJSON(json.toString());
        return item.withBinary(IO_ATTR_NAME, compressString(str));
    }

    private static Job converItemToJob(Item item){
        JsonObject importObjects = null;
        if(item.isPresent(IO_ATTR_NAME)) {
            try{
                importObjects = new JsonObject(Objects.requireNonNull(uncompressString(item.getBinary(IO_ATTR_NAME))));
            }
            catch(Exception e){
                importObjects = new JsonObject(item.getJSON(IO_ATTR_NAME));
            }
        }

        JsonObject json = new JsonObject(item.removeAttribute(IO_ATTR_NAME).toJSON())
                .put(IO_ATTR_NAME, importObjects);
        return Json.decodeValue(json.toString(), Job.class);
    }

    private static byte[] compressString(String input) {
        if( input == null )
            return null;
        return Payload.compress(input.getBytes());
    }

    private static String uncompressString(byte[] input){
        try {
            return new String(Payload.decompress(input), StandardCharsets.UTF_8);
        }
        catch(Exception e) {
            return null;
        }
    }
}