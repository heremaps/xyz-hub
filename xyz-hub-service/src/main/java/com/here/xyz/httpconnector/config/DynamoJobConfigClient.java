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

import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.waiting;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.util.jobs.CombinedJob;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.Status;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * A client for writing and editing JOBs on a DynamoDb
 */
public class DynamoJobConfigClient extends JobConfigClient {

    private static final Logger logger = LogManager.getLogger();
    private final Table jobs;
    private final DynamoClient dynamoClient;
    private static final String IMPORT_OBJECTS = "importObjects";
    private static final String EXPORT_OBJECTS = "exportObjects";
    private static final String SUPER_EXPORT_OBJECTS = "superExportObjects";

    public DynamoJobConfigClient(String tableArn) {
        dynamoClient = new DynamoClient(tableArn, null);
        logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
        jobs = dynamoClient.db.getTable(dynamoClient.tableName);
    }

    @Override
    public Future<Void> init() {
        if (dynamoClient.isLocal()) {
            logger.info("DynamoDB running locally, initializing tables.");

            try {
                List<IndexDefinition> indexes = List.of(new IndexDefinition("type"), new IndexDefinition("status"));
                dynamoClient.createTable(jobs.getTableName(), "id:S,type:S,status:S", "id", indexes, "exp");
            }
            catch (Exception e) {
                logger.error("Failure during creating tables on DynamoSpaceConfigClient init", e);
                return Future.failedFuture(e);
            }
        }
        return Future.succeededFuture();
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
                    logger.info(marker, "job[{}] not found!", jobId);
                    p.complete();
                }
                else {
                    convertItemToJob(jobItem)
                        .onSuccess(job -> p.complete(job))
                        .onFailure(t -> p.fail(t));
                }
            }
            catch (Exception e) {
                p.fail(e);
            }
        });
    }

    @Override
    protected Future<List<Job>> getJobs(Marker marker, String type, Status status, String targetSpaceId) {
        return DynamoClient.dynamoWorkers.executeBlocking(p -> {
            try {
                List<ScanFilter> filterList = new ArrayList<>();

                if(type != null)
                    filterList.add(new ScanFilter("type").eq(type.toString()));
                if(status != null)
                    filterList.add(new ScanFilter("status").eq(status.toString()));
                if(targetSpaceId != null)
                    filterList.add(new ScanFilter("targetSpaceId").eq(targetSpaceId));

                List<Future<Job>> jobFutures = new ArrayList<>();
                jobs.scan(filterList.toArray(new ScanFilter[0])).pages().forEach(page -> page.forEach(item -> {
                    try{
                        jobFutures.add(convertItemToJob(item));
                    }catch (DecodeException e){
                        logger.warn("Cant decode Job-Item - skip!", e);
                    }
                }));

                Future.all(jobFutures)
                    .onSuccess(cf -> p.complete(cf.list()))
                    .onFailure(t -> p.fail(t));
            } catch (Exception e) {
                p.fail(e);
            }
        });
    }

    @Override
    protected Future<List<Job>> getJobs(Marker marker, Status status, String key, DatasetDirection direction) {
        return DynamoClient.dynamoWorkers.executeBlocking(p -> {
            try {

                List<String> filterExpression = new ArrayList<>();
                Map<String, String> nameMap = new HashMap<>();
                Map<String, Object> valueMap = new HashMap<>();

                if (status != null) {
                    nameMap.put("#status", "status");
                    valueMap.put(":status", status);
                    filterExpression.add("#status = :status");
                }

                if(key != null) {
                    nameMap.put("#sourceKey", "_sourceKey");
                    nameMap.put("#targetKey", "_targetKey");
                    valueMap.put(":key", key);
                    if(direction == DatasetDirection.SOURCE)
                        filterExpression.add("#sourceKey = :key");
                    else if(direction == DatasetDirection.TARGET)
                        filterExpression.add("#targetKey = :key");
                    else
                        filterExpression.add("(#sourceKey = :key OR #targetKey = :key)");
                }

                ScanSpec scanSpec = new ScanSpec()
                        .withFilterExpression(String.join(" AND ", filterExpression))
                        .withNameMap(nameMap)
                        .withValueMap(valueMap);

                List<Future<Job>> jobFutures = new ArrayList<>();
                jobs.scan(scanSpec).pages().forEach(page -> page.forEach(item -> {
                    try{
                        jobFutures.add(convertItemToJob(item));
                    }catch (DecodeException e){
                        logger.warn("Cant decode Job-Item - skip!", e);
                    }
                }));

                Future.all(jobFutures)
                    .onSuccess(cf -> p.complete(cf.list()))
                    .onFailure(t -> p.fail(t));
            } catch (Exception e) {
                p.fail(e);
            }
        });
    }

    protected Future<String> findRunningJobOnSpace(Marker marker, String targetSpaceId, String type) {
        return DynamoClient.dynamoWorkers.executeBlocking(p -> {
            try {
                Map<String, Object> expressionAttributeValues = ImmutableMap.of(
                    ":type", type.toString(),
                    ":spaceId", targetSpaceId,
                    ":waiting", waiting.toString(),
                    ":failed", failed.toString(),
                    ":finalized", finalized.toString()
                );
                List<String> conjunctions = ImmutableList.of(
                    "#type = :type",
                    "targetSpaceId = :spaceId",
                    "#status <> :waiting",
                    "#status <> :failed",
                    "#status <> :finalized"
                );
                ScanSpec scanSpec = new ScanSpec()
                    .withFilterExpression(String.join(" AND ", conjunctions))
                    .withNameMap(ImmutableMap.of("#type", "type", "#status", "status"))
                    .withValueMap(expressionAttributeValues);

                for (Page<Item, ScanOutcome> page : jobs.scan(scanSpec).pages())
                    for (Item item : page) {
                        p.complete(item.getString("id"));
                        return;
                    }

                p.complete(null);
            }
            catch (Exception e) {
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
            jobs.deleteItem(deleteItemSpec);
            p.complete(job);
        });
    }

    @Override
    protected Future<Job> storeJob(Marker marker, Job job, boolean isUpdate) {
        return DynamoClient.dynamoWorkers.executeBlocking(p -> storeJobSync(job, p));
    }

    private void storeJobSync(Job job, Promise<Job> p) {
        Item item = convertJobToItem(job);
        jobs.putItem(item);
        p.complete(job);
    }

    private static Item convertJobToItem(Job job) {
        JsonObject json = JsonObject.mapFrom(job);
        if(job.getSource() != null)
            json.put("_sourceKey", job.getSource().getKey());
        if(job.getTarget() != null)
            json.put("_targetKey", job.getTarget().getKey());
        //Set the item's "exp" field on which the table's automated expiry is watching
        json.put("exp", job.getKeepUntil() < 0 ? -1 : job.getKeepUntil() / 1000);
        //TODO: Remove the following hacks from the persistence layer!
        if (job instanceof Import)
            return convertImportJobToItem(json);
        else if (job instanceof CombinedJob combinedJob && combinedJob.getChildren().size() > 0)
            sanitizeChildren(json);
        else
            sanitizeExportJob(json);
        return Item.fromJSON(json.toString());
    }

    private static void sanitizeExportJob(JsonObject jobJson) {
        if (jobJson.containsKey(EXPORT_OBJECTS))
            jobJson.remove(EXPORT_OBJECTS);
        if (jobJson.containsKey(SUPER_EXPORT_OBJECTS))
            jobJson.remove(SUPER_EXPORT_OBJECTS);
    }

    private static void sanitizeChildren(JsonObject combinedJob) {
        JsonArray children = combinedJob.getJsonArray("children");
        for (int i = 0; i < children.size(); i++) {
            JsonObject childJob = children.getJsonObject(i);
            sanitizeExportJob(childJob);
        }
    }

    private Future<Job> convertItemToJob(Item item){
        if(item.isPresent(IMPORT_OBJECTS))
            return convertItemToJob(item, IMPORT_OBJECTS);
        return convertItemToJob(item, EXPORT_OBJECTS);
    }

    private static Item convertImportJobToItem(JsonObject jobJson) {
        if (jobJson.containsKey(IMPORT_OBJECTS)) {
            String str = jobJson.getJsonObject(IMPORT_OBJECTS).encode();
            jobJson.remove(IMPORT_OBJECTS);
            Item item = Item.fromJSON(jobJson.toString());
            return item.withBinary(IMPORT_OBJECTS, compressString(str));
        }
        return Item.fromJSON(jobJson.toString());
    }

    private Future<Job> convertItemToJob(Item item, String attrName) {
        JsonObject ioObjects = null;
        if(item.isPresent(attrName)) {
            try{
                ioObjects = new JsonObject(Objects.requireNonNull(uncompressString(item.getBinary(attrName))));
            }
            catch(Exception e){
                ioObjects = new JsonObject(item.getJSON(attrName));
            }
        }

        JsonObject json = new JsonObject(item.removeAttribute(attrName).toJSON())
            .put(attrName, ioObjects);

        Future<Void> resolvedFuture = Future.succeededFuture();
        if (json.containsKey("children"))
            resolvedFuture = resolveChildren(json);
        try {
            final Job job = XyzSerializable.deserialize(json.toString(), Job.class);
            Long exp = json.getLong("exp");
            job.setKeepUntil(exp == null || exp < 0 ? -1 : exp * 1_000);
            return resolvedFuture.map(v -> job);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Future<Void> resolveChildren(JsonObject combinedJob) {
        JsonArray children = combinedJob.getJsonArray("children");
        if (!children.isEmpty() && children.getValue(0) instanceof String) {
            List<Future<Job>> jobFutures = children.stream().map(childId -> getJob(null, (String) childId))
                .collect(Collectors.toList());
            return Future.all(jobFutures).compose(cf -> {
                combinedJob.put("children", new JsonArray(cf.list()));
                return Future.succeededFuture();
            });
        }
        return Future.succeededFuture();
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