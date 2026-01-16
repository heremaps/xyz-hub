/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl.transport;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.JobClientInfo;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep.LambdaStepRequest.RequestType;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import com.here.xyz.util.web.XyzWebClient.WebClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExtractJsonPathValues extends TaskedSpaceBasedStep<
        ExtractJsonPathValues,
        ExtractJsonPathValues.ExtractTaskInput,
        ExtractJsonPathValues.ExtractTaskOutput> {

    private static final int DEFAULT_THREAD_COUNT = 8;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private Map<String, String> aliasToJsonPath;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private double overallEstimatedAcus = 0d;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private long minI = Long.MIN_VALUE;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private long maxI = Long.MIN_VALUE;

    public Map<String, String> getAliasToJsonPath() {
        return aliasToJsonPath;
    }

    public void setAliasToJsonPath(Map<String, String> aliasToJsonPath) {
        this.aliasToJsonPath = aliasToJsonPath;
    }

    public ExtractJsonPathValues withAliasToJsonPath(Map<String, String> aliasToJsonPath) {
        setAliasToJsonPath(aliasToJsonPath);
        return this;
    }

    public double getOverallEstimatedAcus() {
        return overallEstimatedAcus;
    }

    public void setOverallEstimatedAcus(double overallEstimatedAcus) {
        this.overallEstimatedAcus = overallEstimatedAcus;
    }

    public ExtractJsonPathValues withOverallEstimatedAcus(double overallEstimatedAcus) {
        setOverallEstimatedAcus(overallEstimatedAcus);
        return this;
    }

    @Override
    protected boolean queryRunsOnWriter() {
        return true;
    }

    @Override
    protected void initialSetup() {
        if (threadCount <= 0) {
            threadCount = DEFAULT_THREAD_COUNT;
        }
    }

    @Override
    protected List<ExtractTaskInput> createTaskItems() throws WebClientException {
        // Resolve effective paths
        Map<String, String> effective = loadAliasToJsonPath();

        if (effective.isEmpty()) {
            return List.of();
        }

        int usable = 0;
        for (Map.Entry<String, String> e : effective.entrySet()) {
            String aliasKey = e.getKey();
            String jsonPath = e.getValue();

            if (aliasKey == null || aliasKey.isBlank() || jsonPath == null || jsonPath.isBlank()) {
                continue;
            }

            int typeIdx = jsonPath.lastIndexOf("::");

            if (typeIdx > -1) {
                jsonPath = jsonPath.substring(0, typeIdx).trim();
            }

            if (!jsonPath.isBlank()) {
                usable++;
            }
        }

        if (usable == 0) {
            return List.of();
        }

        long min = loadMinI();
        long max = loadMaxI();

        if (max < min) {
            return List.of();
        }

        long totalRows = max - min + 1;
        int effectiveTaskCount = threadCount > 0 ? threadCount : DEFAULT_THREAD_COUNT;
        effectiveTaskCount = (int) Math.max(1, Math.min(effectiveTaskCount, totalRows));

        List<ExtractTaskInput> items = new ArrayList<>(effectiveTaskCount);

        for (int t = 0; t < effectiveTaskCount; t++) {
            items.add(new ExtractTaskInput(t));
        }

        return items;
    }

    private Map<String, String> loadAliasToJsonPath() throws WebClientException {
        if (aliasToJsonPath == null || aliasToJsonPath.isEmpty()) {
            this.aliasToJsonPath = Space.toExtractableSearchProperties(space());
        }

        return aliasToJsonPath == null ? Map.of() : aliasToJsonPath;
    }

    @Override
    protected SQLQuery buildTaskQuery(Integer taskId, ExtractTaskInput taskInput, String failureCallbackIgnored) throws WebClientException {
        // Build the actual task query
        SQLQuery workQuery = buildWorkQuery(taskInput);

        // Schedule update/failure callbacks
        String updateCallbackSql = buildUpdateCallbackSql(taskId);
        String failureCallbackSql = buildFailureCallbackSql();

        return new SQLQuery("""
            DO
            $extract$
            BEGIN
              ${{workQuery}}
              -- Must be false, otherwise value is lost at COMMIT and the follow-up asyncify won't run
              PERFORM set_config('xyz.next_thread', $a$${{updateCallbackSql}}$a$, false);
            EXCEPTION WHEN OTHERS THEN
              ${{failureCallbackSql}}
            END
            $extract$;
            """)
                .withQueryFragment("workQuery", workQuery)
                .withQueryFragment("updateCallbackSql", updateCallbackSql)
                .withQueryFragment("failureCallbackSql", failureCallbackSql);
    }

    private SQLQuery buildWorkQuery(ExtractTaskInput taskInput) throws WebClientException {
        Map<String, String> effective = loadAliasToJsonPath();

        if (effective.isEmpty()) {
            return new SQLQuery("PERFORM 1;");
        }

        final String schema = getSchema(dbWriter());
        final String tableName = getRootTableName(space());

        long minI = loadMinI();
        long maxI = loadMaxI();

        if (maxI < minI) {
            return new SQLQuery("PERFORM 1;");
        }

        long totalRows = maxI - minI + 1;

        int effectiveTaskCount = threadCount > 0 ? threadCount : DEFAULT_THREAD_COUNT;
        effectiveTaskCount = (int) Math.max(1, Math.min((long) effectiveTaskCount, totalRows));

        long iRangeSize = (long) Math.ceil((double) totalRows / (double) effectiveTaskCount);
        if (iRangeSize <= 0) {
            return new SQLQuery("PERFORM 1;");
        }

        int threadNumber = taskInput.getThreadId();

        long fromI = minI + (threadNumber * iRangeSize);
        long toIExclusive = minI + ((threadNumber + 1L) * iRangeSize);

        if (fromI > maxI) {
            return new SQLQuery("PERFORM 1;");
        }

        toIExclusive = Math.min(toIExclusive, maxI + 1);

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ${schema}.${table} ")
                .append("SET searchable = COALESCE(searchable, '{}'::jsonb)");

        int idx = 0;
        for (Map.Entry<String, String> e : effective.entrySet()) {
            String aliasKey = e.getKey();
            String jsonPathWithType = e.getValue();

            if (aliasKey == null || aliasKey.isBlank() || jsonPathWithType == null || jsonPathWithType.isBlank()) {
                continue;
            }

            String jsonPath = jsonPathWithType.trim();
            boolean scalar = true;

            int typeIdx = jsonPath.lastIndexOf("::");

            if (typeIdx > -1) {
                String type = jsonPath.substring(typeIdx + 2).trim();
                jsonPath = jsonPath.substring(0, typeIdx).trim();
                scalar = !"array".equalsIgnoreCase(type);
            }

            if (jsonPath.isBlank()) {
                continue;
            }

            String safeKey = aliasKey.replace("'", "''");
            String paramName = "jsonPath_" + idx++;

            String fn = scalar ? "jsonpath_scalar" : "jsonpath_array";

            sql.append(" || jsonb_build_object('")
                    .append(safeKey)
                    .append("', ")
                    .append(fn)
                    .append("(jsondata::text, #{")
                    .append(paramName)
                    .append("}::text))");
        }

        if (idx == 0) {
            return new SQLQuery("PERFORM 1;");
        }

        sql.append(" WHERE i >= #{fromI} AND i < #{toI};");

        SQLQuery query = new SQLQuery(sql.toString())
                .withVariable("schema", schema)
                .withVariable("table", tableName)
                .withNamedParameter("fromI", fromI)
                .withNamedParameter("toI", toIExclusive);

        idx = 0;
        for (Map.Entry<String, String> e : effective.entrySet()) {
            String aliasKey = e.getKey();
            String jsonPathWithType = e.getValue();

            if (aliasKey == null || aliasKey.isBlank() || jsonPathWithType == null || jsonPathWithType.isBlank()) {
                continue;
            }

            String jsonPath = jsonPathWithType.trim();
            int typeIdx = jsonPath.lastIndexOf("::");

            if (typeIdx > -1) {
                jsonPath = jsonPath.substring(0, typeIdx).trim();
            }

            if (jsonPath.isBlank()) {
                continue;
            }

            query.withNamedParameter("jsonPath_" + idx++, jsonPath);
        }

        return query;
    }

    private String buildUpdateCallbackSql(int taskId) {
        if (isSimulation) {
            return "SELECT 'Update callback will be simulated';";
        }

        SpaceBasedTaskUpdate<ExtractTaskOutput> update = new SpaceBasedTaskUpdate<>();
        update.taskId = taskId;
        update.taskOutput = new ExtractTaskOutput();

        LambdaStepRequest req = new LambdaStepRequest()
                .withType(RequestType.UPDATE_CALLBACK)
                .withStep(this)
                .withProcessUpdate(update);

        String body = req.serialize();
        String tag = "cbreq" + taskId;

        return """
            SELECT aws_lambda.invoke(
              aws_commons.create_lambda_function_arn('%s', '%s'),
              $%s$%s$%s$::JSON,
              'Event'
            );
            """.formatted(
                getwOwnLambdaArn().toString(),
                getwOwnLambdaArn().getRegion(),
                tag, body, tag
        ).trim();
    }

    private String buildFailureCallbackSql() {
        if (isSimulation) {
            return "PERFORM 'Failure callback will be simulated';";
        }

        LambdaStepRequest req = new LambdaStepRequest()
                .withType(RequestType.FAILURE_CALLBACK)
                .withStep(this);

        String body = req.serialize();
        String tag = "fcreq" + getId().replaceAll("[^A-Za-z0-9]", "");

        return """
            PERFORM aws_lambda.invoke(
              aws_commons.create_lambda_function_arn('%s', '%s'),
              jsonb_set(
                $%s$%s$%s$::JSONB,
                '{step,status}',
                (
                  ($%s$%s$%s$::JSONB->'step'->'status')
                  || jsonb_build_object('errorCode', SQLSTATE::text, 'errorMessage', SQLERRM::text)
                ),
                true
              )::JSON,
              'Event'
            );
            """.formatted(
                getwOwnLambdaArn().toString(),
                getwOwnLambdaArn().getRegion(),
                tag, body, tag,
                tag, body, tag
        ).trim();
    }

    @Override
    protected void processFinalizedTasks(List<FinalizedTaskItem<ExtractTaskInput, ExtractTaskOutput>> finalizedTaskItems) {
        // No-op
    }

    @Override
    protected double calculateOverallNeededACUs() {
        if (overallEstimatedAcus > 0d) {
            return overallEstimatedAcus;
        }

        return 1.0;
    }

    @Override
    public List<Load> getNeededResources() {
        double acus = calculateOverallNeededACUs();
        if (acus <= 0d)
            acus = 1.0;

        try {
            return List.of(new Load().withResource(dbWriter()).withEstimatedVirtualUnits(acus));
        }
        catch (WebClientException e) {
            throw new StepException("Error calculating the necessary resources for the step.", e).withRetryable(true);
        }
    }

    @Override
    public void prepare(String owner, JobClientInfo ownerAuth) throws BaseHttpServerVerticle.ValidationException {
        if (getContext() == null)
            setContext(SpaceContext.DEFAULT);

        if (getVersionRef() == null)
            setVersionRef(new Ref(Ref.HEAD));

        super.prepare(owner, ownerAuth);
    }

    @Override
    protected void finalCleanUp() {
        // No-op
    }

    @Override
    public int getTimeoutSeconds() {
        return 8 * 60 * 60;
    }

    @Override
    public int getEstimatedExecutionSeconds() {
        double overallAcus = calculateOverallNeededACUs();
        int estimated = (int) Math.ceil(overallAcus * 60d);

        return Math.max(60, estimated);
    }

    @Override
    public String getDescription() {
        return "Extract JSONPath-based searchable values for space '" + getSpaceId() + "'.";
    }

    private record IRange(Long minI, Long maxI) {}

    private void loadIRange() {
        try {
            String schema = getSchema(dbWriter());
            String table = getRootTableName(space());

            IRange range = runReadQuerySync(
                    new SQLQuery("SELECT min(i) AS min_i, max(i) AS max_i FROM ${schema}.${table}")
                            .withVariable("schema", schema)
                            .withVariable("table", table),
                    dbWriter(),
                    0d,
                    rs -> {
                        if (!rs.next())
                            return new IRange(0L, -1L);

                        Long min = (Long) rs.getObject("min_i");
                        Long max = (Long) rs.getObject("max_i");

                        if (min == null || max == null)
                            return new IRange(0L, -1L);

                        return new IRange(min, max);
                    }
            );

            this.minI = range.minI();
            this.maxI = range.maxI();
        }
        catch (Exception e) {
            throw new StepException("Error while loading min / max i values.", e);
        }
    }

    private long loadMinI() {
        if (minI == Long.MIN_VALUE)
            loadIRange();

        return minI;
    }

    private long loadMaxI() {
        if (maxI == Long.MIN_VALUE)
            loadIRange();

        return maxI;
    }

    public static class ExtractTaskInput implements TaskPayload {
        private int threadId;

        public ExtractTaskInput() {}

        public ExtractTaskInput(int threadId) {
            this.threadId = threadId;
        }

        public int getThreadId() {
            return threadId;
        }

        public void setThreadId(int threadId) {
            this.threadId = threadId;
        }
    }

    public static class ExtractTaskOutput implements TaskPayload {
        // No-op
    }
}