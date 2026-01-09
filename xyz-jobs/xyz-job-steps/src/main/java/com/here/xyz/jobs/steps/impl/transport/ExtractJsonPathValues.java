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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.JobClientInfo;
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

    @JsonIgnore
    private Boolean searchableColumnExists;

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

        // no searchable column, no task
        if (!hasSearchableColumn()) {
            return List.of();
        }

        int usable = 0;
        for (Map.Entry<String, String> e : effective.entrySet()) {
            String aliasKey = e.getKey();
            String jsonPath = e.getValue();

            if (aliasKey == null || aliasKey.isBlank() || jsonPath == null || jsonPath.isBlank()) {
                continue;
            }

            int typeIdx = jsonPath.indexOf("::");
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

        // empty table
        long min = loadMinI();
        long max = loadMaxI();
        if (max < min) {
            return List.of();
        }

        final int effectiveTaskCount = threadCount > 0 ? threadCount : DEFAULT_THREAD_COUNT;
        List<ExtractTaskInput> items = new ArrayList<>(effectiveTaskCount);
        for (int t = 0; t < effectiveTaskCount; t++) {
            items.add(new ExtractTaskInput(t));
        }
        return items;
    }

    private boolean hasSearchableColumn() {
        if (searchableColumnExists != null) {
            return searchableColumnExists;
        }

        try {
            final String schema = getSchema(dbWriter());
            final String table = getRootTableName(space());

            Boolean exists = runReadQuerySync(
                    new SQLQuery("""
                      SELECT 1
                      FROM information_schema.columns
                      WHERE table_schema = #{schema}
                        AND table_name = #{table}
                        AND column_name = 'searchable'
                      LIMIT 1
                      """)
                            .withNamedParameter("schema", schema)
                            .withNamedParameter("table", table),
                    dbWriter(),
                    0d,
                    rs -> rs.next()
            );

            searchableColumnExists = (exists != null && exists);
            return searchableColumnExists;
        }
        catch (Exception e) {
            throw new StepException("Failed to verify existence of 'searchable' column.", e).withRetryable(true);
        }
    }

    private Map<String, String> loadAliasToJsonPath() throws WebClientException {
        // If job didn’t provide it, derive from the space config at runtime
        if (aliasToJsonPath == null || aliasToJsonPath.isEmpty()) {
            Map<String, String> derived = Space.toExtractableSearchProperties(space());
            this.aliasToJsonPath = derived;
        }
        return aliasToJsonPath == null ? Map.of() : aliasToJsonPath;
    }

    @Override
    protected SQLQuery buildTaskQuery(Integer taskId, ExtractTaskInput taskInput, String failureCallback) throws WebClientException {
        Map<String, String> effective = loadAliasToJsonPath();
        if (effective.isEmpty()) {
            return new SQLQuery("SELECT 1;");
        }

        // OLD_LAYOUT, no-op
        if (!hasSearchableColumn()) {
            return new SQLQuery("SELECT 1;");
        }

        final String schema = getSchema(dbWriter());
        final Space space = space();
        final String tableName = getRootTableName(space);

        long minI = loadMinI();
        long maxI = loadMaxI();
        if (maxI < minI) {
            return new SQLQuery("SELECT 1;");
        }

        final int effectiveTaskCount = threadCount > 0 ? threadCount : DEFAULT_THREAD_COUNT;
        long totalRows = maxI - minI + 1;
        long iRangeSize = (long) Math.ceil((double) totalRows / (double) effectiveTaskCount);
        if (iRangeSize <= 0) {
            return new SQLQuery("SELECT 1;");
        }

        int threadNumber = taskInput.getThreadId();

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ${schema}.${table} ")
                .append("SET searchable = COALESCE(searchable, '{}'::jsonb)");

        int idx = 0;
        for (Map.Entry<String, String> e : effective.entrySet()) {
            String aliasKey = e.getKey();
            String jsonPath = e.getValue();

            if (aliasKey == null || aliasKey.isBlank() || jsonPath == null || jsonPath.isBlank()) {
                continue;
            }

            int typeIdx = jsonPath.indexOf("::");
            if (typeIdx > -1) {
                jsonPath = jsonPath.substring(0, typeIdx).trim();
            }

            if (jsonPath.isBlank()) {
                continue;
            }

            String safeKey = aliasKey.replace("'", "''");
            String paramName = "jsonPath_" + idx++;

            sql.append(" || jsonb_build_object('")
                    .append(safeKey)
                    .append("', jsonpath_rfc9535(jsondata::jsonb, #{")
                    .append(paramName)
                    .append("}::jsonpath))");
        }

        if (idx == 0) {
            return new SQLQuery("SELECT 1;");
        }

        sql.append(" WHERE i >= (#{minI} + (#{threadNumber} * #{iRangeSize}))")
                .append(" AND i < (#{minI} + ((#{threadNumber} + 1) * #{iRangeSize}))");

        SQLQuery query = new SQLQuery(sql.toString())
                .withVariable("schema", schema)
                .withVariable("table", tableName)
                .withNamedParameter("minI", minI)
                .withNamedParameter("threadNumber", threadNumber)
                .withNamedParameter("iRangeSize", iRangeSize);

        idx = 0;
        for (Map.Entry<String, String> e : effective.entrySet()) {
            String aliasKey = e.getKey();
            String jsonPath = e.getValue();

            if (aliasKey == null || aliasKey.isBlank() || jsonPath == null || jsonPath.isBlank()) {
                continue;
            }

            int typeIdx = jsonPath.indexOf("::");
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