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
package com.here.xyz.httpconnector.util.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.models.geojson.implementation.Geometry;

import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class Export extends Job {
    public static String ERROR_TYPE_HTTP_TRIGGER_FAILED = "http_trigger_failed";
    public static String ERROR_TYPE_TARGET_ID_INVALID = "targetId_invalid";
    public static String ERROR_TYPE_HTTP_TRIGGER_STATUS_FAILED = "http_get_trigger_status_failed";

    @JsonInclude
    private String type = "Export";

    @JsonView({Public.class})
    private Map<String,ExportObject> exportObjects;

    @JsonView({Public.class})
    private ExportStatistic statistic;

    @JsonView({Public.class})
    private ExportTarget exportTarget;

    @JsonView({Internal.class})
    private long estimatedFeatureCount;

    /** Only used by type VML */
    @JsonView({Public.class})
    private int maxTilesPerFile;

    /** Only used by type VML */
    @JsonView({Public.class})
    private Integer targetLevel;

    @JsonView({Public.class})
    private String targetVersion;

    @JsonView({Public.class})
    private Filters filters;

    @JsonView({Public.class})
    private String triggerId;

    public Export(){ }

    public Export(String description, String targetSpaceId, String targetTable, CSVFormat csvFormat, Strategy strategy) {
        this.description = description;
        this.targetSpaceId = targetSpaceId;
        this.targetTable = targetTable;
        this.strategy = strategy;
    }

    public String getType() {
        return type;
    }

    public long getEstimatedFeatureCount() {
        return estimatedFeatureCount;
    }

    public void setEstimatedFeatureCount(long estimatedFeatureCount) {
        this.estimatedFeatureCount = estimatedFeatureCount;
    }

    public ExportStatistic getStatistic(){
        return this.statistic;
    }

    public void setStatistic(ExportStatistic statistic) {
        this.statistic = statistic;
    }

    public Map<String,ExportObject> getExportObjects() {
        if(exportObjects == null)
            exportObjects = new HashMap<>();
        return exportObjects;
    }

    public void setExportObjects(Map<String, ExportObject> exportObjects) {
        this.exportObjects = exportObjects;
    }
    public ExportTarget getExportTarget() {
        return exportTarget;
    }

    public void setExportTarget(ExportTarget exportTarget) {
        this.exportTarget = exportTarget;
    }

    public Integer getTargetLevel() {
        return targetLevel;
    }

    public void setTargetLevel(Integer targetLevel) {
        this.targetLevel = targetLevel;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public int getMaxTilesPerFile() {
        return maxTilesPerFile;
    }

    public void setMaxTilesPerFile(int maxTilesPerFile) {
        this.maxTilesPerFile = maxTilesPerFile;
    }

    public Filters getFilters() {
        return filters;
    }

    public void setFilters(Filters filters) {
        this.filters = filters;
    }

    public void setType(String type) { this.type = type; }

    public String getTriggerId() { return triggerId; }
    public void setTriggerId(String triggerId) { this.triggerId = triggerId; }

    public Export withId(final String id) {
        setId(id);
        return this;
    }

    public Export withEstimatedFeatureCount(long estimatedFeatureCount){
        setEstimatedFeatureCount(estimatedFeatureCount);
        return this;
    }

    public Export withExportObjects(Map<String, ExportObject> exportObjects) {
        setExportObjects(exportObjects);
        return this;
    }

    public Export withErrorDescription(final String errorDescription) {
        setErrorDescription(errorDescription);
        return this;
    }

    public Export withDescription(final String description) {
        setDescription(description);
        return this;
    }

    public Export withTargetSpaceId(final String targetSpaceId) {
        setTargetSpaceId(targetSpaceId);
        return this;
    }

    public Export withTargetTable(final String targetTable) {
        setTargetTable(targetTable);
        return this;
    }

    public Export withStatus(final Job.Status status) {
        setStatus(status);
        return this;
    }

    public Export withCsvFormat(CSVFormat csv_format) {
        setCsvFormat(csv_format);
        return this;
    }

    public Export withCsvFormat(Strategy importStrategy) {
        setStrategy(importStrategy);
        return this;
    }

    public Export withCreatedAt(final long createdAt) {
        setCreatedAt(createdAt);
        return this;
    }

    public Export withUpdatedAt(final long updatedAt) {
        setUpdatedAt(updatedAt);
        return this;
    }

    public Export withExecutedAt(final Long startedAt) {
        setExecutedAt(startedAt);
        return this;
    }

    public Export withFinalizedAt(final Long finalizedAt) {
        setFinalizedAt(finalizedAt);
        return this;
    }

    public Export withExp(final Long exp) {
        setExp(exp);
        return this;
    }

    public Export withTargetConnector(String targetConnector) {
        setTargetConnector(targetConnector);
        return this;
    }

    public Export withErrorType(String errorType) {
        setErrorType(errorType);
        return this;
    }

    public Export withSpaceVersion(final long spaceVersion) {
        setSpaceVersion(spaceVersion);
        return this;
    }

    public Export withAuthor(String author) {
        setAuthor(author);
        return this;
    }

    public Export withMaxTilesPerFile(final int maxTilesPerFile) {
        setMaxTilesPerFile(maxTilesPerFile);
        return this;
    }

    public Export withFilters(final Filters filters) {
        setFilters(filters);
        return this;
    }

    public Export withExportTarget(final ExportTarget exportTarget) {
        setExportTarget(exportTarget);
        return this;
    }

    public Export withTargetLevel(final Integer targetLevel) {
        setTargetLevel(targetLevel);
        return this;
    }

    public Export withTargetVersion(final String targetVersion) {
        setTargetVersion(targetVersion);
        return this;
    }

    public Export withParams(Map params) {
        setParams(params);
        return this;
    }

    public Export withTriggerId(String triggerId) {
        setTriggerId(triggerId);
        return this;
    }

    public static class ExportStatistic{
        private long rowsUploaded;
        private long filesUploaded;
        private long bytesUploaded;

        public long getRowsUploaded() {
            return rowsUploaded;
        }

        public void setRowsUploaded(long rowsUploaded) {
            this.rowsUploaded = rowsUploaded;
        }

        public ExportStatistic withRowsUploaded(long rowsUploaded){
            this.setRowsUploaded(rowsUploaded);
            return this;
        }

        public long getFilesUploaded() {
            return filesUploaded;
        }

        public void setFilesUploaded(long filesUploaded) {
            this.filesUploaded = filesUploaded;
        }

        public ExportStatistic withFilesUploaded(long filesUploaded){
            this.setFilesUploaded(filesUploaded);
            return this;
        }

        public long getBytesUploaded() {
            return bytesUploaded;
        }

        public void setBytesUploaded(long bytesUploaded) {
            this.bytesUploaded = bytesUploaded;
        }

        public ExportStatistic withBytesUploaded(long bytesUploaded){
            this.setBytesUploaded(bytesUploaded);
            return this;
        }

        public void addRows(long rows){
            rowsUploaded += rows;
        }
        public void addBytes(long bytes){
            bytesUploaded += bytes;
        }
        public void addFiles(long files){
            filesUploaded += files;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class ExportTarget{
        @JsonView({Public.class})
        public enum Type {
            VML, DOWNLOAD, S3 /** same as download */;
            public static Type of(String value) {
                if (value == null) {
                    return null;
                }
                try {
                    return valueOf(value.toLowerCase());
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }
        }

        @JsonView({Public.class})
        private String targetId;

        @JsonView({Public.class})
        private Type type;

        public String getTargetId() {
            return targetId;
        }

        public void setTargetId(String targetId) {
            this.targetId = targetId;
        }

        public ExportTarget withTargetId(String targetId){
            this.setTargetId(targetId);
            return this;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public ExportTarget withType(final Type type) {
            setType(type);
            return this;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class SpatialFilter{

        @JsonView({Public.class})
        private Geometry geometry;

        @JsonView({Public.class})
        private int radius;

        @JsonView({Public.class})
        private boolean clipped;

        public Geometry getGeometry() {
            return geometry;
        }

        public void setGeometry(Geometry geometry) {
            this.geometry = geometry;
        }

        public int getRadius() {
            return radius;
        }

        public void setRadius(int radius) {
            this.radius = radius;
        }

        public boolean isClipped() {
            return clipped;
        }

        public void setClipped(boolean clipped) {
            this.clipped = clipped;
        }

        public SpatialFilter withGeometry(Geometry geometry){
            this.setGeometry(geometry);
            return this;
        }
        public SpatialFilter withRadius(final int radius) {
            setRadius(radius);
            return this;
        }
        public SpatialFilter withClipped(final boolean clipped) {
            setClipped(clipped);
            return this;
        }
    }

    public static class Filters {
        @JsonView({Public.class})
        private String propertyFilter;

        @JsonView({Public.class})
        private SpatialFilter spatialFilter;

        public String getPropertyFilter() {
            return propertyFilter;
        }

        public void setPropertyFilter(String propertyFilter) {
            this.propertyFilter = propertyFilter;
        }

        public SpatialFilter getSpatialFilter() {
            return spatialFilter;
        }

        public void setSpatialFilter(SpatialFilter spatialFilter) {
            this.spatialFilter = spatialFilter;
        }

        public Filters withSpatialFilter(SpatialFilter spatialFilter) {
            setSpatialFilter(spatialFilter);
            return this;
        }

        public Filters withPropertyFilter(String propertyFilter) {
            setPropertyFilter(propertyFilter);
            return this;
        }
    }
}
