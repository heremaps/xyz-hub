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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.httpconnector.util.Futures.futurify;
import static com.here.xyz.httpconnector.util.jobs.Export.CompositeMode.DEACTIVATED;
import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.DOWNLOAD;
import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.VML;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONID_FC_B64;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.TILEID_FC_B64;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONED_JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.executed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalizing;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.prepared;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.preparing;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.queued;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.trigger_executed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.waiting;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.setJobAborted;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.setJobFailed;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.updateJobStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.amazonaws.services.emrserverless.model.JobRunState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCExporter;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.util.emr.EMRManager;
import com.here.xyz.httpconnector.util.jobs.datasets.DatasetDescription;
import com.here.xyz.httpconnector.util.jobs.datasets.Files;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.responses.StatisticsResponse.PropertyStatistics;
import com.here.xyz.util.Hasher;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class Export extends JDBCBasedJob<Export> {
    private static final Logger logger = LogManager.getLogger();
    public static String ERROR_DESCRIPTION_HTTP_TRIGGER_FAILED = "http_trigger_failed";
    public static String ERROR_DESCRIPTION_TARGET_ID_INVALID = "targetId_invalid";
    public static String ERROR_DESCRIPTION_HTTP_TRIGGER_STATUS_FAILED = "http_get_trigger_status_failed";
    public static String ERROR_DESCRIPTION_PERSISTENT_EXPORT_FAILED = "persistent_export_of_super_failed";
    private static int VML_EXPORT_MIN_TARGET_LEVEL = 4;
    private static int VML_EXPORT_MAX_TARGET_LEVEL = 13;
    private static int VML_EXPORT_MAX_TILES_PER_FILE = 8192;
    @JsonIgnore
    private AtomicBoolean executing = new AtomicBoolean();

    @JsonView({Public.class})
    private Map<String,ExportObject> exportObjects;

    @JsonView({Public.class})
    private Map<String,ExportObject> superExportObjects;

    @JsonView({Public.class})
    private ExportStatistic statistic;

    @JsonView({Public.class})
    private ExportStatistic superStatistic;

    @JsonView({Public.class})
    private String superId;

    @JsonView({Public.class})
    private ExportTarget exportTarget;

    @JsonView({Internal.class})
    private long estimatedFeatureCount;

    @JsonView({Internal.class})
    private Map<String,Long> searchableProperties;

    @JsonView({Internal.class})
    private List<String> processingList;

    /** Only used by type VML */
    @JsonView({Public.class})
    private int maxTilesPerFile;

    /** Only used by type VML */
    @JsonView({Public.class})
    private Boolean clipped;

    /** Only used by type VML */
    @JsonView({Public.class})
    private Integer targetLevel;

    /** Only used by type VML */
    @JsonView({Public.class})
    private String partitionKey;

    @JsonView({Public.class})
    private String targetVersion;

    @JsonView({Public.class})
    private Filters filters;

    @JsonView({Public.class})
    private String triggerId;

    @JsonIgnore
    private AtomicBoolean emrJobExecuting = new AtomicBoolean();
    public String emrJobId;
    @JsonIgnore
    private EMRManager emrManager;
    private boolean emrTransformation;
    private String emrType;

    private static String PARAM_COMPOSITE_MODE = "compositeMode";
    private static String PARAM_PERSIST_EXPORT = "persistExport";
    private static String PARAM_VERSIONS_TO_KEEP = "versionsToKeep";
    private static String PARAM_ENABLE_HASHED_SPACEID = "enableHashedSpaceId";
    private static String PARAM_RUN_AS_ID = "runAsId";
    private static String PARAM_SCOPE = "scope";
    private static String PARAM_EXTENDS = "extends";
    private static String PARAM_CONTEXT = "context";
    private static String PARAM_SKIP_TRIGGER = "skipTrigger";

    public Export() {
        super();
    }

    @Override
    public Future<Export> init() {
        return setDefaults();
    }

    @Override
    public Future<Export> setDefaults() {
        //TODO: Do field initialization at instance initialization time
        return super.setDefaults()
            .compose(job -> {
                if (   getExportTarget() != null && getExportTarget().getType() == VML
                    && (getCsvFormat() == null || ( getCsvFormat() != PARTITIONID_FC_B64 && getCsvFormat() != PARTITIONED_JSON_WKB ))) {
                    setCsvFormat(TILEID_FC_B64);
                    if (getMaxTilesPerFile() == 0)
                        setMaxTilesPerFile(VML_EXPORT_MAX_TILES_PER_FILE);
                }
                return HubWebClient.getSpaceStatistics(job.getTargetSpaceId());
            })
            .compose(statistics -> {
                //Store count of features which are in source layer
                setEstimatedFeatureCount(statistics.getCount().getValue());

                HashMap<String, Long> searchableProperties = new HashMap<>();
                for (PropertyStatistics property : statistics.getProperties().getValue())
                    if (property.isSearchable())
                        searchableProperties.put(property.getKey(), property.getCount());
                if (!searchableProperties.isEmpty())
                    setSearchableProperties(searchableProperties);

                return Future.succeededFuture(this);
            });
    }

    @Override
    public Future<Export> validate() {
        return super.validate()
            .compose(job -> futurify(() -> validateExport()))
            .compose(j -> Future.succeededFuture(j), e -> e instanceof HttpException ? Future.failedFuture(e)
                : Future.failedFuture(new HttpException(BAD_REQUEST, e.getMessage(), e)));
    }

    private Export validateExport() throws HttpException {
        if (getExportTarget() == null)
            throw new HttpException(BAD_REQUEST,("Please specify exportTarget!"));

        if (getExportTarget().getType().equals(VML)){

            switch (getCsvFormat()) {
                case TILEID_FC_B64:
                case PARTITIONID_FC_B64:
                case PARTITIONED_JSON_WKB:
                    break;
                default:
                    throw new HttpException(BAD_REQUEST, "Invalid Format! Allowed [" + TILEID_FC_B64 + ","
                        + PARTITIONID_FC_B64 + "," + PARTITIONED_JSON_WKB + "]");
            }

            if (getExportTarget().getTargetId() == null)
                throw new HttpException(BAD_REQUEST,("Please specify the targetId!"));
        }

        if ( getCsvFormat().equals(TILEID_FC_B64) || ( getCsvFormat().equals(PARTITIONED_JSON_WKB) && (getPartitionKey() == null || "tileid".equalsIgnoreCase(getPartitionKey())))) {
            if (getTargetLevel() == null)
                throw new HttpException(BAD_REQUEST, "Please specify targetLevel! Allowed range [" + VML_EXPORT_MIN_TARGET_LEVEL + ":"
                        + VML_EXPORT_MAX_TARGET_LEVEL + "]");

            if (getTargetLevel() < VML_EXPORT_MIN_TARGET_LEVEL || getTargetLevel() > VML_EXPORT_MAX_TARGET_LEVEL)
                throw new HttpException(BAD_REQUEST, "Invalid targetLevel! Allowed range [" + VML_EXPORT_MIN_TARGET_LEVEL
                        + ":" + VML_EXPORT_MAX_TARGET_LEVEL + "]");
        }

        Filters filters = getFilters();
        if (filters != null) {
            if (filters.getSpatialFilter() != null) {
                Geometry geometry = filters.getSpatialFilter().getGeometry();
                if (geometry == null)
                    throw new HttpException(BAD_REQUEST, "Please specify a geometry for the spatial filter!");
                else {
                    try {
                        geometry.validate();
                        WKTHelper.geometryToWKB(geometry);
                    }
                    catch (Exception e){
                        throw new HttpException(BAD_REQUEST, "Cant parse filter geometry!");
                    }
                }
            }
        }

        CompositeMode compositeMode = readParamCompositeMode();
        Map ext = readParamExtends();

        if (readPersistExport())
            setExp(-1l);

        if (ext != null) {
            boolean superIsReadOnly = ext.get("readOnly") != null ? (boolean) ext.get("readOnly") : false;
            Map l2Ext = (Map) ext.get("extends");

            if (l2Ext != null)
                //L2 Composite
                superIsReadOnly = l2Ext.get("readOnly") != null ? (boolean) l2Ext.get("readOnly") : false;

            if (compositeMode.equals(DEACTIVATED) && superIsReadOnly) {
                //Enabled by default
                params.put(PARAM_COMPOSITE_MODE, CompositeMode.FULL_OPTIMIZED);
            }

            if(!superIsReadOnly && compositeMode.equals(CompositeMode.FULL_OPTIMIZED)) {
                params.remove(PARAM_COMPOSITE_MODE);
                logger.info("job[{}] CompositeMode=FULL_OPTIMIZED requires readOnly on superLayer - fall back!", getId());
            }

            if (csvFormat.equals(GEOJSON))
                params.remove(PARAM_COMPOSITE_MODE);
        }else{
            //Only make sense in case of extended space
            params.remove(PARAM_COMPOSITE_MODE);
        }

        SpaceContext context = readParamContext();

        if (readParamExtends() != null && context == null)
            addParam("context", DEFAULT);

        if (getEstimatedFeatureCount() > 1000000 //searchable limit without index
            && getPartitionKey() != null && !"id".equals(getPartitionKey())
            && !searchableProperties.containsKey(getPartitionKey().replaceFirst("^(p|properties)\\." ,"")))
            throw new HttpException(BAD_REQUEST, "partitionKey [" + getPartitionKey() + "] is not a searchable property");

        return this;
    }

    public List<String> getProcessingList() {
        return processingList;
    }

    public void setProcessingList(List<String> processingList) {
        this.processingList = processingList;
    }

    public Export withProcessingList(List<String> processingList) {
        setProcessingList(processingList);
        return this;
    }

    public long getEstimatedFeatureCount() {
        return estimatedFeatureCount;
    }

    public void setEstimatedFeatureCount(long estimatedFeatureCount) {
        this.estimatedFeatureCount = estimatedFeatureCount;
    }

    public Export withEstimatedFeatureCount(long estimatedFeatureCount){
        setEstimatedFeatureCount(estimatedFeatureCount);
        return this;
    }

    public Map<String,Long> getSearchableProperties() {
        return searchableProperties;
    }

    public void setSearchableProperties(Map<String,Long> searchableProperties) {
        this.searchableProperties = searchableProperties;
    }

    public Export withSearchableProperties(Map<String,Long> searchableProperties) {
        setSearchableProperties(searchableProperties);
        return this;
    }

    public ExportStatistic getSuperStatistic() {
        return this.superStatistic;
    }

    public ExportStatistic getStatistic() {
        return getTotalStatistic();
    }

    @JsonIgnore
    private ExportStatistic getTotalStatistic() {
        if (this.statistic == null)
            return null;

        if (this.superStatistic == null)
            return this.statistic;

        return new ExportStatistic()
            .withBytesUploaded(this.statistic.bytesUploaded + this.superStatistic.bytesUploaded)
            .withFilesUploaded(this.statistic.filesUploaded + this.superStatistic.filesUploaded)
            .withRowsUploaded(this.statistic.rowsUploaded + this.superStatistic.rowsUploaded);
    }

    public void setSuperId(String superId) {
        this.superId = superId;
    }

    public String getSuperId() {
        return this.superId;
    }

    public void setStatistic(ExportStatistic statistic) {
        this.statistic = statistic;
    }

    public void setSuperStatistic(ExportStatistic superStatistic) {
        this.superStatistic = superStatistic;
    }

    public void addStatistic(ExportStatistic statistic) {
        if (this.statistic == null)
            this.statistic = statistic;
        else {
            this.statistic.setBytesUploaded(this.statistic.getBytesUploaded() + statistic.getBytesUploaded());
            this.statistic.setFilesUploaded(this.statistic.getFilesUploaded() + statistic.getFilesUploaded());
            this.statistic.setRowsUploaded(this.statistic.getRowsUploaded() + statistic.getRowsUploaded());
        }
    }

    public Map<String,ExportObject> getSuperExportObjects() {
        if(superExportObjects == null)
            superExportObjects = new HashMap<>();
        return superExportObjects;
    }

    public void setSuperExportObjects(Map<String, ExportObject> superExportObjects) {
        this.superExportObjects = superExportObjects;
    }

    @JsonIgnore
    public List<ExportObject> getExportObjectsAsList() {
        List<ExportObject> exportObjectList = new ArrayList<>();

        if (superExportObjects != null)
            exportObjectList.addAll(superExportObjects.values());
        if (exportObjects != null)
            exportObjectList.addAll(exportObjects.values());

        return exportObjectList;
    }

    public Map<String,ExportObject> getExportObjects() {
        if(exportObjects == null)
            exportObjects = new HashMap<>();
        return exportObjects;
    }

    public void setExportObjects(Map<String, ExportObject> exportObjects) {
        this.exportObjects = exportObjects;
    }

    public Export withExportObjects(Map<String, ExportObject> exportObjects) {
        setExportObjects(exportObjects);
        return this;
    }

    /**
     * @deprecated Please use method {@link #getTarget()} instead.
     */
    @Deprecated
    public ExportTarget getExportTarget() {
        return exportTarget;
    }

    /**
     * @deprecated Please use method {@link #setTarget(DatasetDescription)} instead.
     * @param exportTarget
     */
    @Deprecated
    public void setExportTarget(ExportTarget exportTarget) {
        this.exportTarget = exportTarget;
    }

    /**
     * @deprecated Please use method {@link #withTarget(DatasetDescription)} instead.
     * @param exportTarget
     */
    @Deprecated
    public Export withExportTarget(final ExportTarget exportTarget) {
        setExportTarget(exportTarget);
        return this;
    }

    @Override
    public void setTarget(DatasetDescription target) {
        super.setTarget(target);
        //Keep BWC
        if (target instanceof Files files) {
            setExportTarget(new ExportTarget().withType(DOWNLOAD));
            setCsvFormat(files.getOutputSettings().getFormat());
            setTargetLevel(files.getOutputSettings().getTileLevel());
            setClipped(files.getOutputSettings().isClipped());
            setMaxTilesPerFile(files.getOutputSettings().getMaxTilesPerFile());
        }
    }

    public Integer getTargetLevel() {
        return targetLevel;
    }

    public void setTargetLevel(Integer targetLevel) {
        this.targetLevel = targetLevel;
    }

    public Export withTargetLevel(final Integer targetLevel) {
        setTargetLevel(targetLevel);
        return this;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public Export withPartitionKey(final String partitionKey) {
        setPartitionKey(partitionKey);
        return this;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public Export withTargetVersion(final String targetVersion) {
        setTargetVersion(targetVersion);
        return this;
    }

    public int getMaxTilesPerFile() {
        return maxTilesPerFile;
    }

    public void setMaxTilesPerFile(int maxTilesPerFile) {
        this.maxTilesPerFile = maxTilesPerFile;
    }

    public Export withMaxTilesPerFile(final int maxTilesPerFile) {
        setMaxTilesPerFile(maxTilesPerFile);
        return this;
    }

    public Boolean getClipped() {
        return clipped;
    }

    public void setClipped(boolean clipped) {
        this.clipped = clipped;
    }

    public Export withClipped(final boolean clipped) {
        setClipped(clipped);
        return this;
    }

    public Filters getFilters() {
        return filters;
    }

    public void setFilters(Filters filters) {
        this.filters = filters;
    }

    public Export withFilters(final Filters filters) {
        setFilters(filters);
        return this;
    }

    public String getTriggerId() { return triggerId; }

    public void setTriggerId(String triggerId) { this.triggerId = triggerId; }

    public Export withTriggerId(String triggerId) {
        setTriggerId(triggerId);
        return this;
    }

    public boolean readParamSkipTrigger() {
        return params != null && params.containsKey(PARAM_SKIP_TRIGGER) ? (boolean) this.getParam(PARAM_SKIP_TRIGGER) : false;
    }

    public boolean readPersistExport() {
        return  params != null && params.containsKey(PARAM_PERSIST_EXPORT) ? (boolean) this.getParam(PARAM_PERSIST_EXPORT) : false;
    }

    public Map readParamExtends() {
        return params != null && params.containsKey(PARAM_EXTENDS) ? (Map) this.getParam(PARAM_EXTENDS) : null;
    }

    public SpaceContext readParamContext() {
        return params != null && params.containsKey(PARAM_CONTEXT) ? SpaceContext.valueOf((String) this.params.get(PARAM_CONTEXT)) : null;
    }

    public CompositeMode readParamCompositeMode() {
        return params != null && params.containsKey(PARAM_COMPOSITE_MODE)
                ? CompositeMode.valueOf((String) params.get(PARAM_COMPOSITE_MODE))
                : DEACTIVATED;
    }

    public String extractSuperSpaceId() {
        Map extension = readParamExtends();
        if (extension == null)
            return null;

        Map recursiveExtension = (Map) extension.get(PARAM_EXTENDS);
        if (recursiveExtension != null)
            return (String) recursiveExtension.get("spaceId");

        return (String) extension.get("spaceId");
    }

    public boolean isSuperSpacePersist() {
        Map extension = readParamExtends();
        if (extension == null)
            return false;

        Map recursiveExtension = (Map) extension.get(PARAM_EXTENDS);
        if (recursiveExtension != null)
            return recursiveExtension.get("readOnly") != null ? (boolean) recursiveExtension.get("readOnly") : false;

        return extension.get("readOnly") != null ? (boolean) extension.get("readOnly") : false;
    }

    public void resetToPreviousState() throws Exception {
        switch (getStatus()) {
            case failed:
            case aborted:
                super.resetToPreviousState();
                break;
            case preparing:
                resetStatus(waiting);
                break;
            case executing:
                resetStatus(prepared);
                break;
            case executing_trigger:
                resetStatus(executed);
                break;
            case collecting_trigger_status:
                resetStatus(trigger_executed);
                break;
        }
    }

    public static class ExportStatistic {
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
    public static class ExportTarget {
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
    public static class SpatialFilter {

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

        public SpatialFilter withGeometry(Geometry geometry){
            this.setGeometry(geometry);
            return this;
        }

        public int getRadius() {
            return radius;
        }

        public void setRadius(int radius) {
            this.radius = radius;
        }

        public SpatialFilter withRadius(final int radius) {
            setRadius(radius);
            return this;
        }

        public boolean isClipped() {
            return clipped;
        }

        public void setClipped(boolean clipped) {
            this.clipped = clipped;
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

        public Filters withPropertyFilter(String propertyFilter) {
            setPropertyFilter(propertyFilter);
            return this;
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
    }

    @Override
    public String getQueryIdentifier() {
        return "export_hint";
    }

    @Override
    public boolean needRdsCheck() {
        //In next stage we need database resources
        if (getStatus().equals(Job.Status.queued))
            return true;
        return false;
    }

    public Future<Job> checkPersistentExports() {
        //Check if we already have persistent exports. May trigger one.
        if(readPersistExport())
            return checkPersistentExport();
        else if(isSuperSpacePersist() && readParamCompositeMode().equals(CompositeMode.FULL_OPTIMIZED))
            return checkPersistentSuperExport();
        //No indication for persistent exports are given - proceed normal
        return updateJobStatus(this, prepared);
    }

    public Future<Export> searchPersistentJobOnTarget(String targetId){
        return CService.jobConfigClient.getList(getMarker(), null , null, targetId)
                .compose(jobs -> {
                    Export existingJob = null;

                    for (Job jobCandidate :jobs) {
                        if(jobCandidate instanceof Import)
                            continue;

                        //exp=-1 => persistent
                        //hash must fit
                        logger.info(getMarker(), "job[{}] Check existing job {}:{} ", getId(), jobCandidate.getId(), jobCandidate.getStatus());
                        if( jobCandidate.getExp() == -1 && ((Export)jobCandidate).getHashForPersistentStorage().equals(getHashForPersistentStorage())){
                            //try to find a finalized one - doesn't matter if its the origin export
                            if(jobCandidate.getStatus().equals(finalized) || jobCandidate.getStatus().equals(trigger_executed)) {
                                logger.info(getMarker(), "job[{}] Found existing persistent job {}:{} ", getId(), jobCandidate.getId(), jobCandidate.getStatus());
                                existingJob = (Export) jobCandidate;
                                break;
                            }else if(jobCandidate.getStatus().equals(failed) && jobCandidate.getId().equalsIgnoreCase(getId() + "_missing_base")) {
                                logger.info(getMarker(), "job[{}] Spawned job has failed {}:{} ", getId(), jobCandidate.getId(), jobCandidate.getStatus());
                                existingJob = (Export) jobCandidate;
                                break;
                            }
                        }
                    }
                    return Future.succeededFuture(existingJob);
                });
    }

    public Future<Job> checkPersistentExport() {
        //Deliver result if Export is already available
        return searchPersistentJobOnTarget(targetSpaceId)
            .compose(existingJob -> {
                if (existingJob != null) {
                    //metafile is present but Export is not started yet
                    if (existingJob.getId().equals(getId()) && existingJob.getStatus() == preparing) {
                        //We need to start the export by ourselves
                        return updateJobStatus(this, prepared);
                    }
                    else if (existingJob.getStatus() == finalized || existingJob.getStatus() == trigger_executed) {
                        //Export is available - use it and skip jdbc-export
                        logger.info("job[{}] found persistent export files of {}", getId(), existingJob.getId());
                        setSuperId(existingJob.getId());
                        addDownloadLinks(existingJob);
                        setExportObjects(existingJob.getExportObjects());

                        setStatistic(existingJob.getStatistic());
                        return updateJobStatus(this, executed);
                    }
                    else if(existingJob.getStatus() == failed) {
                        //Export is available but is failed - abort also this export.
                        String message = String.format("Another related job "+existingJob.getId()+" has failed.",
                                existingJob.getTargetSpaceId(), existingJob.getTargetLevel(), existingJob.getStatus());

                        logger.warn("job[{}] Export {}", getId(), message);
                        setJobFailed(this, message, Job.ERROR_TYPE_EXECUTION_FAILED);
                        return Future.failedFuture(Job.ERROR_TYPE_EXECUTION_FAILED);
                    }
                    else {
                        //Go back to queuing - we need to wait for the completion of an already running persist export.
                        return updateJobStatus(this, queued);
                    }
                }
                else
                    return updateJobStatus(this, prepared);
            });
    }

    public Future<Job> checkPersistentSuperExport() {
        //Check if we can find a persistent export and check status. If no persistent export is available we are starting one.
        String superSpaceId = extractSuperSpaceId();

        return searchPersistentJobOnTarget(superSpaceId)
            .compose(existingJob -> {
                if (existingJob == null) {
                    logger.info("job[{}] Persist Export {} of Base-Layer is missing -> starting one!", getId(), superSpaceId);

                    Export baseExport = new Export()
                        .withId(getId() + "_missing_base")
                        .withDescription("Persistent Base Export for " + getId())
                        .withExportTarget(getExportTarget())
                        .withFilters(getFilters())
                        .withMaxTilesPerFile(getMaxTilesPerFile())
                        .withTargetLevel(getTargetLevel())
                        .withPartitionKey(getPartitionKey())
                        .withCsvFormat(getCsvFormat())
                        .withEmrTransformation(isEmrTransformation())
                        .withEmrType(getEmrType());


                    //We only need reusable data on S3
                    baseExport.addParam(PARAM_SKIP_TRIGGER, true);
                    baseExport.addParam(PARAM_PERSIST_EXPORT, true);

                    logger.info("job[{}] Trigger Persist Export {} of Super-Layer!", getId(), superSpaceId);
                    return HubWebClient.performBaseLayerExport(superSpaceId, baseExport)
                        .compose(newBaseExport -> {
                            logger.info("job[{}] Need to wait for finalization of persist Export {} of base-layer!", getId(), newBaseExport.getId());
                            setSuperId(newBaseExport.getId());

                            return updateJobStatus(this,queued);
                        })
                        .onFailure(f -> setJobFailed(this, ERROR_DESCRIPTION_PERSISTENT_EXPORT_FAILED, ERROR_TYPE_EXECUTION_FAILED));
                }
                else if (existingJob.getStatus() == failed) {
                    logger.info("job[{}] Persist Export {} of Super-Layer has failed!", getId(), superSpaceId);
                    return setJobFailed(this,ERROR_DESCRIPTION_PERSISTENT_EXPORT_FAILED, ERROR_TYPE_EXECUTION_FAILED);
                }
                else if (existingJob.getStatus() == finalized) {
                    logger.info("job[{}] Persist Export {} of Super-Layer is available!", getId(), superSpaceId);
                    if (superId == null)
                        setSuperId(existingJob.getId());

                    setSuperStatistic(existingJob.getStatistic());
                    return updateJobStatus(this, prepared);
                }
                else {
                    logger.info("job[{}] Persist Export {} - need to wait for finalization of persist Export of base-layer!", getId(), superSpaceId);
                    return updateJobStatus(this, queued);
                }
            });
    }

    @Override
    public Future<Job> executeStart() {
        return isValidForStart()
            .compose(job -> prepareStart())
            .onSuccess(job -> CService.exportQueue.addJob(job));
    }

    @Override
    public void execute() {
        if (!executing.compareAndSet(false, true))
            return;

        setExecutedAt(Core.currentTimeMillis() / 1000L);

        JDBCExporter.executeExport(this, JDBCImporter.getDefaultSchema(getTargetConnector()), CService.configuration.JOBS_S3_BUCKET,
                CService.jobS3Client.getS3Path(this), CService.configuration.JOBS_REGION)
            .onSuccess(statistic -> {
                    //Everything is processed
                    logger.info("job[{}] Export of '{}' completely succeeded!", getId(), getTargetSpaceId());
                    addStatistic(statistic);
                    addDownloadLinks(this);
                    updateJobStatus(this, executed);
                }
            )
            .onFailure(e -> {
                logger.warn("job[{}] export of '{}' failed! ", getId(), getTargetSpaceId(), e);

                if (e.getMessage() != null && e.getMessage().equalsIgnoreCase("Fail to read any response from the server, the underlying connection might get lost unexpectedly."))
                    setJobAborted(this);
                else {
                    setJobFailed(this, null, Job.ERROR_TYPE_EXECUTION_FAILED);
                }}
            );
    }

    protected void addDownloadLinks(Job j) {
        Export export = ((Export) j); //TODO: Use this instance once scheduler is fixed
        String emrSuffix = isEmrTransformation() ? EMRConfig.S3_PATH_SUFFIX : "";

        //Add file statistics and downloadLinks
        Map<String, ExportObject> exportObjects = CService.jobS3Client.scanExportPath(
            CService.jobS3Client.getS3Path(j, false) + emrSuffix);
        export.setExportObjects(exportObjects);

        if (export.getSuperId() != null) {
            //Add exportObjects including fresh download links for persistent base exports
            Map<String, ExportObject> superExportObjects = CService.jobS3Client.scanExportPath(
                CService.jobS3Client.getS3Path(j, true) + emrSuffix);
            export.setSuperExportObjects(superExportObjects);
        }
    }

    private static class EMRConfig {
        public static final String APPLICATION_ID = System.getenv("EMR_APPLICATION_ID");
        public static final String EMR_RUNTIME_ROLE_ARN = System.getenv("EMR_RUNTIME_ROLE_ARN");
        public static final String JAR_PATH = System.getenv("EMR_JAR_PATH");
        public static final String SPARK_PARAMS = "--class com.here.xyz.FeatureAggregator " +
            "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory";
        public static final String S3_PATH_SUFFIX = "-transformed";
    }

    private EMRManager getEmrManager() {
        if (emrManager == null)
            emrManager = new EMRManager();
        return emrManager;
    }

    @Deprecated
    public boolean isEmrTransformation() {
        return emrTransformation;
    }

    @Deprecated
    public void setEmrTransformation(boolean emrTransformation) {
        this.emrTransformation = emrTransformation;
    }

    @Deprecated
    public Export withEmrTransformation(boolean emrTransformation) {
        setEmrTransformation(emrTransformation);
        return this;
    }

    @Deprecated
    public String getEmrType() {
        return emrType;
    }

    @Deprecated
    public void setEmrType(String emrType) {
        this.emrType = emrType;
    }
    @Deprecated
    public Export withEmrType(String emrType) {
        setEmrType(emrType);
        return this;
    }

    @Deprecated
    public String getEmrJobId() {
        return emrJobId;
    }

    @Deprecated
    public void setEmrJobId(String emrJobId) {
        this.emrJobId = emrJobId;
    }

    @Override
    public Future<Job> executeAbort() {
        if (isEmrTransformation() && getStatus() == finalizing)
            //Cancel EMR Job
            getEmrManager().shutdown(EMRConfig.APPLICATION_ID, emrJobId);
        return super.executeAbort();
    }

    @Override
    public void finalizeJob() {
        if (isEmrTransformation()) {
            updateJobStatus(this, finalizing)
                .compose(job -> {
                    //Start EMR Job, return jobId
                    String sourceS3Url = getS3UrlForPath(CService.jobS3Client.getS3Path(job));
                    List<String> scriptParams = new ArrayList<>();
                    scriptParams.add(sourceS3Url);
                    scriptParams.add(sourceS3Url + EMRConfig.S3_PATH_SUFFIX);
                    scriptParams.add("--type=" + getEmrType());

                    String emrJobId = getEmrManager().startJob(EMRConfig.APPLICATION_ID, job.getId(), EMRConfig.EMR_RUNTIME_ROLE_ARN,
                        EMRConfig.JAR_PATH, scriptParams, EMRConfig.SPARK_PARAMS);
                    this.emrJobId = emrJobId;
                    return Future.succeededFuture(emrJobId);
                })
                .onSuccess(emrJobId -> startEmrJobStatePolling(EMRConfig.APPLICATION_ID, emrJobId))
                .onFailure(err -> {
                    logger.warn(getMarker(), "Failure starting finalization. (EMR Transformation)", err);
                    setJobFailed(this, "Error trying to start finalization.", "START_EMR_JOB_FAILED");
                });
        }
        else
            super.finalizeJob();
    }

    private static String getS3UrlForPath(String path) {
        return "s3://" + CService.configuration.JOBS_S3_BUCKET + "/" + path;
    }

    private void startEmrJobStatePolling(String applicationId, String emrJobId) {
        if (emrJobExecuting.compareAndSet(false, true)) {
            new Thread(() -> {
                setUpdatedAt(Core.currentTimeMillis() / 1000l);
                while (emrJobExecuting.get()) {
                    JobRunState jobState = null;
                    try {
                        jobState = getEmrManager().getExecutionSummary(applicationId, emrJobId);
                        setUpdatedAt(Core.currentTimeMillis() / 1000l);
                    }
                    catch (Exception e) {
                        logger.warn("job[{}] Error fetching job state of EMR transformation with emr job id \"{}\"", getId(), emrJobId, e);
                    }
                    switch (jobState) {
                        case SUCCESS:
                            logger.info("job[{}] execution of EMR transformation {} succeeded ", getId(), emrJobId);
                            addDownloadLinks(this);
                            //Update this job's state finally to "finalized"
                            updateJobStatus(this, finalized);
                            //Stop this thread
                            emrJobExecuting.set(false);
                            break;
                        case FAILED:
                        case CANCELLED:
                            logger.warn("job[{}] EMR transformation {} ended with state \"{}\"", getId(), emrJobId, jobState);
                            setJobFailed(this, "EMR job " + emrJobId + " ended with state \"" + jobState + "\"", "EMR_JOB_FAILED");
                            //Stop this thread
                            emrJobExecuting.set(false);
                    }
                    //Check if last state update is too long (>60s) ago (timeout or other issue with EMR job)
                    if (getUpdatedAt() < Core.currentTimeMillis() / 1000l - 60) {
                        setJobFailed(this, "No state update from EMR job " + emrJobId + " since more than 60 seconds", "EMR_JOB_TIMEOUT");
                        //Stop this thread
                        emrJobExecuting.set(false);
                    }

                    try {
                        Thread.sleep(3_000);
                    }
                    catch (InterruptedException ignore) {}
                }
            }).start();
        }
    }

    @JsonIgnore
    public String getHashForPersistentStorage() {
        return Hasher.getHash(
                (targetLevel != null ? targetLevel.toString() : "")
                + maxTilesPerFile
                + partitionKey
                + csvFormat
                + (filters != null && filters.getSpatialFilter() != null ? filters.getSpatialFilter().geometry.getJTSGeometry().hashCode() : "")
                + (filters != null && filters.getPropertyFilter() != null ? filters.getPropertyFilter().hashCode() : "")
                + (filters != null && filters.getSpatialFilter() != null ? filters.getSpatialFilter().getRadius() : "")
                + (filters != null && filters.getSpatialFilter() != null ? filters.getSpatialFilter().isClipped() : false));
    }

    public enum CompositeMode {
        FULL_OPTIMIZED, //Load persistent Base + (Changes)
        CHANGES, //Only changes
        DEACTIVATED;

        public static CompositeMode of(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }
}
