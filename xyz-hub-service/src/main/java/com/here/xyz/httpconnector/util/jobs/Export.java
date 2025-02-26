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

package com.here.xyz.httpconnector.util.jobs;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.httpconnector.util.Futures.futurify;
import static com.here.xyz.httpconnector.util.emr.config.Step.ReadFeaturesCSV.CsvColumns.JSON_DATA;
import static com.here.xyz.httpconnector.util.emr.config.Step.ReadFeaturesCSV.CsvColumns.WKB;
import static com.here.xyz.httpconnector.util.jobs.Export.CompositeMode.CHANGES;
import static com.here.xyz.httpconnector.util.jobs.Export.CompositeMode.DEACTIVATED;
import static com.here.xyz.httpconnector.util.jobs.Export.CompositeMode.FULL_OPTIMIZED;
import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.DOWNLOAD;
import static com.here.xyz.httpconnector.util.jobs.Export.ExportTarget.Type.VML;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONED_JSON_WKB;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.PARTITIONID_FC_B64;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.TILEID_FC_B64;
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.collect.ImmutableList;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.config.JDBCExporter;
import com.here.xyz.httpconnector.util.emr.EMRManager;
import com.here.xyz.httpconnector.util.emr.config.EmrConfig;
import com.here.xyz.httpconnector.util.emr.config.Step.ConvertToGeoparquet;
import com.here.xyz.httpconnector.util.emr.config.Step.ReadFeaturesCSV;
import com.here.xyz.httpconnector.util.emr.config.Step.ReplaceWkbWithGeo;
import com.here.xyz.httpconnector.util.emr.config.Step.WriteGeoparquet;
import com.here.xyz.httpconnector.util.web.LegacyHubWebClient;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileBasedTarget;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Identifiable;
import com.here.xyz.jobs.datasets.VersionedSource;
import com.here.xyz.jobs.datasets.files.Csv;
import com.here.xyz.jobs.datasets.files.FileFormat;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.jobs.datasets.files.GeoParquet;
import com.here.xyz.jobs.datasets.filters.Filters;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse.PropertyStatistics;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    @JsonIgnore
    private Map<String,ExportObject> exportObjects;

    @JsonIgnore
    private Map<String,ExportObject> superExportObjects;

    @JsonView({Public.class})
    private ExportStatistic statistic;

    @JsonView({Public.class})
    private ExportStatistic superStatistic;

    @JsonView({Public.class})
    private String superId;

    @JsonIgnore
    private Export superJob;

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
    private AtomicReference<Future<Void>> emrJobExecutionFuture = new AtomicReference<>();
    public String emrJobId;
    @JsonIgnore
    private EMRManager emrManager;
    private boolean emrTransformation;
    private String emrType;

    @JsonView({Static.class})
    private String s3Key;
    private static final long UNKNOWN_MAX_SPACE_VERSION = -42;
    @JsonView(Public.class)
    private long maxSpaceVersion = UNKNOWN_MAX_SPACE_VERSION;

    @JsonView(Public.class)
    private long minSpaceVersion = UNKNOWN_MAX_SPACE_VERSION;
    @JsonView(Public.class)
    private long spaceCreatedAt = 0;

    @JsonView(Public.class)
    private long maxSuperSpaceVersion = UNKNOWN_MAX_SPACE_VERSION;

    private static String PARAM_COMPOSITE_MODE = "compositeMode";
    private static String PARAM_PERSIST_EXPORT = "persistExport";
    private static String PARAM_VERSIONS_TO_KEEP = "versionsToKeep";
    private static String PARAM_ENABLE_HASHED_SPACEID = "enableHashedSpaceId";
    private static String PARAM_RUN_AS_ID = "runAsId";
    private static String PARAM_SCOPE = "scope";
    private static String PARAM_EXTENDS = "extends";
    private static String PARAM_SKIP_TRIGGER = "skipTrigger";
    private static String PARAM_INCREMENTAL_MODE = "incrementalMode";

    public static String PARAM_CONTEXT = "context";

    public Export() {
        super();
    }

    public static CSVFormat toBWCFormat(Csv csv) {
      return csv.isAddPartitionKey() ? PARTITIONED_JSON_WKB : JSON_WKB;
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
                if (getExportTarget() != null && getExportTarget().getType() == VML
                    && (getCsvFormat() == null || ( getCsvFormat() != PARTITIONID_FC_B64 && getCsvFormat() != PARTITIONED_JSON_WKB ))) {
                    setCsvFormat(TILEID_FC_B64);
                    if (getMaxTilesPerFile() == 0)
                        setMaxTilesPerFile(VML_EXPORT_MAX_TILES_PER_FILE);
                }

                //Set Composite related defaults
                CompositeMode compositeMode = readParamCompositeMode();
                SpaceContext context = readParamContext();

                Map ext = readParamExtends();

                if (readPersistExport())
                    setKeepUntil(-1);

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

                    if (GEOJSON.equals(csvFormat) || context == EXTENSION || context == SUPER)
                        params.remove(PARAM_COMPOSITE_MODE);
                }else{
                    //Only make sense in case of extended space
                    params.remove(PARAM_COMPOSITE_MODE);
                }

                if (readParamCompositeMode() == FULL_OPTIMIZED && exportTarget != null && DOWNLOAD.equals(exportTarget.type)) {
                    //Override Target-Format to PARTITIONED_JSON_WKB
                    csvFormat = PARTITIONED_JSON_WKB;
                    //if (getTarget() instanceof FileBasedTarget<?> fbt)
                    //    fbt.getOutputSettings().setFormat(PARTITIONED_JSON_WKB);
                    //TODO: Also set the new format & partitioning if type is Files
                    if (targetLevel == null)
                        targetLevel = 12;
                }

                if (readParamExtends() != null && context == null)
                    addParam(PARAM_CONTEXT, DEFAULT);

                if (getSource() instanceof VersionedSource<?> versionRefSource
                    && getSource() instanceof Identifiable<?> identifiable
                    && versionRefSource.getVersionRef() != null
                    && versionRefSource.getVersionRef().isTag()) {
                  final Ref ref = versionRefSource.getVersionRef();
                  try {
                    long version = CService.hubWebClient.loadTag(identifiable.getId(), ref.getTag()).getVersion();
                    if (version >= 0) {
                      versionRefSource.setVersionRef(new Ref(version));
                      setTargetVersion(String.valueOf(version));
                    } 
                    else if(readParamExtends() == null) // non composite tag on empty -> finalize job
                    {
                     setStatistic( new ExportStatistic().withBytesUploaded(0).withFilesUploaded(0).withRowsUploaded(0) );    
                     setTargetVersion("-1");   
                     setErrorDescription("tag on no-data");
                     setErrorType("no_operation");
                     setStatus(finalized);
                    }
                    else // for composite 
                    {
                     versionRefSource.setVersionRef(new Ref(0));
                     setTargetVersion("0");   
                    }
                  }
                  catch (WebClientException e) {
                    return Future.failedFuture(e);
                  }
                }

                if (getSource() instanceof Identifiable<?> identifiable) {

                  try {
                    Space space = CService.hubWebClient.loadSpace(identifiable.getId());
                    setSpaceCreatedAt( space.getCreatedAt() );
                  }
                  catch (WebClientException e) {
                    return Future.failedFuture(e);
                  }
                }


                return Future.succeededFuture();
            }).compose(f -> {
                String superSpaceId = extractSuperSpaceId();

                if(superSpaceId != null) {
                    return LegacyHubWebClient.getSpaceStatistics(superSpaceId, null)
                        .compose(statistics -> {
                            //Set version of base space
                            setMaxSuperSpaceVersion(statistics.getMaxVersion().getValue());
                            return Future.succeededFuture();
                        });
                }else
                    return Future.succeededFuture();
            }).compose(f -> {
                SpaceContext ctx = (   readParamContext() == EXTENSION
                        || readParamCompositeMode() == CHANGES
                        || readParamCompositeMode() == FULL_OPTIMIZED
                        ? EXTENSION : null
                );
                return LegacyHubWebClient.getSpaceStatistics(getTargetSpaceId(), ctx)
                        .compose(statistics ->{
                            //Set version of target space
                            
                            setMaxSpaceVersion(statistics.getMaxVersion().getValue());
                            setMinSpaceVersion(statistics.getMinVersion().getValue());
                            return Future.succeededFuture(statistics);
                        });
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
                    }
                    catch (Exception e){
                        throw new HttpException(BAD_REQUEST, "Cant parse filter geometry!");
                    }
                }
            }
        }

        if (getEstimatedFeatureCount() > 1000000 //searchable limit without index
            && getPartitionKey() != null && !"id".equals(getPartitionKey()) && !"tileid".equals(getPartitionKey())
            && (searchableProperties == null || !searchableProperties.containsKey(getPartitionKey().replaceFirst("^(p|properties)\\." ,""))))
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
        return this.statistic;
    }

    @JsonIgnore
    public ExportStatistic getTotalStatistic() {
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

    @JsonIgnore
    public Export getSuperJob() {
        return superJob;
    }

    @JsonIgnore
    public void setSuperJob(Export superJob) {
        this.superJob = superJob;
    }

    public Export withSuperJob(Export superJob) {
        setSuperJob(superJob);
        return this;
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

    @JsonIgnore
    public Map<String,ExportObject> getSuperExportObjects() {
        if (CService.jobS3Client == null) //If being used as a model on the client side
            return this.superExportObjects == null ? Collections.emptyMap() : this.superExportObjects;

        Map<String, ExportObject> superExportObjects = null;

        if (getSuperId() != null && !readPersistExport())
            superExportObjects = getSuperJob() != null ? getSuperJob().getExportObjects() : null;

        return superExportObjects == null ? Collections.emptyMap() : superExportObjects;
    }

    @JsonProperty
    public void setSuperExportObjects(Map<String, ExportObject> superExportObjects) {
        this.superExportObjects = superExportObjects;
    }

    @JsonIgnore
    public List<ExportObject> getExportObjectsAsList() {
        List<ExportObject> exportObjectList = new ArrayList<>();

        if (getSuperExportObjects() != null)
            exportObjectList.addAll(getSuperExportObjects().values());
        if (getExportObjects() != null)
            exportObjectList.addAll(getExportObjects().values());

        return exportObjectList;
    }

    @JsonIgnore
    public Map<String,ExportObject> getExportObjects() {
        if (CService.jobS3Client == null) //If being used as a model on the client side
            return exportObjects == null ? Collections.emptyMap() : exportObjects;
        if (exportObjects != null && !exportObjects.isEmpty())
            return exportObjects;

        Map<String, ExportObject> exportObjects = null;

        boolean isPersistExport = getSuperId() != null && readPersistExport();
        boolean isEmptyGeoparquet = getSuperId() != null && getStatus() == finalized && "geoparquet".equals(getEmrType()) && getStatistic().rowsUploaded == 0;

        if (isPersistExport  || isEmptyGeoparquet)
            return getSuperJob() != null ? getSuperJob().getExportObjects() : Collections.emptyMap();
        else if (getS3Key() != null) {
            if (getStatus() == finalized)
                exportObjects = CService.jobS3Client.scanExportPathCached(getS3Key());
            else
                exportObjects = CService.jobS3Client.scanExportPath(getS3Key());
        }

        return exportObjects == null ? Collections.emptyMap() : exportObjects;
    }
    
    @JsonProperty
    public void setExportObjects(Map<String, ExportObject> exportObjects) {
        this.exportObjects = exportObjects;
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
        if (target instanceof FileBasedTarget fbt) {
            final FileOutputSettings os = fbt.getOutputSettings();

            setTargetLevel(os.getTileLevel());
            setClipped(os.isClipped());
            setMaxTilesPerFile(os.getMaxTilesPerFile());
            setPartitionKey(os.getPartitionKey());

            setExportTarget(new ExportTarget().withType(DOWNLOAD));
            //setCsvFormat(fbt.getOutputSettings().getFormat());

            if (getCsvFormat() == null) {
                if (os.getFormat() instanceof GeoJson)
                    setCsvFormat(GEOJSON);
                else if (os.getFormat() instanceof GeoParquet) {
                    setCsvFormat(JSON_WKB);
                    setEmrTransformation(true);
                    setEmrType("geoparquet");
                    setPartitionKey("id");
                    os.setPartitionKey("id");
                }
                else if (os.getFormat() instanceof Csv csv)
                    setCsvFormat(toBWCFormat(csv));
            }
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
        return params != null && params.containsKey(PARAM_CONTEXT) ? SpaceContext.valueOf(this.params.get(PARAM_CONTEXT).toString()) : null;
    }

/* incremental */

    public int readParamVersionsToKeep() {
     return params != null && params.containsKey(PARAM_VERSIONS_TO_KEEP) ? (int) this.getParam(PARAM_VERSIONS_TO_KEEP) : 1;
    }

    @JsonIgnore
    public boolean isIncrementalMode() { 
     return params != null && params.containsKey(PARAM_INCREMENTAL_MODE);
    }

    public void setIncrementalValid() { 
     addParam(PARAM_INCREMENTAL_MODE, IncrementalMode.VALID); 
    }

    public void setIncrementalInvalid() { 
     addParam(PARAM_INCREMENTAL_MODE, IncrementalMode.INVALID); 
    }
    
    @JsonIgnore
    public boolean isIncrementalValid() { 
     return isIncrementalMode() && (IncrementalMode.VALID == IncrementalMode.valueOf( params.get(PARAM_INCREMENTAL_MODE).toString()));
    }

    @JsonIgnore
    public boolean isIncrementalInvalid() { 
     return isIncrementalMode() && (IncrementalMode.INVALID == IncrementalMode.valueOf( params.get(PARAM_INCREMENTAL_MODE).toString()));
    }
/* incremental */

    public CompositeMode readParamCompositeMode() {
        return params != null && params.containsKey(PARAM_COMPOSITE_MODE)
                ? CompositeMode.valueOf(params.get(PARAM_COMPOSITE_MODE).toString())
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

        public ExportStatistic addRows(long rows){
            rowsUploaded += rows;
            return this;
        }
        public ExportStatistic addBytes(long bytes){
            bytesUploaded += bytes;
            return this;
        }
        public ExportStatistic addFiles(long files){
            filesUploaded += files;
            return this;
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
        if (readPersistExport())
            return checkPersistentExport();
        else if (isSuperSpacePersist() && readParamCompositeMode().equals(CompositeMode.FULL_OPTIMIZED))
          return checkPersistentSuperExport();
        //No indication for persistent exports are given - proceed normal
        return updateJobStatus(this, prepared);
    }

    private Future<Export> searchPersistentJobOnTarget(String targetId, CSVFormat csvFormat, long targetVersion){
        return CService.jobConfigClient.getList(getMarker(), null , null, targetId)
            //Sort the candidates in reverse order by updated TS to get the oldest candidate
            .map(jobs -> jobs.stream().sorted(Comparator.comparingLong(Job::getUpdatedAt)).toList())
            .map(sortedJobs -> {
              for (Job<?> job : sortedJobs) {
                // filter out non Export jobs
                if (!(job instanceof Export exportJob)) continue;

                // filter out non persistent
                if (exportJob.getKeepUntil() >= 0) continue;

                // filter out the non-matching hash
                if (!exportJob.getHashForPersistentStorage(null).equals(getHashForPersistentStorage(csvFormat))) continue;

                // filter out the ones with different EMR configuration
                if (exportJob.isEmrTransformation() != isEmrTransformation()
                    || !Objects.equals(exportJob.getEmrType(), getEmrType())) continue;

                // filter out the ones with different max space version
                if (exportJob.getMaxSpaceVersion() != targetVersion) continue;

                // try to find a finalized one - doesn't matter if it's the original export
                if (exportJob.getStatus().equals(finalized)
                    || exportJob.getStatus().equals(trigger_executed)
                    || (exportJob.getStatus().equals(failed) && exportJob.getId().equalsIgnoreCase(getId() + "_missing_base"))) {
                  return Optional.of(exportJob);
                }
              }
              return Optional.<Export>empty();
            })
            // log the result
            .map(candidate -> candidate.map(job -> {
              final String logMessage = job.getStatus().equals(failed) ? "Spawned job has failed {}:{} " : "Found existing persistent job {}:{} ";
              logger.info(getMarker(), "job[{}] " + logMessage, getId(), job.getId(), job.getStatus());
              return job;
            }).orElse(null));
    }

    public Future<Job> checkPersistentExport() {
        //Deliver result if Export is already available
        return searchPersistentJobOnTarget(targetSpaceId, null, getMaxSpaceVersion())
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
                        setSuperJob(existingJob);
                        setStatistic(existingJob.getStatistic());

                        return updateJobStatus(this, finalized);
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

        return searchPersistentJobOnTarget(superSpaceId, JSON_WKB, getMaxSuperSpaceVersion())
            .compose(existingJob -> {
                if (existingJob == null) {
                    logger.info("job[{}] Persist Export {} of Base-Layer is missing -> starting one!", getId(), superSpaceId);

                    Export baseExport = new Export()
                        .withId(getId() + "_missing_base")
                        .withDescription("Persistent Base Export for " + getId())
                        .withExportTarget(new ExportTarget().withType(DOWNLOAD))
                        .withFilters(getFilters())
                        .withMaxTilesPerFile(getMaxTilesPerFile())
                        .withTargetLevel(getTargetLevel())
                        .withPartitionKey(getPartitionKey())
                            //always use JSON_WKB
                        .withCsvFormat(JSON_WKB)
                        .withEmrTransformation(isEmrTransformation())
                        .withEmrType(getEmrType());


                    //We only need reusable data on S3
                    baseExport.addParam(PARAM_SKIP_TRIGGER, true);
                    baseExport.addParam(PARAM_PERSIST_EXPORT, true);

                    logger.info("job[{}] Trigger Persist Export {} of Super-Layer!", getId(), superSpaceId);
                    return LegacyHubWebClient.performBaseLayerExport(superSpaceId, baseExport)
                        .compose(newBaseExport -> {
                            logger.info("job[{}] Need to wait for finalization of persist Export {} of base-layer!", getId(), newBaseExport.getId());
                            setSuperId(newBaseExport.getId());
                            setSuperJob((Export) newBaseExport);

                            return updateJobStatus(this,queued);
                        })
                        .onFailure(t -> {
                            logger.error(t);
                            setJobFailed(this, ERROR_DESCRIPTION_PERSISTENT_EXPORT_FAILED, ERROR_TYPE_EXECUTION_FAILED);
                        });
                }
                else if (existingJob.getStatus() == failed) {
                    logger.info("job[{}] Persist Export {} of Super-Layer has failed!", getId(), superSpaceId);
                    return setJobFailed(this,ERROR_DESCRIPTION_PERSISTENT_EXPORT_FAILED, ERROR_TYPE_EXECUTION_FAILED);
                }
                else if (existingJob.getStatus() == finalized) {
                    logger.info("job[{}] Persist Export {} of Super-Layer is available!", getId(), superSpaceId);
                    if (superId == null)
                        setSuperId(existingJob.getId());

                    setSuperJob(existingJob);
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

        JDBCExporter.getInstance().executeExport(this, CService.configuration.JOBS_S3_BUCKET,
                CService.jobS3Client.getS3Path(this), CService.configuration.JOBS_REGION)
            .onSuccess(statistic -> {
                    //Everything is processed
                    logger.info("job[{}] Export of '{}' completely succeeded!", getId(), getTargetSpaceId());
                    addStatistic(statistic);
                    setS3Key(CService.jobS3Client.getS3Path(this));
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

    private static class EMRConfig {
        public static final String APPLICATION_ID = System.getenv("EMR_APPLICATION_ID");
        public static final String EMR_RUNTIME_ROLE_ARN = System.getenv("EMR_RUNTIME_ROLE_ARN");
        public static final String JAR_PATH = System.getenv("EMR_JAR_PATH");
        public static final String SPARK_PARAMS = System.getenv("EMR_SPARK_PARAMS");
        public static final String S3_PATH_SUFFIX = "-transformed";
    }

    private EMRManager getEmrManager() {
        if (emrManager == null)
            emrManager = EMRManager.getInstance();
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

    public String getS3Key() {
        return s3Key == null || s3Key.endsWith("/") ? s3Key : (s3Key + "/");
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public Export withS3Key(String s3Key) {
        setS3Key(s3Key);
        return this;
    }

    public long getMaxSpaceVersion() {
        return maxSpaceVersion;
    }

    public void setMaxSpaceVersion(long maxSpaceVersion) {
        this.maxSpaceVersion = maxSpaceVersion;
    }

    public Export withMaxSpaceVersion(long maxSpaceVersion) {
        setMaxSpaceVersion(maxSpaceVersion);
        return this;
    }

    public long getMaxSuperSpaceVersion() {
        return maxSuperSpaceVersion;
    }

    public void setMaxSuperSpaceVersion(long maxSuperSpaceVersion) {
        this.maxSuperSpaceVersion = maxSuperSpaceVersion;
    }

    public Export withMaxSuperSpaceVersion(long maxSuperSpaceVersion) {
        setMaxSuperSpaceVersion(maxSuperSpaceVersion);
        return this;
    }

    public long getMinSpaceVersion() {
        return minSpaceVersion;
    }

    public void setMinSpaceVersion(long minSpaceVersion) {
        this.minSpaceVersion = minSpaceVersion;
    }

    public Export withMinSpaceVersion(long minSpaceVersion) {
        setMinSpaceVersion(minSpaceVersion);
        return this;
    }

    public long getSpaceCreatedAt() {
        return spaceCreatedAt;
    }

    public void setSpaceCreatedAt(long spaceCreatedAt) {
        this.spaceCreatedAt = spaceCreatedAt;
    }

    public Export withSpaceCreatedAt(long spaceCreatedAt) {
        setSpaceCreatedAt(spaceCreatedAt);
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

    private long targetToVersion( String targetVersion )
    {
     if( targetVersion == null ) return -1;
     String[] s = targetVersion.split("\\.\\.");
     return Long.parseLong( (s.length == 1 ? s[0] : s[1]) );
    }

    @Override
    public Future<Job> prepareStart() {

      String srcKey = (getSource() != null ? getSource().getKey() : getTargetSpaceId() ); // when legacy export used

      long targetVersion = targetToVersion( getTargetVersion() );

      Future<Tag> pushVersionTag = ( targetVersion < 0 )
       ? Future.succeededFuture()
       : CService.hubWebClient.postTagAsync( srcKey, new Tag().withId(getId()).withVersion( targetVersion ).withSystem(true) );

      return pushVersionTag.compose( v -> super.prepareStart() );
    }

    @Override
    public Future<Void> finalizeJob() {

        String srcKey = (getSource() != null ? getSource().getKey() : getTargetSpaceId() ); // when legacy export used

        Future<Void> deleteVersionTag = ( getTargetVersion() == null )
        ? Future.succeededFuture()
        : CService.hubWebClient.deleteTagAsync( srcKey, getId() ).compose( tag -> Future.succeededFuture());

        return finalizeJob(true).compose( v -> deleteVersionTag  );
    }

    protected Future<Void> finalizeJob(boolean finalizeAfterCompletion) {
        if (isEmrTransformation() && !getExportObjects().isEmpty() && (statistic == null || statistic.getRowsUploaded() > 0)) {
            return updateJobStatus(this, finalizing)
                .compose(job -> startEmrJob(job))
                .onFailure(err -> {
                    logger.warn(getMarker(), "Failure starting finalization. (EMR Transformation)", err);
                    setJobFailed(this, "Error trying to start finalization.", "START_EMR_JOB_FAILED");
                })
                .compose(emrJobId -> startEmrJobStatePolling(EMRConfig.APPLICATION_ID, emrJobId, finalizeAfterCompletion));
        }
        else if (finalizeAfterCompletion)
            return super.finalizeJob();
        else
            return Future.succeededFuture();
    }

    private Future<String> startEmrJob(Job job) {
        final String sourceS3UrlWithoutSlash = getS3UrlForPath(CService.jobS3Client.getS3Path(this));
        String sourceS3Url = sourceS3UrlWithoutSlash + "/";
        String targetS3Url = sourceS3UrlWithoutSlash + EMRConfig.S3_PATH_SUFFIX + "/";

        //Start EMR Job, return jobId
        List<String> scriptParams = new ArrayList<>();
        scriptParams.add(sourceS3Url);
        scriptParams.add(targetS3Url);
        scriptParams.add("--type=" + getEmrType());
        if (readParamCompositeMode() == FULL_OPTIMIZED || isIncrementalValid()) {
          scriptParams.add("--delta");
          if ("geoparquet".equals(getEmrType())) {
            final String sourceSuperS3UrlWithoutSlash = getS3UrlForPath(CService.jobS3Client.getS3Path(getSuperJob()));
            String targetSuperS3Url = sourceSuperS3UrlWithoutSlash + EMRConfig.S3_PATH_SUFFIX + "/";
            scriptParams.add("--baseInputDir=" + targetSuperS3Url);
          }
        }

        String emrJobId = getEmrManager().startJob(EMRConfig.APPLICATION_ID, job.getId(), EMRConfig.EMR_RUNTIME_ROLE_ARN,
            EMRConfig.JAR_PATH, scriptParams, EMRConfig.SPARK_PARAMS);
        this.emrJobId = emrJobId;
        return store().map(emrJobId);
    }

    private String buildEmrJsonConfig(String inputDirectory, String outputDirectory) {
        FileFormat targetFormat = ((FileBasedTarget) getTarget()).getOutputSettings().getFormat();
        if (targetFormat instanceof GeoParquet) {
            return new EmrConfig().withSteps(ImmutableList.of(
                new ReadFeaturesCSV().withInputDirectory(inputDirectory).withColumns(ImmutableList.of(
                    JSON_DATA, WKB
                )),
                new ReplaceWkbWithGeo(),
                new ConvertToGeoparquet(),
                new WriteGeoparquet().withOutputDirectory(outputDirectory)
            )).serialize();
        }
        else if (targetFormat instanceof Csv) {

        }
        throw new IllegalArgumentException("Unsupported file format: " + targetFormat);
    }

    private static String getS3UrlForPath(String path) {
        return "s3://" + CService.configuration.JOBS_S3_BUCKET + "/" + path;
    }

    private Future<Void> startEmrJobStatePolling(String applicationId, String emrJobId, boolean finalizeAfterCompletion) {
        final Promise<Void> promise = Promise.promise();
        if (emrJobExecutionFuture.compareAndSet(null, promise.future())) {
            new Thread(() -> {
                setUpdatedAt(Core.currentTimeMillis() / 1000l);
                while (emrJobExecutionFuture.get() != null) {
                    try {
                        String jobState = getEmrManager().getExecutionSummary(applicationId, emrJobId);
                        setUpdatedAt(Core.currentTimeMillis() / 1000l);

                        switch (jobState) {
                            case "SUCCESS":
                                logger.info("job[{}] execution of EMR transformation {} succeeded ", getId(), emrJobId);
                                setS3Key(CService.jobS3Client.getS3Path(this) + EMRConfig.S3_PATH_SUFFIX);
                                //Update this job's state finally to "finalized"
                                if (finalizeAfterCompletion)
                                    updateJobStatus(this, finalized);
                                //Stop this thread
                                completePollingThread(promise);
                                break;
                            case "FAILED":
                            case "CANCELLED":
                                logger.warn("job[{}] EMR transformation {} ended with state \"{}\"", getId(), emrJobId, jobState);
                                final String errorMessage = "EMR job " + emrJobId + " ended with state \"" + jobState + "\"";
                                setJobFailed(this, errorMessage, "EMR_JOB_FAILED");
                                //Stop this thread
                                failPollingThread(promise, errorMessage);
                        }
                    }
                    catch (Exception e) {
                        logger.warn("job[{}] Error fetching job state of EMR transformation with emr job id \"{}\"", getId(), emrJobId, e);
                    }

                    //Check if last state update is too long (>60s) ago (timeout or other issue with EMR job)
                    if (getUpdatedAt() < Core.currentTimeMillis() / 1000l - 60) {
                        final String errorMessage = "No state update from EMR job " + emrJobId + " since more than 60 seconds";
                        setJobFailed(this, errorMessage, "EMR_JOB_TIMEOUT");
                        //Stop this thread
                        failPollingThread(promise, errorMessage);
                    }

                    try {
                        Thread.sleep(3_000);
                    }
                    catch (InterruptedException ignore) {}
                }
            }).start();
            return emrJobExecutionFuture.get();
        }
        else
            //Return the future of the other process which is actually performing the polling
            //TODO: Throw some kind of ConcurrencyException instead?
            return emrJobExecutionFuture.get();
    }

    private void completePollingThread(Promise<Void> promise) {
        emrJobExecutionFuture.set(null);
        promise.complete();
    }

    private void failPollingThread(Promise<Void> promise, String errorMessage) {
        emrJobExecutionFuture.set(null);
        promise.fail(errorMessage);
    }

    @JsonIgnore
    public String getHashForPersistentStorage(CSVFormat targetCSVFormat) {
        return Hasher.getHash(
                (targetLevel != null ? targetLevel.toString() : "")
                + maxTilesPerFile
                + partitionKey
                + (targetCSVFormat == null ? csvFormat : targetCSVFormat)
                + (filters != null && filters.getSpatialFilter() != null ? filters.getSpatialFilter().getGeometry().getJTSGeometry().hashCode() : "")
                + (filters != null && filters.getPropertyFilter() != null ? filters.getPropertyFilter().hashCode() : "")
                + (filters != null && filters.getSpatialFilter() != null ? filters.getSpatialFilter().getRadius() : "")
                + (filters != null && filters.getSpatialFilter() != null && filters.getSpatialFilter().isClipped()));
    }

    public enum CompositeMode {
        FULL_OPTIMIZED, //Load persistent Base + (Changes)
        CHANGES, //Only changes
        FULL, //context=default
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

    public enum IncrementalMode {

        VALID,
        INVALID;

        public static IncrementalMode of(String value) {
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
