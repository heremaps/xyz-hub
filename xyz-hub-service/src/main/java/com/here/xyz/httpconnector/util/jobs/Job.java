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

import static com.here.xyz.httpconnector.util.jobs.Export.PARAM_CONTEXT;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.aborted;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.collecting_trigger_status;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.executed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.executing;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.executing_trigger;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.failed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalized;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.finalizing;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.prepared;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.preparing;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.queued;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.trigger_executed;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.trigger_status_collected;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.validated;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.validating;
import static com.here.xyz.httpconnector.util.jobs.Job.Status.waiting;
import static com.here.xyz.httpconnector.util.jobs.RuntimeStatus.State.CANCELLED;
import static com.here.xyz.httpconnector.util.jobs.RuntimeStatus.State.FAILED;
import static com.here.xyz.httpconnector.util.jobs.RuntimeStatus.State.PENDING;
import static com.here.xyz.httpconnector.util.jobs.RuntimeStatus.State.RUNNING;
import static com.here.xyz.httpconnector.util.jobs.RuntimeStatus.State.SUBMITTED;
import static com.here.xyz.httpconnector.util.jobs.RuntimeStatus.State.SUCCEEDED;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.addJob;
import static com.here.xyz.httpconnector.util.scheduler.JobQueue.updateJobStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Payload;
import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.task.StatusHandler;
import com.here.xyz.httpconnector.util.jobs.RuntimeStatus.State;
import com.here.xyz.httpconnector.util.web.LegacyHubWebClient;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

@JsonSubTypes({
        @JsonSubTypes.Type(value = Import.class, name = "Import"),
        @JsonSubTypes.Type(value = Export.class, name = "Export"),
        @JsonSubTypes.Type(value = CombinedJob.class, name = "CombinedJob")
})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public abstract class Job<T extends Job> extends Payload {
    public static String ERROR_TYPE_VALIDATION_FAILED = "validation_failed";
    public static String ERROR_TYPE_PREPARATION_FAILED = "preparation_failed";
    public static String ERROR_TYPE_EXECUTION_FAILED = "execution_failed";
    public static String ERROR_TYPE_FINALIZATION_FAILED = "finalization_failed";
    public static String ERROR_TYPE_FAILED_DUE_RESTART = "failed_due_node_restart";
    public static String ERROR_TYPE_ABORTED = "aborted";
    private static final Logger logger = LogManager.getLogger();
    private Marker marker;

    /**
     * The creation timestamp.
     */
    @JsonView({Public.class})
    private long createdAt = Core.currentTimeMillis() / 1000l;

    /**
     * The last update timestamp.
     */
    @JsonView({Public.class})
    private long updatedAt = createdAt;

    /**
     * The timestamp which indicates when the execution began.
     */
    @JsonView({Public.class})
    private long executedAt = -1;

    /**
     * The timestamp at which time the finalization is completed.
     */
    @JsonView({Public.class})
    private long finalizedAt = -1;

    /**
     * The timestamp (in milliseconds) of when this job will expire.
     * When a job expires, it will be deleted from the system. Also, all the stored data associated with it will be deleted and will not
     * be accessible after that date.
     * Set this value to -1 to keep a job and its data forever.
     */
    @JsonView({Public.class})
    private long keepUntil = System.currentTimeMillis() + 14 * 24 * 60 * 60_000; //Default is 2 weeks

    /**
     * The job ID
     */
    @JsonView({Public.class})
    private String id;

    @JsonView({Public.class})
    protected String description;

    @JsonView({Internal.class})
    protected String targetSpaceId;

    @JsonView({Internal.class})
    protected String targetTable;

    @JsonView({Public.class})
    private String errorDescription;

    @JsonView({Public.class})
    private String errorType;

    /**
     * The status of this job. The flow for a job consists of several phases. Each phase corresponds to a status.
     * A newly created Job has status "waiting", so it's waiting to be executed.
     */
    @JsonView({Public.class, Static.class})
    private Status status;

    @JsonView({Public.class})
    private Status lastStatus;

    @JsonView({Public.class})
    protected CSVFormat csvFormat;

    @JsonView({Public.class})
    protected Strategy strategy;

    @JsonView({Internal.class})
    private String targetConnector;

    @JsonView({Internal.class})
    private long spaceVersion;

    @JsonView({Internal.class})
    private String author;

    @JsonView({Public.class})
    protected Boolean clipped;

    @JsonView({Public.class})
    protected Boolean omitOnNull;

    /**
     * Arbitrary parameters to be provided from hub
     */
    @JsonView({Internal.class})
    protected Map<String, Object> params;

    private DatasetDescription source;
    private DatasetDescription target;
    @Deprecated
    @JsonIgnore
    private boolean childJob;
    private RuntimeStatus runtimeStatus = new RuntimeStatus();

    public Job() {}

    public Job(String description, String targetSpaceId, String targetTable, Strategy strategy) {
        setDescription(description);
        setTargetSpaceId(targetSpaceId);
        setTargetTable(targetTable);
        setStrategy(strategy);
    }

    public abstract Future<T> init();

    protected static String generateRandomId() {
        return RandomStringUtils.randomAlphabetic(1) + RandomStringUtils.randomAlphanumeric(5);
    }

    public Future<Job> injectConfigValues() {
        return readEnableHashedSpaceId()
            .compose(enableHashedSpaceId -> {
                addParam("enableHashedSpaceId", enableHashedSpaceId);
                if (getTargetTable() == null)
                    //Only for debugging purpose
                    setTargetTable(enableHashedSpaceId ? Hasher.getHash(getTargetSpaceId()) : getTargetSpaceId());
                return Future.succeededFuture(this);
            });
    }

    private Future<Boolean> readEnableHashedSpaceId() {
        return LegacyHubWebClient.getConnectorConfig(getTargetConnector())
            .compose(connector -> {
                boolean enableHashedSpaceId = connector.params.containsKey("enableHashedSpaceId")
                    ? (boolean) connector.params.get("enableHashedSpaceId") : false;
                return Future.succeededFuture(enableHashedSpaceId);
            });
    }

    public Future<Job> executeAbort() {
      return updateJobStatus(this, aborted);
    }

    public Future<Job> prepareStart() {
        return injectConfigValues()
            .compose(job -> CService.jobConfigClient.update(getMarker(), job));
    }

    public abstract Future<Job> executeStart();

    public Future<Job> executeRetry() {
      try {
        isValidForRetry();
      }
      catch (HttpException e) {
        return Future.failedFuture(e);
      }
      try {
        resetToPreviousState();
      }
      catch (Exception e) {
        return Future.failedFuture(new HttpException(BAD_REQUEST, "Job has no lastStatus - can't retry!"));
      }
      //Add job directly to queue instead executing Start to skip validations.
      return CService.jobConfigClient.update(getMarker(), this)
          .onSuccess(job -> addJob(this));
    }

    /**
     * @deprecated URL creation will be handled automatically at the inputs / outputs of the job
     * @param urlCount
     * @return
     */
    @Deprecated
    public Future<Job> executeCreateUploadUrl(int urlCount) {
      return Future.failedFuture(new HttpException(NOT_IMPLEMENTED, "For Export not available!"));
    }

    @JsonIgnore
    protected Future<T> setDefaults() {
        //TODO: Do field initialization at instance initialization time
        if (getId() == null)
            setId(generateRandomId());
        if (getErrorType() != null)
            setErrorType(null);
        if (getErrorDescription() != null)
            setErrorDescription(null);
        //A newly created Job waits for an execution
        if (getStatus() == null)
            setStatus(Job.Status.waiting);

        return injectSpaceParameter();
    }

    public Future<T> validate() {
        if (getCsvFormat() == null)
            return Future.failedFuture(new HttpException(BAD_REQUEST, "Please specify 'csvFormat'!"));
        return Future.succeededFuture((T) this);
    }

    private Future<T> injectSpaceParameter() {
        if (getTargetSpaceId() == null)
            return Future.failedFuture(new HttpException(BAD_REQUEST, "Please specify 'targetSpaceId'!"));

        return LegacyHubWebClient.getSpace(getTargetSpaceId())
                .compose(space -> {
                    setTargetSpaceId(space.getId());
                    setTargetConnector(space.getStorage().getId());
                    addParam("versionsToKeep",space.getVersionsToKeep());
                    addParam("persistExport", space.isReadOnly());

                    Promise<Map> p = Promise.promise();

                    if (space.getExtension() != null) {
                        /** Resolve Extension */
                        LegacyHubWebClient.getSpace(space.getExtension().getSpaceId())
                                .onSuccess(baseSpace -> {
                                    p.complete(space.resolveCompositeParams(baseSpace));
                                })
                                .onFailure(e -> p.fail(e));
                    }else
                        p.complete(null);
                    return p.future();
                }) .compose(extension -> {
                    Promise<Map> p = Promise.promise();
                    if (extension != null && extension.get("extends") != null  && ((Map)extension.get("extends")).get("extends") != null) {
                        /** Resolve 2nd Level Extension */
                        LegacyHubWebClient.getSpace((String)((Map)((Map)extension.get("extends")).get("extends")).get("spaceId"))
                                .onSuccess(baseSpace -> {
                                    /** Add persistExport flag to Parameters */
                                    Map<String, Object> ext = new HashMap<>();
                                    ext.putAll(extension);
                                    ((Map) ((Map) ext.get("extends")).get("extends")).put("readOnly", baseSpace.isReadOnly());
                                    p.complete(ext);
                                })
                                .onFailure(e -> p.fail(e));
                    }else
                        p.complete(extension);
                    return p.future();
                })
                .compose(extension -> {
                    if(extension != null) {
                        /** Add extends to jobConfig params  */
                        addParam("extends",extension.get("extends"));
                    }
                    return Future.succeededFuture((T) this);
                });
    }

    @JsonIgnore
    protected Future<Job> isValidForStart() {
        if (getStatus() != waiting)
            return Future.failedFuture(new HttpException(PRECONDITION_FAILED, "Invalid state: " + getStatus()
                + " execution is only allowed on status = waiting"));
        return Future.succeededFuture(this);
    }

    @JsonIgnore
    protected void isValidForRetry() throws HttpException {
        if (!getStatus().equals(Status.failed) && !getStatus().equals(Status.aborted))
            throw new HttpException(PRECONDITION_FAILED, "Invalid state: [" + getStatus() + "] for retry!");
    }

    @JsonIgnore
    public boolean isValidForDelete() {
        return getStatus().isFinal() || getStatus() == waiting;
    }


    @JsonProperty("type")
    @JsonInclude
    private String getType() {
        return getClass().getSimpleName();
    }

    public Future<T> store() {
      return CService.jobConfigClient.store(getMarker(), this)
          .map(job -> (T) job);
    }

    @JsonView({Public.class})
    public enum Status {
        waiting, queued, validating, validated, preparing, prepared, executing, executed,
            executing_trigger, trigger_executed, collecting_trigger_status, trigger_status_collected,
            finalizing, finalized(true), aborted(true), failed(true);

        private final boolean isFinal;

        Status() {
            this(false);
        }

        Status(boolean isFinal) {
            this.isFinal = isFinal;
        }

        public boolean isFinal() {
            return isFinal;
        }

        public static Status of(String value) {
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
    public enum CSVFormat {
        GEOJSON, JSON_WKT, JSON_WKB, TILEID_FC_B64, PARTITIONID_FC_B64, PARTITIONED_JSON_WKB;

        public static CSVFormat of(String value) {
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

    @JsonView({Public.class})
    public enum Strategy {
        LASTWINS, SKIPEXISTING, ERROR;

        public static Strategy of(String value) {
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

    public String getId(){
        return id;
    }

    public void setId(final String id){
        this.id = id;
    }

    public T withId(final String id) {
        setId(id);
        return (T) this;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    public void setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public T withErrorDescription(final String errorDescription) {
        setErrorDescription(errorDescription);
        return (T) this;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public T withDescription(final String description) {
        setDescription(description);
        return (T) this;
    }

    /**
     * @deprecated Please use methods {@link #getTarget()} or {@link #getSource()} instead.
     * @return
     */
    @Deprecated
    public String getTargetSpaceId() {
        return targetSpaceId;
    }

    /**
     * @deprecated Please use methods {@link #setTarget(DatasetDescription)} or {@link #setSource(DatasetDescription)} instead.
     * @param targetSpaceId
     */
    @Deprecated
    public void setTargetSpaceId(String targetSpaceId) {
        this.targetSpaceId = targetSpaceId;
        //Keep BWC
//        if (this instanceof Import && target == null)
//            setTarget(new DatasetDescription.Space().withId(targetSpaceId));
//        else if (this instanceof Export && source == null)
//            setSource(new DatasetDescription.Space().withId(targetSpaceId));
    }

    /**
     * @deprecated Please use methods {@link #withTarget(DatasetDescription)} or {@link #withSource(DatasetDescription)} instead.
     * @param targetSpaceId
     * @return
     */
    @Deprecated
    public T withTargetSpaceId(final String targetSpaceId) {
        setTargetSpaceId(targetSpaceId);
        return (T) this;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public T withTargetTable(final String targetTable) {
        setTargetTable(targetTable);
        return (T) this;
    }

    public Status getStatus() { return status; }

    public void setStatus(Job.Status status) {
        if (this.status != null && this.status.isFinal()) {
            if (getFinalizedAt() == -1)
                setFinalizedAt(Core.currentTimeMillis() / 1000l);
            return;
        }
        if ((status == aborted || status == failed) && lastStatus == null)
            lastStatus = this.status;
        this.status = status;
        //Set future-proof state
        getRuntimeStatus().setState(bwcStatusMapping.get(status));
    }

    public T withStatus(final Job.Status status) {
        setStatus(status);
        return (T) this;
    }

    public void resetStatus(Job.Status status) {
        this.status = status;
    }

    public Status getLastStatus() { return lastStatus; }

    public void setLastStatus(Job.Status lastStatus) {
        this.lastStatus = lastStatus;
    }

    public CSVFormat getCsvFormat() {
        return csvFormat;
    }

    public void setCsvFormat(CSVFormat csvFormat) {
        this.csvFormat = csvFormat;
    }

    public T withCsvFormat(CSVFormat csvFormat) {
        setCsvFormat(csvFormat);
        return (T) this;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public T withCsvFormat(Strategy strategy) {
        setStrategy(strategy);
        return (T) this;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final long createdAt) {
        this.createdAt = createdAt;
    }

    public T withCreatedAt(final long createdAt) {
        setCreatedAt(createdAt);
        return (T) this;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(final long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public T withUpdatedAt(final long updatedAt) {
        setUpdatedAt(updatedAt);
        return (T) this;
    }

    public long getExecutedAt() {
        return executedAt;
    }

    public void setExecutedAt(final long executedAt) {
        this.executedAt = executedAt;
    }

    public T withExecutedAt(final long executedAt) {
        setExecutedAt(executedAt);
        return (T) this;
    }

    public long getFinalizedAt() {
        return finalizedAt;
    }

    public void setFinalizedAt(final long finalizedAt) {
        this.finalizedAt = finalizedAt;
    }

    public T withFinalizedAt(final long finalizedAt) {
        setFinalizedAt(finalizedAt);
        return (T) this;
    }

    public Future<Void> finalizeJob() {
        logger.info("job[{}] is finalized!", id);
        return updateJobStatus(this, finalized).mapEmpty();
    }

    public long getKeepUntil() {
        return keepUntil;
    }

    public void setKeepUntil(long keepUntil) {
        this.keepUntil = keepUntil;
    }

    public T withKeepUntil(long keepUntil) {
        setKeepUntil(keepUntil);
        return (T) this;
    }

    public String getTargetConnector() {
        return targetConnector;
    }

    public void setTargetConnector(String targetConnector) {
        this.targetConnector = targetConnector;
    }

    public T withTargetConnector(String targetConnector) {
        setTargetConnector(targetConnector);
        return (T) this;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType){
        this.errorType = errorType;
    }

    public T withErrorType(String errorType) {
        setErrorType(errorType);
        return (T) this;
    }

    public long getSpaceVersion() {
        return spaceVersion;
    }

    public void setSpaceVersion(final long spaceVersion) {
        this.spaceVersion = spaceVersion;
    }

    public T withSpaceVersion(final long spaceVersion) {
        setSpaceVersion(spaceVersion);
        return (T) this;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public T withAuthor(String author) {
        setAuthor(author);
        return (T) this;
    }

    public Boolean getClipped() {
        return clipped;
    }

    public void setClipped(Boolean clipped) {
        this.clipped = clipped;
    }

    public Boolean getOmitOnNull() {
        return omitOnNull;
    }

    public void setOmitOnNull(Boolean omitOnNull) {
        this.omitOnNull = omitOnNull;
    }

    public T withOmitOnNull(final boolean omitOnNull) {
        setOmitOnNull(omitOnNull);
        return (T) this;
    }

    /**
     * @deprecated Please use actual fields instead of params.
     *  Utilization of fields is easier to track than loosely coupled / untyped params in a map.
     */
    @Deprecated
    public Object getParam(String key) {
        if(params == null)
            return null;
        return params.get(key);
    }

    /**
     * @deprecated Please use actual fields instead of params.
     *  Utilization of fields is easier to track than loosely coupled / untyped params in a map.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * @deprecated Please use actual fields instead of params.
     *  Utilization of fields is easier to track than loosely coupled / untyped params in a map.
     * @param params
     */
    @Deprecated
    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    /**
     * @deprecated Please use actual fields instead of params.
     *  Utilization of fields is easier to track than loosely coupled / untyped params in a map.
     * @param params
     */
    public T withParams(Map params) {
        setParams(params);
        return (T) this;
    }

    public DatasetDescription getSource() {
        return source;
    }

    public void setSource(DatasetDescription source) {
        this.source = source;
        //Keep BWC
        if (source instanceof DatasetDescription.Space space) {
            setTargetSpaceId(space.getId());
            if (this instanceof Export export) {

                Ref ref = space.getVersionRef();

                if (ref != null && !ref.isHead() && ( ref.isSingleVersion() || ref.isRange() ) )
                  export.setTargetVersion( ref.toString() );

                export.setFilters(space.getFilters());
                if (export.getFilters() != null) {
                 addParam(PARAM_CONTEXT, export.getFilters().getContext());
                }
            }
        }
    }

    public T withSource(DatasetDescription source) {
        setSource(source);
        return (T) this;
    }

    public DatasetDescription getTarget() {
        return target;
    }

    public void setTarget(DatasetDescription target) {
        this.target = target;
        //Keep BWC
        if (this instanceof Import && target instanceof DatasetDescription.Space space)
            setTargetSpaceId(space.getId());
    }

    public T withTarget(DatasetDescription target) {
        setTarget(target);
        return (T) this;
    }

    @Deprecated
    public boolean isChildJob() {
        return childJob;
    }


    @Deprecated
    public void setChildJob(boolean childJob) {
        this.childJob = childJob;
    }

    /**
     * @deprecated Please use actual fields instead of params.
     *  Utilization of fields is easier to track than loosely coupled / untyped params in a map.
     * @param key
     * @param value
     */
    @Deprecated
    public void addParam(String key, Object value){
        if (this.params == null)
            this.params = new HashMap<>();
        this.params.put(key,value);
    }
    @JsonIgnore
    public void resetToPreviousState() throws Exception {
        switch (getStatus()) {
            case failed:
            case aborted:
                setErrorType(null);
                setErrorDescription(null);
                if (getLastStatus() != null) {
                    //Set to last valid state
                    resetStatus(getLastStatus());
                    setLastStatus(null);
                    resetToPreviousState();
                }
                else
                    throw new Exception("No last Status found!");
                break;
        }
    };

    @JsonIgnore
    public abstract String getQueryIdentifier();

    public Future<Job> isProcessingPossible() {
        if (!needRdsCheck())
            //No RDS check needed - no database intensive stage.
            return Future.succeededFuture(this);
        return isProcessingOnRDSPossible();
    }

    protected Future<Job> isProcessingOnRDSPossible() {
        return StatusHandler.getInstance().getRDSStatus(getTargetConnector())
            .compose(rdsStatus -> {

                if (rdsStatus.getCloudWatchDBClusterMetric(this).getAcuUtilization() > CService.configuration.JOB_MAX_RDS_MAX_ACU_UTILIZATION) {
                    logger.info("job[{}] JOB_MAX_RDS_MAX_ACU_UTILIZATION to high {} > {}", getId(), rdsStatus.getCloudWatchDBClusterMetric(this).getAcuUtilization(), CService.configuration.JOB_MAX_RDS_MAX_ACU_UTILIZATION);
                    return Future.failedFuture(new ProcessingNotPossibleException());
                }

                /** Stop here */
                if(this instanceof Export)
                    return Future.succeededFuture(this);

                //TODO: Move following into according sub-clases
                if (this instanceof Import && rdsStatus.getRdsMetrics().getTotalInflightImportBytes() > CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES) {
                    logger.info("job[{}] JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES to high {} > {}", getId(), rdsStatus.getRdsMetrics().getTotalInflightImportBytes(), CService.configuration.JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES);
                    return Future.failedFuture(new ProcessingNotPossibleException());
                }
                if (this instanceof Import && rdsStatus.getRdsMetrics().getTotalRunningIDXQueries() > CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS) {
                    logger.info("job[{}] JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS to high {} > {}", getId(), rdsStatus.getRdsMetrics().getTotalRunningIDXQueries(), CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS);
                    return Future.failedFuture(new ProcessingNotPossibleException());
                }
                if (this instanceof Import && rdsStatus.getRdsMetrics().getTotalRunningImportQueries() > CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES) {
                    logger.info("job[{}] JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES to high {} > {}", getId(), rdsStatus.getRdsMetrics().getTotalRunningImportQueries(), CService.configuration.JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES);
                    return Future.failedFuture(new ProcessingNotPossibleException());
                }

                return Future.succeededFuture(this);
            });
    }

    public boolean needRdsCheck() {
        return false;
    }

    public abstract void execute();

    public static class Public extends XyzSerializable.Public {} //TODO: User XyzSerializable.Public everywhere directly

    public static class Static implements SerializationView {}

    public static class Internal extends Space.Internal {}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return Objects.equals(id, job.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @JsonIgnore
    protected Marker getMarker() {
        if (marker == null)
            marker = new Log4jMarker(getId());
        return marker;
    }

    @JsonIgnore
    public RuntimeStatus getRuntimeStatus() {
        return runtimeStatus;
    }

    private static final Map<Status, State> bwcStatusMapping = new HashMap<>() {{
        put(waiting, SUBMITTED);
        put(queued, PENDING);
        put(validating, PENDING);
        put(validated, PENDING);
        put(preparing, PENDING);
        put(prepared, PENDING);
        put(executing, RUNNING);
        put(executed, RUNNING);
        put(executing_trigger, RUNNING);
        put(trigger_executed, RUNNING);
        put(collecting_trigger_status, RUNNING);
        put(trigger_status_collected, RUNNING);
        put(finalizing, RUNNING);

        put(finalized, SUCCEEDED);
        put(aborted, CANCELLED);
        put(failed, FAILED);
    }};

    public static class ProcessingNotPossibleException extends Exception {

        private static final String PROCESSING_NOT_POSSIBLE = "waits on free resources";

        ProcessingNotPossibleException() {
            super(PROCESSING_NOT_POSSIBLE);
        }
    }

    static {
        XyzSerializable.registerSubtypes(Job.class);
    }
}
