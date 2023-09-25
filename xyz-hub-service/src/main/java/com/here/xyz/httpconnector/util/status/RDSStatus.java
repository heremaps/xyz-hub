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
package com.here.xyz.httpconnector.util.status;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.httpconnector.config.AwsCWClient;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.text.DecimalFormat;
import java.util.List;

public class RDSStatus {
    private static final Logger logger = LogManager.getLogger();
    private static final DecimalFormat DF = new DecimalFormat("0.0000");

    @JsonProperty("RDS_WRITER_METRICS")
    private RdsMetrics rdsMetrics;
    @JsonProperty("CLOUDWATCH_CLUSTER_METRICS")
    private CloudWatchDBClusterMetrics cloudWatchDBClusterMetrics;
    @JsonProperty("RDS_WRITER_RUNNING_QUERIES")
    private List<RunningQueryStatistic>  runningQueries;

    @JsonIgnore
    private String connectorId;

    public RDSStatus(String connectorId){
        this.connectorId = connectorId;
        this.cloudWatchDBClusterMetrics = new CloudWatchDBClusterMetrics();
    }

    public String getConnectorId(){
        return connectorId;
    }

    public RdsMetrics getRdsMetrics() {
        return rdsMetrics;
    }

    public CloudWatchDBMetric getCloudWatchDBClusterMetric(Job job) {
        if(job instanceof Export)
            return cloudWatchDBClusterMetrics.getReader();
        return cloudWatchDBClusterMetrics.getWriter();
    }

    public void addRdsMetrics(RunningQueryStatistics runningQueryStatistics) {
        this.rdsMetrics = new RdsMetrics(runningQueryStatistics);
        this.runningQueries = runningQueryStatistics.getRunningQueries();
    }

    public void addCloudWatchDBWriterMetrics(JSONObject currentWriterMetrics) {
        this.cloudWatchDBClusterMetrics.setCloudWatchDBWriterMetrics(currentWriterMetrics);
    }

    public void addCloudWatchDBReaderMetrics(JSONObject currentReaderMetrics) {
        this.cloudWatchDBClusterMetrics.setCloudWatchDBReaderMetrics(currentReaderMetrics);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public class RdsMetrics{
        private int totalRunningIDXQueries;
        private int totalRunningImportQueries;
        private int totalRunningExportQueries;

        private int totalRunningS3ExportQueries;
        private int totalRunningVMLExportQueries;
        private long totalInflightImportBytes;

        public RdsMetrics(RunningQueryStatistics runningQueryStatistics){
            this.totalRunningIDXQueries = runningQueryStatistics.getRunningIndexQueries();
            this.totalRunningImportQueries = runningQueryStatistics.getRunningImports();
            this.totalInflightImportBytes = runningQueryStatistics.getImportBytesInProgress();
            this.totalRunningVMLExportQueries = runningQueryStatistics.getRunningVMLExports();
            this.totalRunningS3ExportQueries = runningQueryStatistics.getRunningS3Exports();
            this.totalRunningExportQueries = runningQueryStatistics.getRunningS3Exports() + runningQueryStatistics.getRunningVMLExports();
        }

        public int getTotalRunningIDXQueries() {
            return totalRunningIDXQueries;
        }

        public int getTotalRunningImportQueries() {
            return totalRunningImportQueries;
        }

        public int getTotalRunningExportQueries() {
            return totalRunningExportQueries;
        }

        public int getTotalRunningS3ExportQueries() {
            return totalRunningS3ExportQueries;
        }

        public int getTotalRunningVMLExportQueries() {
            return totalRunningVMLExportQueries;
        }

        public long getTotalInflightImportBytes() {
            return totalInflightImportBytes;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public class CloudWatchDBMetric{
        private double cpuLoad;
        private double dbConnections;
        private double networkReceiveThroughputInMb;
        private double writeThroughputInMb;
        private double freeMemInGb;
        private double acuUtilization;
        private double capacity;

        public CloudWatchDBMetric(JSONObject currentMetrics){
            if(currentMetrics.isEmpty())
                return;
            try {
                this.cpuLoad = (Double) currentMetrics.get(AwsCWClient.CPU_UTILIZATION.toLowerCase());
                this.dbConnections = (Double) currentMetrics.get(AwsCWClient.DATABASE_CONNECTIONS.toLowerCase());
                this.writeThroughputInMb = (Double) currentMetrics.get(AwsCWClient.WRITE_THROUGHPUT.toLowerCase());
                this.networkReceiveThroughputInMb = (Double) currentMetrics.get(AwsCWClient.NETWORK_RECEIVE_THROUGHPUT.toLowerCase());
                this.freeMemInGb = (Double) currentMetrics.get(AwsCWClient.FREEABLE_MEMORY.toLowerCase());
                this.acuUtilization = (Double) currentMetrics.get(AwsCWClient.ACU_UTILIZATION.toLowerCase());
                this.capacity = (Double) currentMetrics.get(AwsCWClient.SERVERLESS_DATABASE_CAPACITY.toLowerCase());
            }catch (Exception e){ logger.warn("Can't parse currentMetrics from CW!",e); }
        }

        public double getCpuLoad() { return cpuLoad; }

        public double getDbConnections() { return dbConnections; }

        public double getNetworkReceiveThroughputInMb() { return networkReceiveThroughputInMb; }

        public double getWriteThroughputInMb() { return writeThroughputInMb; }

        public double getFreeMemInGb() { return freeMemInGb; }

        public double getAcuUtilization() { return acuUtilization; }

        public double getCapacity() {
            return capacity;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public class CloudWatchDBClusterMetrics{
        @JsonProperty("WRITER")
        private CloudWatchDBMetric writer;
        @JsonProperty("READER")
        private CloudWatchDBMetric reader;

        public CloudWatchDBClusterMetrics(){
            this.writer = new CloudWatchDBMetric(new JSONObject());
            this.reader = new CloudWatchDBMetric(new JSONObject());
        }

        public void setCloudWatchDBWriterMetrics(JSONObject currentWriterMetrics){
            this.writer = new CloudWatchDBMetric(currentWriterMetrics);
        }

        public void setCloudWatchDBReaderMetrics(JSONObject currentReaderMetrics){
            this.reader = new CloudWatchDBMetric(currentReaderMetrics);
        }

        public CloudWatchDBMetric getWriter() {
            return writer;
        }

        public CloudWatchDBMetric getReader() {
            return reader;
        }
    }

    public static int calculateMemory (Integer maxCapacityUnits){
        if(maxCapacityUnits == null)
            maxCapacityUnits = 16;

        /** 2GB per ACU / 6GB reserved */
        return maxCapacityUnits * 2 - 6;
    }
}
