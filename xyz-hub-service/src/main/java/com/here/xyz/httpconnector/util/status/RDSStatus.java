/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import com.here.xyz.httpconnector.CService;
import org.json.JSONObject;

import java.util.List;

public class RDSStatus {
    @JsonProperty("CURRENT_METRICS")
    private CurrentMetrics currentMetrics;
    @JsonProperty("LIMITS")
    private Limits limits;
    @JsonProperty("RUNNING_QUERIES")
    private RunningQueries runningQueries;
    @JsonIgnore
    private String clientId;

    public RDSStatus(){}

    public RDSStatus(String clientId, JSONObject currentMetrics, RunningQueryStatistics runningQueryStatistics ){
        this.clientId = clientId;
        this.currentMetrics = new CurrentMetrics(currentMetrics, runningQueryStatistics);
        this.runningQueries = new RunningQueries(runningQueryStatistics.getRunningQueries());
        this.limits = new Limits(CService.rdsLookupCapacity.get(clientId));
    }

    public String getClientId(){
        return clientId;
    }

    public CurrentMetrics getCurrentMetrics() {
        return currentMetrics;
    }

    public Limits getLimits() {
        return limits;
    }

    public RunningQueries getRunningQueries() {
        return runningQueries;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class CurrentMetrics{
        double cpuLoad;
        double dbConnections;
        double writeThroughput;
        double freeMem;
        double freeMemPercentage;
        double capacityUnits;

        int totalRunningIDXQueries;
        int totalRunningImportQueries;
        long totalInflightImportBytes;

        public CurrentMetrics(JSONObject currentMetrics, RunningQueryStatistics runningQueryStatistics){
            try {
                this.cpuLoad = (Double) currentMetrics.get("cpuLoad");
                this.dbConnections = (Double) currentMetrics.get("dbConnections");
                this.writeThroughput = (Double) currentMetrics.get("writeThroughput");
                this.freeMem = (Double) currentMetrics.get("freemem");
                this.freeMemPercentage = (Double) currentMetrics.get("freememPercentage");
                this.capacityUnits = (Double) currentMetrics.get("capacity");
            }catch (Exception e){ /**Ignore*/ }

            this.totalRunningIDXQueries = runningQueryStatistics.getRunningIndexQueries();
            this.totalRunningImportQueries = runningQueryStatistics.getRunningImports();
            this.totalInflightImportBytes = runningQueryStatistics.getImportBytesInProgress();
        }

        public double getCpuLoad() {
            return cpuLoad;
        }

        public double getDbConnections() {
            return dbConnections;
        }

        public double getWriteThroughput() {
            return writeThroughput;
        }

        public double getFreeMem() {
            return freeMem;
        }

        public double getFreeMemPercentage() {
            return freeMemPercentage;
        }

        public double getCapacityUnits() {return capacityUnits;}

        public int getTotalRunningIDXQueries() {
            return totalRunningIDXQueries;
        }

        public long getTotalInflightImportBytes() {
            return totalInflightImportBytes;
        }

        public long getTotalRunningImportQueries() { return totalRunningImportQueries; }
    }

    public static class Limits{
        int maxCapacityUnits;
        int maxMemInGB;

        public Limits(Integer maxCapacityUnits){
            if(maxCapacityUnits == null)
                this.maxCapacityUnits = 16;
            else
                this.maxCapacityUnits = maxCapacityUnits;

            /** 2GB per ACU / 8GB reserved */
            this.maxMemInGB =  this.maxCapacityUnits * 2 -8;
        }

        public int getMaxCapacityUnits() {
            return maxCapacityUnits;
        }

        public int getMaxMemInGB() {
            return maxMemInGB;
        }
    }

    public static class RunningQueries{
        List<RunningQueryStatistic> runningQueryStatisticList;
        public RunningQueries(List<RunningQueryStatistic> runningQueryStatisticList){
            this.runningQueryStatisticList = runningQueryStatisticList;
        }

        public List<RunningQueryStatistic> getRunningQueryStatisticList() {
            return runningQueryStatisticList;
        }
    }
}
