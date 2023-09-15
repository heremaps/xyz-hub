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

package com.here.xyz.httpconnector.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricDataResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricDataQuery;
import com.amazonaws.services.cloudwatch.model.MetricDataResult;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import com.here.xyz.httpconnector.CService;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

public class AwsCWClient {
    private static final Logger logger = LogManager.getLogger();

    private final AmazonCloudWatch client;
    private static final String RDS_NAMESPACE = "AWS/RDS";
    private static final String DB_CLUSTER_IDENTIFIER = "DBClusterIdentifier";
    private static final DecimalFormat DF = new DecimalFormat("0.0000");

    private static final HashMap<String,String> METRIC_MAP = new HashMap(){{
        put("DatabaseConnections","dbConnections");
        put("ACUUtilization","acuUtilization");
        put("CPUUtilization","cpuLoad");
        put("FreeableMemory","freemem");
        put("WriteThroughput","writeThroughput");
        put("NetworkReceiveThroughput","networkReceiveThroughput");
        put("ServerlessDatabaseCapacity","capacity");
    }};

    public enum Role {
        READER, WRITER;

        public static Role of(String value) {
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

    public AwsCWClient(){
        final String region = CService.configuration != null ? CService.configuration.JOBS_REGION : "eu-west-1";
        AmazonCloudWatchClientBuilder builder = AmazonCloudWatchClientBuilder.standard();

        if(isLocal()){
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                            CService.configuration.LOCALSTACK_ENDPOINT, CService.configuration.JOBS_REGION))
                   .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("localstack", "localstack")));
        }else {
            builder.setRegion(region);
        }

        this.client = builder.build();
    }

    public JSONObject getAvg5MinRDSMetrics(String dBClusterIdentifier) {
        return getRDSMetrics(dBClusterIdentifier , 5 , 5 * 60 , "Average", Role.READER);
    }

    public JSONObject getAvg5MinRDSMetrics(String dBClusterIdentifier, Role role) {
        return getRDSMetrics(dBClusterIdentifier , 5 , 5 * 60 , "Average", role);
    }

    private JSONObject getRDSMetrics(String dBClusterIdentifier, int timeRangeInMin, int periodInSec, String statistic, Role role) {
        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(new Dimension().withName(DB_CLUSTER_IDENTIFIER).withValue(dBClusterIdentifier));
        dimensions.add(new Dimension().withName("Role").withValue(role.toString()));

        final Date endTime = new Date();

        List<MetricDataQuery> metricDataQueryList = new ArrayList<>();

        METRIC_MAP.keySet().forEach(
                metric ->  metricDataQueryList.add(createMetricDataQuery(dimensions, metric, statistic, periodInSec))
        );

        final GetMetricDataRequest getMetricDataRequest = new GetMetricDataRequest()
                .withMetricDataQueries(metricDataQueryList)
                .withStartTime(new Date(endTime.getTime() - (timeRangeInMin * 60 * 1000)))
                .withEndTime(endTime);

        try {
            GetMetricDataResult metricData = this.client.getMetricData(getMetricDataRequest);
            return metricDataResultToJson(metricData.getMetricDataResults());
        }catch (Exception e){
            logger.warn("Cant get AWS Metrics!",e);
        }
        /** In ErrorCase deliver empty result */
        return new JSONObject();
    }

    private JSONObject metricDataResultToJson(List<MetricDataResult> metricDataResult){
        JSONObject rdsStatistic = new JSONObject();

        for (MetricDataResult res : metricDataResult) {
            res.getValues().forEach(
                    r -> {
                        if(res.getId().equalsIgnoreCase("freemem")) {
                            /** to GB */
                            r = r / 1024 / 1024 / 1024;
                        }if(res.getId().equalsIgnoreCase("networkReceiveThroughput")
                            || res.getId().equalsIgnoreCase("writeThroughput")) {
                            /** to MB */
                            r = r / 1024 / 1024 ;
                        }
                        r = Double.parseDouble(DF.format(r));
                        rdsStatistic.put(res.getId(), r);
                    }
            );
        }
        return rdsStatistic;
    }

    private MetricDataQuery createMetricDataQuery(List<Dimension> dimensions, String metricName, String statistic, int periodInSec){

        final Metric metric = new Metric()
                .withDimensions(dimensions)
                .withMetricName(metricName)
                .withNamespace(RDS_NAMESPACE);

        final MetricStat metricStat = new MetricStat()
                .withMetric(metric)
                .withPeriod(periodInSec)
                .withStat(statistic);

        return new MetricDataQuery()
                .withId(METRIC_MAP.get(metricName))
                .withMetricStat(metricStat);
    }

    public boolean isLocal() {
        if(CService.configuration.HUB_ENDPOINT.contains("localhost") ||
                CService.configuration.HUB_ENDPOINT.contains("xyz-hub:8080"))
            return true;
        return false;
    }
}
