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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.text.DecimalFormat;
import java.util.*;

public class AwsCWClient {
    private static final Logger logger = LogManager.getLogger();

    private final AmazonCloudWatch client;
    private static final String RDS_NAMESPACE = "AWS/RDS";
    private static final DecimalFormat DF = new DecimalFormat("0.0000");
    private static final HashMap<String,String> METRIC_MAP = new HashMap<String,String>(){{
        put("DatabaseConnections","dbConnections");
        put("CPUUtilization","cpuLoad");
        put("FreeableMemory","freemem");
        put("WriteThroughput","writeThroughput");
        put("NetworkReceiveThroughput","networkReceiveThroughput");
        put("ServerlessDatabaseCapacity","capacity");
    }};

    public AwsCWClient(){
        //TODO: Support Multiregion
        this.client = AmazonCloudWatchClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
    }

    public JSONObject getAvg5MinRDSMetrics(String dbInstanceIdentifier){
        return getRDSMetrics(dbInstanceIdentifier , 5 , 5 * 60 , "Average" );
    }

    private JSONObject getRDSMetrics(String dbInstanceIdentifier, int timeRangeInMin, int periodInSec, String statistic){
        final Dimension dimension = new Dimension().withName("DBInstanceIdentifier").withValue(dbInstanceIdentifier);
        final Date endTime = new Date();

        List<MetricDataQuery> metricDataQueryList = new ArrayList<>();

        METRIC_MAP.keySet().forEach(
                metric ->  metricDataQueryList.add(createMetricDataQuery(dimension, metric, statistic, periodInSec))
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
                            rdsStatistic.put("freememPercentage", (r/90));
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

    private MetricDataQuery createMetricDataQuery(Dimension dimension, String metricName, String statistic, int periodInSec){
              final Metric metric = new Metric()
                .withDimensions(Collections.singletonList(dimension))
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
}
