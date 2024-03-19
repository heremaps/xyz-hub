package com.here.xyz.test;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.here.xyz.util.db.metrics.Metric;
import com.here.xyz.util.db.metrics.MetricsReporterService;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MetricsReporterServiceIT {

    private final AWSLambda lambdaClient = AWSLambdaClientBuilder.standard()
            .withRegion(Regions.EU_WEST_1)
            .withCredentials(new ProfileCredentialsProvider("vhnes-rd"))
            .build();

    @Test
    public void test() throws InterruptedException {
        MetricsReporterService metricsReporterService = new MetricsReporterService(lambdaClient, "iml-metrics-scrapper-sit", 1, List.of("io", "storage"));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "2", "realmId", "a"), "io", 2L));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "2", "realmId", "a"), "io", 4L));
        metricsReporterService.consumeMetric(new Metric(Map.of("realmId", "a", "userId", "2"), "io", 4L));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "a", "realmId", "b"), "io", 3L));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "2", "realmId", "a"), "storage", 100L));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "2", "realmId", "a"), "storage", 100L));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "a"), "io", 32L));
        Thread.sleep(1 * 60 * 2000);
        metricsReporterService.shutdown();
    }
}
