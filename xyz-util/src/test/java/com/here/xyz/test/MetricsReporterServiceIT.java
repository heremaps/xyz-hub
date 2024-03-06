package com.here.xyz.test;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.here.xyz.util.db.metrics.Metric;
import com.here.xyz.util.db.metrics.MetricsReporterService;
import org.junit.Test;

import java.util.Map;

public class MetricsReporterServiceIT {

    private final AWSLambda lambdaClient = AWSLambdaClientBuilder.standard()
            .withRegion(Regions.EU_WEST_1)
            .withCredentials(new ProfileCredentialsProvider("vhnes-rd"))
            .build();
    @Test
    public void test() throws InterruptedException {
        MetricsReporterService metricsReporterService = new MetricsReporterService(lambdaClient,"iml-metrics-scrapper-sit", 1);
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "2", "dim", "a"), 2L));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "2", "dim", "a"), 2L));
        metricsReporterService.consumeMetric(new Metric(Map.of("userId", "2", "dim", "a"), 100L));
        metricsReporterService.consumeMetric(new Metric(Map.of("dim", "a"), 3L));
        metricsReporterService.consumeMetric(new Metric(Map.of("dim", "a"), 1L));
        metricsReporterService.consumeMetric(new Metric(Map.of("dim", "b"), 2L));
        metricsReporterService.consumeMetric(new Metric(Map.of("di", "b"), 2L));
        Thread.sleep(1 * 60 * 1000);
        metricsReporterService.shutdown();
    }
}
