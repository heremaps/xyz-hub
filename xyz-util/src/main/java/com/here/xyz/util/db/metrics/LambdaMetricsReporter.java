package com.here.xyz.util.db.metrics;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LambdaMetricsReporter implements MetricsReporter {

    private final Map<String, ConcurrentHashMap<String, AggregatedMetric>> metricStorage = new HashMap<>();
    private final ScheduledExecutorService scheduler;

    private static final Logger logger = LogManager.getLogger();
    private final String lambdaFunctionName;
    private final AWSLambda lambdaClient;

    public LambdaMetricsReporter(AWSLambda lambdaClient, String lambdaFunctionName, int reportIntervalMs, List<String> metricTypes) {
        this.lambdaClient = lambdaClient;
        this.lambdaFunctionName = lambdaFunctionName;
        this.scheduler = Executors.newScheduledThreadPool(1);
        metricTypes.forEach(type -> {
            metricStorage.put(type, new ConcurrentHashMap<>());
        });
        scheduler.scheduleAtFixedRate(this::aggregateAndInvokeLambda, 1, reportIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportMetric(Metric metric) {
        Optional.ofNullable(metricStorage.get(metric.type()))
                .ifPresentOrElse(map -> {
                    map.compute(getKey(metric.dimensions()), (key, existingMetric) -> {
                        if (existingMetric != null) {
                            existingMetric.addToValue(metric.value());
                            return existingMetric;
                        } else {
                            return new AggregatedMetric(metric.dimensions(), metric.value());
                        }
                    });
                }, () -> logger.error("Unknown metric"));
    }

    @Override
    public void flush() {
        aggregateAndInvokeLambda();
    }

    private void aggregateAndInvokeLambda() {
        metricStorage.forEach((type, metricMap) -> {
            var metricsJsonArray = new JsonArray();

            Iterator<Map.Entry<String, AggregatedMetric>> iterator = metricMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, AggregatedMetric> entry = iterator.next();
                AggregatedMetric metric = entry.getValue();

                var jsonObject = new JsonObject();
                metric.getDimensions().forEach(jsonObject::put);
                jsonObject.put("value", metric.getValue());
                metricsJsonArray.add(jsonObject);

                iterator.remove();
            }

            var eventJson = new JsonObject();
            eventJson.put("type", "REPORT_METRIC");
            eventJson.put("metricType", type);
            eventJson.put("metrics", metricsJsonArray);
            logger.info("Metric of type " + type +" is aggregated: " + eventJson);
            invokeLambda(eventJson.toString());
        });
    }

    private void invokeLambda(String eventJson) {
        try {
            InvokeRequest request = new InvokeRequest()
                    .withFunctionName(lambdaFunctionName)
                    .withPayload(eventJson)
                    .withInvocationType(InvocationType.Event);

            lambdaClient.invoke(request);
            logger.info("Metric reporter has successfully reported the metrics");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
    }

    public static String getKey(Map<String, String> map) {
        List<Map.Entry<String, String>> entryList = new ArrayList<>(map.entrySet());
        entryList.sort(Map.Entry.comparingByKey());

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : entryList) {
            sb.append("<").append(entry.getKey()).append(":").append(entry.getValue()).append(">");
        }
        return sb.toString();
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
