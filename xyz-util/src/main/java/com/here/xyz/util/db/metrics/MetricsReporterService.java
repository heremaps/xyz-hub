package com.here.xyz.util.db.metrics;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.here.xyz.util.Hasher;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class MetricsReporterService {

    private final Map<String, ConcurrentHashMap<String, StorageMetric>> metricStorage = new HashMap<>();
    private final ScheduledExecutorService scheduler;

    private static final Logger logger = LogManager.getLogger();
    private final String lambdaFunctionName;
    private final AWSLambda lambdaClient;

    public MetricsReporterService(AWSLambda lambdaClient, String lambdaFunctionName, int minutesPeriod, List<String> metricNames) {
        this.lambdaClient = lambdaClient;
        this.lambdaFunctionName = lambdaFunctionName;
        this.scheduler = Executors.newScheduledThreadPool(1);
        metricNames.forEach(name -> {
            metricStorage.put(name, new ConcurrentHashMap<>());
        });
        scheduler.scheduleAtFixedRate(this::aggregateAndInvokeLambda, 1, minutesPeriod, TimeUnit.MINUTES);
    }

    public void consumeMetric(Metric metric) {
        Optional.ofNullable(metricStorage.get(metric.getName()))
                .ifPresentOrElse(map -> {
                    map.compute(Hasher.getHash(metric.getDimensions().toString()), (key, existingMetric) -> {
                        if (existingMetric != null) {
                            existingMetric.addToValue(metric.getValue());
                            return existingMetric;
                        } else {
                            return new StorageMetric(metric.getDimensions(), initializeAdder(metric.getValue()));
                        }
                    });
                }, () -> logger.error("Unknown metric"));
    }

    private LongAdder initializeAdder(long value) {
        var adder = new LongAdder();
        adder.add(value);
        return adder;
    }

    private void aggregateAndInvokeLambda() {
        metricStorage.forEach((name, metricMap) -> {
            var metricsJsonArray = new JsonArray();

            Iterator<Map.Entry<String, StorageMetric>> iterator = metricMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, StorageMetric> entry = iterator.next();
                StorageMetric metric = entry.getValue();

                var jsonObject = new JsonObject();
                metric.getDimensions().forEach(jsonObject::put);
                jsonObject.put("value", metric.getValue());
                metricsJsonArray.add(jsonObject);

                iterator.remove();
            }

            metricMap.forEach((key, value) -> System.out.println("Key: " + key + ", Value: " + value));
            var eventJson = new JsonObject();
            eventJson.put("type", name);
            eventJson.put("metrics", metricsJsonArray);
            System.out.println("Aggregated: " + eventJson);
            logger.info("Aggregated: " + eventJson);
            invokeLambda(eventJson.toString());
        });
    }

    private void invokeLambda(String eventJson) {
        try {
            InvokeRequest request = new InvokeRequest()
                    .withFunctionName(lambdaFunctionName)
                    .withPayload(eventJson);

            lambdaClient.invoke(request);
            logger.info("Lambda invoked");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
