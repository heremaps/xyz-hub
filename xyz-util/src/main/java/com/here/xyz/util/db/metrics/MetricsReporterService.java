package com.here.xyz.util.db.metrics;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class MetricsReporterService {

    private final ConcurrentHashMap<Integer, StorageMetric> metricStorage = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    private static final Logger logger = LogManager.getLogger();
    private final String lambdaFunctionName;
    private final AWSLambda lambdaClient;

    public MetricsReporterService(AWSLambda lambdaClient, String lambdaFunctionName, int minutesPeriod) {
        this.lambdaClient = lambdaClient;
        this.lambdaFunctionName = lambdaFunctionName;
        this.scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::aggregateAndInvokeLambda, 0, minutesPeriod, TimeUnit.MINUTES);
    }

    public void consumeMetric(Metric metric) {
        var optionalMetric = Optional.ofNullable(metricStorage.get(metric.generateHash()));
        optionalMetric.ifPresentOrElse(m -> m.addToValue(metric.getValue()),
                () -> metricStorage.put(
                        metric.generateHash(),
                        new StorageMetric(metric.getDimensions(), initializeAdder(metric.getValue()))
                )
        );
    }

    private LongAdder initializeAdder(long value) {
        var adder = new LongAdder();
        adder.add(value);
        return adder;
    }

    private void aggregateAndInvokeLambda() {
        var metricsJsonArray = new JsonArray();
        metricStorage.forEach((hash, metric) -> {
            var jsonObject = new JsonObject();
            metric.getDimensions().forEach(jsonObject::put);
            jsonObject.put("value", metric.getValue());
            metricsJsonArray.add(jsonObject);
        });

        var eventJson = new JsonObject();
        eventJson.put("type", "TYPE_PUSH_IO_METRIC");
        eventJson.put("metrics", metricsJsonArray);
        System.out.println("Aggregated: " + eventJson);
        logger.info("Aggregated: " + eventJson);

        invokeLambda(eventJson.toString());
        metricStorage.clear();
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
