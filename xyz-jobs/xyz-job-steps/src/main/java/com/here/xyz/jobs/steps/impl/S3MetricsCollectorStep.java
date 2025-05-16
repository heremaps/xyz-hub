package com.here.xyz.jobs.steps.impl;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3MetricsCollectorStep extends LambdaBasedStep<S3MetricsCollectorStep> {
    public static final String S3_METRICS = "s3_metrics";
    private static final Logger logger = LogManager.getLogger();
    private static final Pattern FEATURE_PATTERN = Pattern.compile("\"type\"\\s*:\\s*\"Feature\"");

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private String version;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private String tag;

    {
        setOutputSets(List.of(new Step.OutputSet(S3_METRICS, Step.Visibility.USER, true)));
    }

    @Override
    public List<Load> getNeededResources() {
        return List.of();
    }

    @Override
    public int getTimeoutSeconds() {
        return 60;
    }

    @Override
    public int getEstimatedExecutionSeconds() {
        return 30;
    }

    @Override
    public String getDescription() {
        return "Collects metrics from input files";
    }

    @Override
    public void execute(boolean resume) throws Exception {
        logger.info("Starting S3MetricsCollectorStep execution");

        List<Input> inputs = loadAllInputs();
        long featureCount = calculateFeatureCount(inputs);
        long totalFileCount = inputs.size();
        long totalByteSize = calculateTotalByteSize(inputs);

        FeatureStatistics resultOutput = new FeatureStatistics()
                .withFileCount(totalFileCount)
                .withFeatureCount(featureCount)
                .withByteSize(totalByteSize);

        if (version != null && !version.isEmpty()) {
            resultOutput.withVersion(version);
        }
        if (tag != null && !tag.isEmpty()) {
            resultOutput.withTag(tag);
        }

        logger.info("Collected metrics: fileCount={}, byteSize={}, featureCount={}",
                totalFileCount, totalByteSize, featureCount);

        registerOutputs(List.of(resultOutput), S3_METRICS);
    }

    private long calculateTotalByteSize(List<Input> inputs) {
        return inputs.stream()
                .mapToLong(Input::getByteSize)
                .sum();
    }

    private long calculateFeatureCount(List<Input> inputs) {
        long count = 0;
        try {
            for (Input input : inputs) {
                if (input instanceof UploadUrl) {
                    URL downloadUrl = ((UploadUrl) input).getDownloadUrl();
                    if (downloadUrl != null) {
                        count += countFeaturesInFile(downloadUrl);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error calculating feature count", e);
        }
        return count;
    }

    private long countFeaturesInFile(URL url) {
        long count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = FEATURE_PATTERN.matcher(line);
                while (matcher.find()) {
                    count++;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to count features in file: {}", url, e);
        }
        return count;
    }

    @Override
    public void cancel() throws Exception {
        logger.info("Cancelling S3MetricsCollectorStep execution");
    }

    @Override
    public boolean validate() throws BaseHttpServerVerticle.ValidationException {
        return true;
    }

    private List<Input> loadAllInputs() {
        return loadInputs(UploadUrl.class);
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public S3MetricsCollectorStep withVersion(String version) {
        setVersion(version);
        return this;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public S3MetricsCollectorStep withTag(String tag) {
        setTag(tag);
        return this;
    }

    @Override
    public AsyncExecutionState getExecutionState() throws UnknownStateException {
        return AsyncExecutionState.SUCCEEDED;
    }

    @Override
    public ExecutionMode getExecutionMode() {
        return ExecutionMode.SYNC;
    }
}
