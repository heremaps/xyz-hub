package com.here.xyz.jobs.steps.impl;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.SyncLambdaStep;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.InputFromOutput;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class S3MetricsCollectorStep extends SyncLambdaStep {
    public static final String S3_METRICS = "s3_metrics";
    private static final Logger logger = LogManager.getLogger();

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private Ref version;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private String providedTag;

    {
        setOutputSets(List.of(new Step.OutputSet(S3_METRICS, Step.Visibility.USER, true)));
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
        logger.debug("Starting S3MetricsCollectorStep execution");
        List<Input> stepInputs = loadAllInputs();

        long totalFeatureCount = calculateTotalFeatureCount(stepInputs);

        List<Output> resultOutputs = processUploadUrlInputs(stepInputs, totalFeatureCount);

        registerOutputs(resultOutputs, S3_METRICS);
        logger.debug("S3MetricsCollectorStep execution completed successfully");
    }

    private long calculateTotalFeatureCount(List<Input> inputs) {
        return inputs.stream()
                .filter(input -> input instanceof InputFromOutput)
                .map(input -> ((InputFromOutput) input).getDelegate())
                .filter(delegate -> delegate instanceof FeatureStatistics)
                .mapToLong(delegate -> ((FeatureStatistics) delegate).getFeatureCount())
                .sum();
    }

    private List<Output> processUploadUrlInputs(List<Input> allInputs, long totalFeatureCount) {
        List<Output> outputs = new ArrayList<>();

        List<Input> uploadUrlInputs = allInputs.stream()
                .filter(input -> input instanceof UploadUrl)
                .toList();

        if (!uploadUrlInputs.isEmpty()) {
            long totalFileCount = uploadUrlInputs.size();
            long totalByteSize = calculateTotalByteSize(uploadUrlInputs);

            FeatureStatistics resultOutput = new FeatureStatistics()
                    .withFileCount(totalFileCount)
                    .withFeatureCount(totalFeatureCount)
                    .withByteSize(totalByteSize);

            if (version != null) {
                resultOutput.withVersionRef(version);
            }
            if (providedTag != null && !providedTag.isEmpty()) {
                resultOutput.withTag(providedTag);
            }

            outputs.add(resultOutput);
        }

        return outputs;
    }

    private long calculateTotalByteSize(List<Input> inputs) {
        return inputs.stream()
                .mapToLong(Input::getByteSize)
                .sum();
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
        return loadInputs(UploadUrl.class, InputFromOutput.class);
    }

    public Ref getVersion() {
        return version;
    }

    public void setVersion(Ref version) {
        this.version = version;
    }

    public S3MetricsCollectorStep withVersion(Ref version) {
        setVersion(version);
        return this;
    }

    public String getProvidedTag() {
        return providedTag;
    }

    public void setProvidedTag(String providedTag) {
        this.providedTag = providedTag;
    }

    public S3MetricsCollectorStep withProvidedTag(String tag) {
        setProvidedTag(tag);
        return this;
    }
}
