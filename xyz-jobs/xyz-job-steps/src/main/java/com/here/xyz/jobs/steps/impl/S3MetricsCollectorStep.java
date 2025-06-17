package com.here.xyz.jobs.steps.impl;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.payloads.StepPayload;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class S3MetricsCollectorStep extends LambdaBasedStep<S3MetricsCollectorStep> {
    public static final String S3_METRICS = "s3_metrics";
    private static final Logger logger = LogManager.getLogger();
    private static final Pattern FEATURE_PATTERN = Pattern.compile("\"type\"\\s*:\\s*\"Feature\"");

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private Ref version;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private String providedTag;

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
        List<Output> resultOutputs = new ArrayList<>();

        for (Input input : loadAllInputs()) {
            List<Input> inputs = List.of(input);
            long featureCount = calculateFeatureCount(inputs, input.isCompressed());
            long totalFileCount = inputs.size();
            long totalByteSize = calculateTotalByteSize(inputs);

            FeatureStatistics resultOutput = new FeatureStatistics()
                    .withFileCount(totalFileCount)
                    .withFeatureCount(featureCount)
                    .withByteSize(totalByteSize);

            if (version != null) {
                resultOutput.withVersionRef(version);
            }
            if (providedTag != null && !providedTag.isEmpty()) {
                resultOutput.withTag(providedTag);
            }

            resultOutputs.add(resultOutput);
        }

        registerOutputs(resultOutputs, S3_METRICS);
    }

    private long calculateTotalByteSize(List<Input> inputs) {
        return inputs.stream()
                .mapToLong(Input::getByteSize)
                .sum();
    }

    private long calculateFeatureCount(List<Input> inputs, boolean compressed) {
        // we've decided to ignore counting features for now
        return 0;
//        long count = 0;
//        try {
//            for (Input input : inputs) {
//                if (input instanceof UploadUrl) {
//                    URL downloadUrl = ((UploadUrl) input).getDownloadUrl();
//                    if (downloadUrl != null) {
//                        count += countFeaturesInFile(downloadUrl, compressed);
//                    }
//                }
//            }
//        } catch (Exception e) {
//            logger.error("Error calculating feature count", e);
//        }
//        return count;
    }

    private long countFeaturesInFile(URL url, boolean compressed) {
        long count = 0;
        try {
            if (compressed) {
                try (ZipInputStream zipStream = new ZipInputStream(url.openStream())) {
                    ZipEntry entry;
                    while ((entry = zipStream.getNextEntry()) != null) {
                        if (!entry.isDirectory()) {
                            count += countFeaturesInZipEntry(zipStream);
                        }
                        zipStream.closeEntry();
                    }
                }
            } else {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Matcher matcher = FEATURE_PATTERN.matcher(line);
                        while (matcher.find()) {
                            count++;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to count features in file: {}", url, e);
        }
        return count;
    }

    private long countFeaturesInZipEntry(InputStream zipEntryStream) {
        long count = 0;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(zipEntryStream));
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = FEATURE_PATTERN.matcher(line);
                while (matcher.find()) {
                    count++;
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to count features in zip entry", e);
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

    @Override
    public AsyncExecutionState getExecutionState() throws UnknownStateException {
        return AsyncExecutionState.SUCCEEDED;
    }

    @Override
    public ExecutionMode getExecutionMode() {
        return ExecutionMode.SYNC;
    }
}
