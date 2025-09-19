/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.execution.SyncExecutionStep;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.inputs.InputFromOutput;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S3MetricsCollectorStep extends SyncExecutionStep<S3MetricsCollectorStep> {
    public static final String S3_METRICS = "metrics";
    private static final Logger logger = LogManager.getLogger();

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private Ref version;

    @JsonView({XyzSerializable.Internal.class, XyzSerializable.Static.class})
    private String providedTag;

    private S3MetricsCollectorStep() {
      setOutputSets(List.of(new Step.OutputSet(S3_METRICS, Step.Visibility.USER, true)));
    }

    public S3MetricsCollectorStep(String outputSetGroup) {
      this();
      setOutputSetGroup(outputSetGroup);
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

        FeatureStatistics featureStatistics = processUploadUrlInputs(stepInputs, totalFeatureCount);

        registerOutputs(List.of(featureStatistics), S3_METRICS);
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

    private FeatureStatistics processUploadUrlInputs(List<Input> allInputs, long totalFeatureCount) {

        List<Input> uploadUrlInputs = allInputs.stream()
                .filter(input -> input instanceof UploadUrl)
                .toList();

        long totalFileCount = uploadUrlInputs.size();
        long totalByteSize = calculateTotalByteSize(uploadUrlInputs);

        FeatureStatistics featureStatistics = new FeatureStatistics()
                .withFileCount(totalFileCount)
                .withFeatureCount(totalFeatureCount)
                .withByteSize(totalByteSize);

        if (version != null) {
            featureStatistics.withVersionRef(version);
        }
        if (providedTag != null && !providedTag.isEmpty()) {
            featureStatistics.withTag(providedTag);
        }
        return featureStatistics;
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
