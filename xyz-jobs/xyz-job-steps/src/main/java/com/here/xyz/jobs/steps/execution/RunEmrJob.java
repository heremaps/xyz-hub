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

package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.steps.execution.InputSetReference.INPUT_SET_REF_PATTERN;
import static com.here.xyz.jobs.steps.execution.InputSetReference.INPUT_SET_REF_PREFIX;
import static com.here.xyz.jobs.steps.execution.InputSetReference.INPUT_SET_REF_SUFFIX;
import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;
import static com.here.xyz.jobs.steps.execution.resolver.S3Util.copyFileFromS3ToLocal;
import static java.util.regex.Matcher.quoteReplacement;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.execution.resolver.EmrScriptResolver;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.KeyValue;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RunEmrJob extends LambdaBasedStep<RunEmrJob> {
  public static final String EMR_JOB_NAME_PREFIX = "step:";
  private static final Logger logger = LogManager.getLogger();

  private String applicationId;
  private String executionRoleArn;
  private String jarUrl;
  private List<String> positionalScriptParams = new ArrayList<>();
  private Map<String, String> namedScriptParams = new HashMap<>();
  private String sparkParams;

  @JsonIgnore
  public String getEmrJobName() {
    return EMR_JOB_NAME_PREFIX + getGlobalStepId();
  }

  public static String globalStepIdFromEmrJobName(String emrJobName) {
    return emrJobName.startsWith(EMR_JOB_NAME_PREFIX) ? emrJobName.substring(emrJobName.indexOf(':') + 1) : null;
  }

  @Override
  public List<Load> getNeededResources() {
    return List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 24 * 3600; //TODO: Calculate expected value from job history
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 3600; //TODO: Calculate expected value from job history
  }

  @Override
  public String getDescription() {
    return "Runs a serverless EMR job on application " + applicationId;
  }

  //Gets only executed when running locally (see GraphTransformer)
  @Override
  public void execute(boolean resume) throws Exception {
    logger.info("[EMR-local] Positional script params: {}", String.join(" ", getPositionalScriptParams()));
    logger.info("[EMR-local] Named script params: {}", getNamedScriptParams());
    List<String> rawScriptParams = getScriptParams();

    String tmpDir = String.format("/tmp/%s/%s/%s", UUID.randomUUID().toString().substring(0, 4), getJobId(), getId());
    EmrScriptResolver emrScriptResolver = new EmrScriptResolver(getJobId(), getId(), tmpDir, getInputSets());
    List<String> scriptParams = emrScriptResolver.resolveScriptParams(rawScriptParams);

    logger.info("[EMR-local] Resolved script params: {}", String.join(" ", scriptParams));

    emrScriptResolver.prepareInputDirectories();

    String localJarPath = copyFileFromS3ToLocal(jarUrl, tmpDir);

    scriptParams.add("--local");

    String localSparkParams = sparkParams.replace("$localJarPath$", localJarPath);
    localSparkParams = "java -Xshare:off --add-exports=java.base/java.nio=ALL-UNNAMED "
        + "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED "
        + "--add-exports=java.base/java.lang.invoke=ALL-UNNAMED "
        + "--add-exports=java.base/java.util=ALL-UNNAMED "
        + localSparkParams;

    List<String> emrParams = new ArrayList<>(List.of(localSparkParams.split(" ")));
    emrParams.addAll(scriptParams);

    logger.info("[EMR-local] Start local EMR job with the following params: {} ", emrParams.toString());

    ProcessBuilder processBuilder = new ProcessBuilder(emrParams);
    //Modify the environment variables of the process to clear any JDWP options
    //to avoid -agentlib:jdwp=transport=dt_socket
    Map<String, String> env = processBuilder.environment();
    env.remove("_JAVA_OPTIONS");
    env.remove("JAVA_TOOL_OPTIONS");

    //Combine stdout and stderr
    processBuilder.redirectErrorStream(true);
    Process process = processBuilder.start();

    //Capture and log the output of the JAR process
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line;

    while ((line = reader.readLine()) != null)
      logger.info("[EMR-local] {}", line);

    int exitCode = process.waitFor();

    if (exitCode != 0)
      throw new StepException("Local EMR execution failed with exit code " + exitCode + ". Please check the logs.")
          .withCode("exit:" + exitCode);

    emrScriptResolver.publishOutputDirectories();
  }

  @Override
  public void cancel() throws Exception {
    //NOTE: As this step is just a "configuration holder", this method should never actually be called
    throw new RuntimeException("RunEmrJob#cancel() was called.");
  }

  @Override
  public boolean validate() throws ValidationException {
    if (getScriptParams().isEmpty())
      throw new ValidationException("ScriptParams are mandatory!"); //TODO: Check if this is really needed for *all* EMR jobs (if not move to according sub-class)
    if (sparkParams == null)
      throw new ValidationException("SparkParams are mandatory!"); //TODO: Check if this is really needed for *all* EMR jobs (if not move to according sub-class)
    if (jarUrl == null)
      throw new ValidationException("JAR URL is mandatory!");
    //TODO: Move the ScriptParams length check into the according sub-class
    if (getScriptParams().size() < 2)
      throw new ValidationException("ScriptParams length is to small!");

    return !isUserInputsExpected() || isUserInputsPresent(Input.class);
  }

  @JsonIgnore
  public List<String> getScriptParams() {
    List<String> scriptParams = new ArrayList<>(positionalScriptParams != null ? positionalScriptParams : List.of());
    if (namedScriptParams != null && !namedScriptParams.isEmpty())
      namedScriptParams.forEach((name, value) -> scriptParams.add("--" + name + (value != null && !value.isEmpty() ? "=" + value : "")));
    return scriptParams;
  }

  @Override
  public boolean isEquivalentTo(StepExecution other) {
    if (!(other instanceof RunEmrJob otherEmrStep))
      return super.isEquivalentTo(other);

    return Objects.equals(otherEmrStep.applicationId, applicationId)
        && Objects.equals(otherEmrStep.executionRoleArn, executionRoleArn)
        && Objects.equals(otherEmrStep.jarUrl, jarUrl)
        //TODO: For now do not compare the actual values of the positional params until the full fledged inputSet comparison was implemented for the GraphFusion
        && Objects.equals(otherEmrStep.positionalScriptParams.size(), positionalScriptParams.size())
        //TODO: For now do not compare the actual values of the positional that contain an inputSet reference until the full fledged inputSet comparison was implemented for the GraphFusion
        && isEquivalentTo(namedScriptParams, otherEmrStep.namedScriptParams)
        && Objects.equals(otherEmrStep.sparkParams, sparkParams);
  }

  private boolean isEquivalentTo(Map<String, String> namedScriptParams, Map<String, String> otherNamedScriptParams) {
    return Objects.equals(namedScriptParams.keySet(), otherNamedScriptParams.keySet()) &&
        namedScriptParams != null && otherNamedScriptParams != null && namedScriptParams.entrySet().stream()
            .allMatch(entry -> isEquivalentTo(namedScriptParams.get(entry.getKey()), otherNamedScriptParams.get(entry.getKey())));
  }

  private boolean isEquivalentTo(String namedScriptParamValue, String otherNamedScriptParamValue) {
    return Objects.equals(namedScriptParamValue, otherNamedScriptParamValue)
        || namedScriptParamValue.contains(INPUT_SET_REF_PREFIX) && otherNamedScriptParamValue.contains(INPUT_SET_REF_PREFIX);
  }

  public String getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public RunEmrJob withApplicationId(String applicationId) {
    setApplicationId(applicationId);
    return this;
  }

  public String getExecutionRoleArn() {
    return executionRoleArn;
  }

  public void setExecutionRoleArn(String executionRoleArn) {
    this.executionRoleArn = executionRoleArn;
  }

  public RunEmrJob withExecutionRoleArn(String executionRoleArn) {
    setExecutionRoleArn(executionRoleArn);
    return this;
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public void setJarUrl(String jarUrl) {
    this.jarUrl = jarUrl;
  }

  public RunEmrJob withJarUrl(String jarUrl) {
    setJarUrl(jarUrl);
    return this;
  }

  public List<String> getPositionalScriptParams() {
    return positionalScriptParams;
  }

  public void setPositionalScriptParams(List<String> positionalScriptParams) {
    this.positionalScriptParams = positionalScriptParams;
  }

  public RunEmrJob withPositionalScriptParams(List<String> positionalScriptParams) {
    setPositionalScriptParams(positionalScriptParams);
    return this;
  }

  public Map<String, String> getNamedScriptParams() {
    return namedScriptParams;
  }

  public void setNamedScriptParams(Map<String, String> namedScriptParams) {
    this.namedScriptParams = namedScriptParams;
  }

  public RunEmrJob withNamedScriptParams(Map<String, String> namedScriptParams) {
    setNamedScriptParams(namedScriptParams);
    return this;
  }

  public RunEmrJob withNamedScriptParam(String key, String value) {
    getNamedScriptParams().put(key, value);
    return this;
  }

  public RunEmrJob withNamedScriptParam(KeyValue<String, String> param) {
    param.putToMap(getNamedScriptParams());
    return this;
  }

  public String getSparkParams() {
    return sparkParams;
  }

  public void setSparkParams(String sparkParams) {
    this.sparkParams = sparkParams;
  }

  public RunEmrJob withSparkParams(String sparkParams) {
    setSparkParams(sparkParams);
    return this;
  }

  @Override
  public LambdaBasedStep.AsyncExecutionState getExecutionState() throws UnknownStateException {
    //TODO: Better extend SyncLambdaStep in that case
    throw new UnknownStateException("RunEmrJob runs in SYNC mode only.");
  }

  @Override
  public LambdaBasedStep.ExecutionMode getExecutionMode() {
    return SYNC;
  }

  //Increase the visibility of that method, because for the native EMR integration it's actually necessary to define the expected outputs in the compilers
  @Override
  public void setOutputSets(List<OutputSet> outputSets) {
    super.setOutputSets(outputSets);
  }

  public RunEmrJob withOutputSets(List<OutputSet> outputSets) {
    setOutputSets(outputSets);
    return this;
  }

  public String inputSetReference(InputSet inputSet) {
    if (!getInputSets().contains(inputSet))
      throw new IllegalArgumentException("The provided inputSet is not a part of this EMR step.");

    return toInputSetReference(inputSet);
  }

  public static String toInputSetReference(InputSet inputSet) {
    return INPUT_SET_REF_PREFIX + toReferenceIdentifier(inputSet) + INPUT_SET_REF_SUFFIX;
  }

  private static String toReferenceIdentifier(InputSet inputSet) {
    return inputSet.providerId() + "." + inputSet.name();
  }

  InputSet fromReferenceIdentifier(String referenceIdentifier) {
    ReferenceIdentifier ref = ReferenceIdentifier.fromString(referenceIdentifier);
    return getInputSet(ref.stepId(), ref.name());
  }

  protected InputSet getInputSet(String providerId, String name) {
    try {
      return getInputSets().stream()
          .filter(inputSet -> Objects.equals(inputSet.name(), name) && Objects.equals(inputSet.providerId(), providerId))
          .findFirst()
          .get();
    }
    catch (NoSuchElementException e) {
      throw new IllegalArgumentException("No input set \"" + providerId + "." + name + "\" exists in step \"" + getId() + "\"");
    }
  }

  List<String> getResolvedScriptParams() {
    //Replace potential inputSet references within the script params
    return getScriptParams().stream().map(scriptParam -> replaceInputSetReferences(scriptParam)).toList();
  }

  private String replaceInputSetReferences(String scriptParam) {
    return mapInputReferencesIn(scriptParam,
        referenceIdentifier -> fromReferenceIdentifier(referenceIdentifier).toS3Uri(getJobId()).toString());
  }

  static String mapInputReferencesIn(String scriptParam, Function<String, String> mapper) {
    if (scriptParam == null)
      return null;
    return INPUT_SET_REF_PATTERN.matcher(scriptParam)
        .replaceAll(match -> {
          String replacement = mapper.apply(match.group(1));
          if (replacement == null)
            return quoteReplacement(match.group());
          return quoteReplacement(replacement);
        });
  }

  record ReferenceIdentifier(String stepId, String name) {
    public static ReferenceIdentifier fromString(String referenceIdentifier) {
      return new ReferenceIdentifier(referenceIdentifier.substring(0, referenceIdentifier.indexOf(".")),
          referenceIdentifier.substring(referenceIdentifier.indexOf(".") + 1));
    }
  }
}
