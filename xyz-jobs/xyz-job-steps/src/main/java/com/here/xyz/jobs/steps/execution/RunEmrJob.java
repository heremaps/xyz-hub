/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.ExecutionMode.SYNC;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RunEmrJob extends LambdaBasedStep<RunEmrJob> {
  private static final Logger logger = LogManager.getLogger();

  private String applicationId;
  private String executionRoleArn;
  private String jarUrl;
  private List<String> scriptParams;
  private String sparkParams;
  private boolean inputsExpected;

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
  public void execute() throws Exception {
    //Create the local target directory in which EMR writes the output
    String localTmpOutputsFolder = createLocalFolder(scriptParams.get(1), true);

    //Download EMR executable JAR from S3 to local
    String localJarPath = copyFileFromS3ToLocal(jarUrl);
    //Copy step input files from S3 to local /tmp
    String localTmpInputsFolder = copyFolderFromS3ToLocal(scriptParams.get(0));

    scriptParams.set(0, localTmpInputsFolder);
    scriptParams.set(1, localTmpOutputsFolder);

    sparkParams = sparkParams.replace("$localJarPath$", localJarPath);
    List<String> emrParams = new ArrayList<>(List.of(sparkParams.split(" ")));
    emrParams.addAll(scriptParams);

    logger.info("Start local EMR job with the following params: {} ", emrParams.toString());

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
      throw new RuntimeException("Local EMR execution failed with exit code " + exitCode + ". Please check the logs.");

    //Register the EMR files, which are stored locally, as step outputs.
    registerEmrOutputs(new File(localTmpOutputsFolder));
  }

  @Override
  public void resume() throws Exception {
    //NOTE: As this step is just a "configuration holder", this method should never actually be called
    throw new RuntimeException("RunEmrJob#resume() was called.");
  }

  @Override
  public void cancel() throws Exception {
    //NOTE: As this step is just a "configuration holder", this method should never actually be called
    throw new RuntimeException("RunEmrJob#cancel() was called.");
  }

  @Override
  public boolean validate() throws ValidationException {
    if (scriptParams == null)
      throw new ValidationException("ScriptParams are mandatory!"); //TODO: Check if this is really needed for *all* EMR jobs (if not move to according sub-class)
    if (sparkParams == null)
      throw new ValidationException("SparkParams are mandatory!"); //TODO: Check if this is really needed for *all* EMR jobs (if not move to according sub-class)
    if (jarUrl == null)
      throw new ValidationException("JAR URL is mandatory!");
    //TODO: Move the ScriptParams length check into the according sub-class
    if (scriptParams.size() < 2)
      throw new ValidationException("ScriptParams length is to small!");

    return !isInputsExpected() || currentInputsCount(Input.class) > 0;
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

  public List<String> getScriptParams() {
    return scriptParams;
  }

  public void setScriptParams(List<String> scriptParams) {
    this.scriptParams = scriptParams;
  }

  public RunEmrJob withScriptParams(List<String> scriptParams) {
    setScriptParams(scriptParams);
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

  public boolean isInputsExpected() {
    return inputsExpected;
  }

  public void setInputsExpected(boolean inputsExpected) {
    this.inputsExpected = inputsExpected;
  }

  public RunEmrJob withInputsExpected(boolean inputsExpected) {
    setInputsExpected(inputsExpected);
    return this;
  }

  private String getLocalTmpPath(String s3Path) {
    final String localRootPath = "/tmp/";
    return localRootPath + s3Path;
  }

  /**
   * @param s3Path
   * @return Local path of tmp directory
   */
  private String copyFileFromS3ToLocal(String s3Path) {
    //Lambda allows writing to /tmp folder - Jar file could be bigger than 512MB
    try {
      logger.info("Copy file: '{}' to local.", s3Path);
      InputStream jarStream = S3Client.getInstance().streamObjectContent(s3Path);

      //Create local target Folder
      createLocalFolder(Paths.get(s3Path).getParent().toString(), false);
      Files.copy(jarStream, Paths.get(getLocalTmpPath(s3Path)));
      jarStream.close();
    }
    catch (FileAlreadyExistsException e) {
      logger.info("File: '{}' already exists locally - skip download.", s3Path);
    }
    catch (IOException e) {
      throw new RuntimeException("Can't copy File: '" + s3Path + "'!", e);
    }
    return getLocalTmpPath(s3Path);
  }

  /**
   * @param s3Path
   * @return Local path of tmp directory
   */
  private String copyFolderFromS3ToLocal(String s3Path) {
    List<S3ObjectSummary> s3ObjectSummaries = S3Client.getInstance().scanFolder(s3Path);

    for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
      if (!s3ObjectSummary.getKey().contains("modelBased"))
        copyFileFromS3ToLocal(s3ObjectSummary.getKey());
    }
    return getLocalTmpPath(s3Path);
  }

  private static void deleteDirectory(File directory) {
    if (directory.isDirectory()) {
      //Get all files and directories within the directory
      File[] files = directory.listFiles();
      if (files != null) {
        //Recursively delete each file and subdirectory
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    // Delete the directory or file
    directory.delete();
  }

  /**
   * @param s3Path
   * @return
   * @throws IOException
   */
  private String createLocalFolder(String s3Path, boolean deleteBefore) throws IOException {
    Path path = Paths.get(getLocalTmpPath(s3Path));

    //TODO: Use the step ID as prefix within /tmp instead
    if (deleteBefore)
      deleteDirectory(path.getParent().toFile());

    Files.createDirectories(path);

    return getLocalTmpPath(s3Path);
  }

  private void registerEmrOutputs(File emrOutputDir) throws IOException {
    if (emrOutputDir.exists() && emrOutputDir.isDirectory()) {
      File[] files = emrOutputDir.listFiles();

      if (files == null) {
        logger.info("EMR job has not produced any files!");
        return;
      }

      for (File file : files) {
        //TODO: check why this happens
        if (file.getPath().endsWith("crc"))
          continue;
        //TODO: skip _SUCCESS?

        logger.info("Register output for local file {} ", file);
        //TODO: Check if this is the correct content-type
        Output output = new DownloadUrl().withContentType("text").withContent(Files.readAllBytes(file.toPath()));
        registerOutputs(List.of(output), !isUseSystemOutput());
      }
    }
  }

  @Override
  public LambdaBasedStep.AsyncExecutionState getExecutionState() throws LambdaBasedStep.UnknownStateException {
    throw new UnknownStateException("RunEmrJob runs in SYNC mode only.");
  }

  @Override
  public LambdaBasedStep.ExecutionMode getExecutionMode() {
    return SYNC;
  }
}
