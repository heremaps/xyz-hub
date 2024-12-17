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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RunEmrJob extends LambdaBasedStep<RunEmrJob> {

  private static final Logger logger = LogManager.getLogger();
  private static final String INPUT_SET_REF_PREFIX = "${inputSet:";
  private static final String INPUT_SET_REF_SUFFIX = "}";
  private static final Pattern INPUT_SET_REF_PATTERN = Pattern.compile("\\$\\{inputSet:([a-zA-Z0-9._=-]+)\\}");
  public static final String USER_REF = "USER";

  private String applicationId;
  private String executionRoleArn;
  private String jarUrl;
  private List<String> scriptParams;
  private String sparkParams;

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
    List<String> scriptParams = new ArrayList<>(getResolvedScriptParams());

    //Create the local target directory in which EMR writes the output
    String localTmpOutputsFolder = createLocalFolder(S3Client.getKeyFromS3Uri(scriptParams.get(1)), true);
    String s3TargetDir = scriptParams.get(1);

    //Download EMR executable JAR from S3 to local
    String localJarPath = copyFileFromS3ToLocal(jarUrl);
    //Copy step input files from S3 to local /tmp
    String localTmpInputsFolder = copyFolderFromS3ToLocal(S3Client.getKeyFromS3Uri(scriptParams.get(0)));

    //override params with local paths
    scriptParams.set(0, localTmpInputsFolder);
    scriptParams.set(1, localTmpOutputsFolder);

    for (int i = 0; i < scriptParams.size(); i++) {
      String baseDirKey = "--baseInputDir=";
      if(scriptParams.get(i).startsWith(baseDirKey)){
        String localTmpBaseInputsFolder = copyFolderFromS3ToLocal(
                S3Client.getKeyFromS3Uri(scriptParams.get(i).substring(baseDirKey.length())));
        scriptParams.set(i, baseDirKey + localTmpBaseInputsFolder);
      }
    }

    scriptParams.add("--local");

    sparkParams = sparkParams.replace("$localJarPath$", localJarPath);
    sparkParams = "java -Xshare:off --add-exports=java.base/java.nio=ALL-UNNAMED "
            + "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED "
            + "--add-exports=java.base/java.lang.invoke=ALL-UNNAMED "
            + "--add-exports=java.base/java.util=ALL-UNNAMED "
            + sparkParams;

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

    //Upload EMR files, which are stored locally
    uploadEMRResultsToS3(new File(localTmpOutputsFolder), S3Client.getKeyFromS3Uri(s3TargetDir));
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

    return !isUserInputsExpected() || isUserInputsPresent(Input.class);
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
    } catch (FileAlreadyExistsException e) {
      logger.info("File: '{}' already exists locally - skip download.", s3Path);
    }catch (AmazonS3Exception e){
      throw new RuntimeException("Can't download File: '" + s3Path + "' for local copy!", e);
    } catch (IOException e) {
      throw new RuntimeException("Can't copy File: '" + s3Path + "'!", e);
    }
    return getLocalTmpPath(s3Path);
  }

  /**
   * @param s3Path
   * @return Local path of tmp directory
   */
  private String copyFolderFromS3ToLocal(String s3Path) {
    //TODO: Use inputs loading of framework instead
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

    logger.info("Create tmp dir: " + path);
    Files.createDirectories(path);

    return getLocalTmpPath(s3Path);
  }

  private void uploadEMRResultsToS3(File emrOutputDir, String s3TargetPath) throws IOException {
    if (emrOutputDir.exists() && emrOutputDir.isDirectory()) {
      File[] files = emrOutputDir.listFiles();

      if (files == null) {
        logger.info("EMR job has not produced any files!");
        return;
      }

      for (File file : files) {
        //TODO: check why this happens & skip _SUCCESS
        if (file.getPath().endsWith("crc"))
          continue;

        if(file.isDirectory()) {
          logger.info("Folder detected {} ", file);
          uploadEMRResultsToS3(file, s3TargetPath + file.getName());
          continue;
        }

        logger.info("Store local file {} to {} ", file, s3TargetPath);
        //TODO: Check if this is the correct content-type
        new DownloadUrl()
                .withContentType("text")
                .withContent(Files.readAllBytes(file.toPath()))
                .store(s3TargetPath + "/" + UUID.randomUUID());
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
    return inputSet.name() == null ? USER_REF : (inputSet.stepId() + "." + inputSet.name());
  }

  private InputSet fromReferenceIdentifier(String referenceIdentifier) {
    return USER_REF.equals(referenceIdentifier) ? getInputSet(null, null)
        : getInputSet(referenceIdentifier.substring(0, referenceIdentifier.indexOf(".")),
            referenceIdentifier.substring(referenceIdentifier.indexOf(".") + 1));
  }

  protected InputSet getInputSet(String stepId, String name) {
    try {
      return getInputSets().stream()
          .filter(inputSet -> Objects.equals(inputSet.name(), name) && Objects.equals(inputSet.stepId(), stepId))
          .findFirst()
          .get();
    }
    catch (NoSuchElementException e) {
      throw new IllegalArgumentException("No input set \"" + (name == null ? "<USER-INPUTS>" : stepId + "." + name) + "\" exists in step \"" + getId() + "\"");
    }
  }

  List<String> getResolvedScriptParams() {
    //Replace potential inputSet references within the script params
    return getScriptParams().stream().map(scriptParam -> replaceInputSetReferences(scriptParam)).toList();
  }

  private String replaceInputSetReferences(String scriptParam) {
    return INPUT_SET_REF_PATTERN.matcher(scriptParam)
        .replaceAll(match -> toS3Uri(fromReferenceIdentifier(match.group(1)).toS3Path(getJobId())));
  }

  private String toS3Uri(String s3Path) {
    return "s3://" + Config.instance.JOBS_S3_BUCKET + "/" + s3Path + "/";
  }
}
