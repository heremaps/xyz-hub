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

import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.util.List;

public class RunEmrJob extends Step<RunEmrJob> {

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

  @Override
  public void execute() throws Exception {
    //NOTE: As this step is just a "configuration holder", this method should never actually be called
    throw new RuntimeException("RunEmrJob#execute() was called.");
  }

  @Override
  public void resume() throws Exception {
    //NOTE: As this step is just a "configuration holder", this method should never actually be called
    throw new RuntimeException("RunEmrJob#execute() was called.");
  }

  @Override
  public void cancel() throws Exception {
    //NOTE: As this step is just a "configuration holder", this method should never actually be called
    throw new RuntimeException("RunEmrJob#execute() was called.");
  }

  @Override
  public boolean validate() throws ValidationException {
    return true;
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
}
