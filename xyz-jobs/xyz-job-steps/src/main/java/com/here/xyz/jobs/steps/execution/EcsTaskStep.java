/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EcsTaskStep extends SyncLambdaStep<EcsTaskStep> {
  private static final Logger logger = LogManager.getLogger();
  private Map<String, Object> ecsTaskConfig;
  private String additionalEcsInfo;

  public void setEcsTaskConfig(Map<String, Object> ecsTaskConfig) {
    this.ecsTaskConfig = ecsTaskConfig;
  }

  public String getAdditionalEcsInfo() {
    return additionalEcsInfo;
  }

  public void setAdditionalEcsInfo(String additionalEcsInfo) {
    this.additionalEcsInfo = additionalEcsInfo;
  }

  public EcsTaskStep withAdditionalEcsInfo(String additionalEcsInfo) {
    setAdditionalEcsInfo(additionalEcsInfo);
    return this;
  }

  public Map<String, Object> getEcsTaskConfig(String payload) {
    if (ecsTaskConfig == null)
      ecsTaskConfig = defaultEcsTaskConfig();

    //The original map may be immutable
    ecsTaskConfig = new HashMap<>(ecsTaskConfig);

    ecsTaskConfig.put("Overrides", Map.of(
        "ContainerOverrides", List.of(
            Map.of(
                "Name", requiredSetting("ECS_CONTAINER_NAME", Config.instance.ECS_CONTAINER_NAME),
                "Environment", List.of(Map.of("Name", "EcsTaskStep", "Value", payload))
            ))
    ));

    logger.info("[{}] Created the RunTask parameters of the step.", getGlobalStepId());

    return ecsTaskConfig;
  }

  /**
   * Builds the RunTask parameters from the service configuration (see the ECS_* settings in {@link Config}).
   * The deployment (e.g. MapFlow) provides the actual values.
   */
  private static Map<String, Object> defaultEcsTaskConfig() {
    Config config = Config.instance;
    return Map.of(
        "Cluster", requiredSetting("ECS_CLUSTER_ARN", config.ECS_CLUSTER_ARN),
        "TaskDefinition", requiredSetting("ECS_TASK_DEFINITION", config.ECS_TASK_DEFINITION),
        "LaunchType", "FARGATE",
        "NetworkConfiguration", Map.of(
            "AwsvpcConfiguration", Map.of(
                "Subnets", requiredList("ECS_SUBNETS", config.ecsSubnets()),
                "SecurityGroups", requiredList("ECS_SECURITY_GROUPS", config.ecsSecurityGroups()),
                "AssignPublicIp", config.ECS_ASSIGN_PUBLIC_IP != null && !config.ECS_ASSIGN_PUBLIC_IP.isBlank()
                    ? config.ECS_ASSIGN_PUBLIC_IP : "DISABLED"
            )
        )
    );
  }

  /**
   * Checks that the service configuration has all settings the default RunTask parameters need.
   * @throws IllegalStateException naming the first missing setting
   */
  public static void checkConfiguration() {
    defaultEcsTaskConfig();
    requiredSetting("ECS_CONTAINER_NAME", Config.instance.ECS_CONTAINER_NAME);
  }

  private static String requiredSetting(String name, String value) {
    if (value == null || value.isBlank())
      throw new IllegalStateException("The setting " + name + " is required to run an " + EcsTaskStep.class.getSimpleName() + ".");
    return value;
  }

  private static List<String> requiredList(String name, List<String> values) {
    if (values.isEmpty())
      throw new IllegalStateException("The setting " + name + " is required to run an " + EcsTaskStep.class.getSimpleName() + ".");
    return values;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 60 * 60 * 6; //6h
  }

  @Override
  public String getDescription() {
    return "stepfunction running as standalone task on ecs";
  }

  @Override
  public void execute(boolean resume) throws Exception {
    logger.info("[{}] execute -> {}", getGlobalStepId(), getAdditionalEcsInfo());
  }

  @Override
  public boolean validate() throws ValidationException {
    return true;
  }

  /**
   * The entrypoint of the ECS task's container (see ecs.java.dockerfile).
   * Reads the step from the environment variable "EcsTaskStep" and executes it.
   */
  public static void main(String[] args) {
    try {
      //Initialize Config from environment variables, as the step lambda does (LambdaBasedStepExecutor). The steps read it on construction.
      if (Config.instance == null) {
        XyzSerializable.fromMap(Map.copyOf(System.getenv()), Config.class);
        LambdaBasedStepExecutor.loadPlugins();
      }

      LambdaBasedStep<?> step = XyzSerializable.deserialize(System.getenv("EcsTaskStep"), LambdaBasedStep.class);

      if (step instanceof EcsTaskStep ecsTaskStep) {
        logger.info("[{}] Starting the execution of the step ...", ecsTaskStep.getGlobalStepId());
        ecsTaskStep.execute(false);
        logger.info("[{}] Execution of the step has been completed.", ecsTaskStep.getGlobalStepId());
      }
      else
        throw new ValidationException(String.format("class %s must be instanceof EcsTaskStep", step.getClass().getCanonicalName()));
    }
    catch (Exception e) {
      logger.error("Error executing the ECS task step.", e);
      System.exit(1);
    }
  }
}
