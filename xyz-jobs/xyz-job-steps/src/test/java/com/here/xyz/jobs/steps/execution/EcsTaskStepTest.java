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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.BaseConfig;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EcsTaskStepTest {
  private Config previousConfig;
  private BaseConfig previousBaseConfig;

  @BeforeEach
  void setUp() {
    previousConfig = Config.instance;
    previousBaseConfig = BaseConfig.instance;
    new Config();
    Config.instance.ECS_CLUSTER_ARN = "test-cluster";
    Config.instance.ECS_TASK_DEFINITION = "test-steps:1";
    Config.instance.ECS_CONTAINER_NAME = "test-container";
    Config.instance.ECS_SUBNETS = "subnetA, subnetB";
    Config.instance.ECS_SECURITY_GROUPS = "groupA";
  }

  @AfterEach
  void tearDown() {
    Config.instance = previousConfig;
    BaseConfig.instance = previousBaseConfig;
  }

  @Test
  void deserializesAsLambdaBasedStep() throws Exception {
    EcsTaskStep step = new EcsTaskStep().withAdditionalEcsInfo("someInfo");

    //EcsTaskStep.main() reads the step the same way
    LambdaBasedStep<?> restored = XyzSerializable.deserialize(step.serialize(), LambdaBasedStep.class);

    EcsTaskStep restoredStep = assertInstanceOf(EcsTaskStep.class, restored);
    assertEquals(step.getId(), restoredStep.getId());
    assertEquals("someInfo", restoredStep.getAdditionalEcsInfo());
  }

  @Test
  void runTaskParametersFromConfiguration() {
    Map<String, Object> parameters = new EcsTaskStep().getEcsTaskConfig("somePayload");

    assertEquals("test-cluster", parameters.get("Cluster"));
    assertEquals("test-steps:1", parameters.get("TaskDefinition"));
    assertEquals("FARGATE", parameters.get("LaunchType"));
    assertEquals(Map.of("AwsvpcConfiguration", Map.of(
        "Subnets", List.of("subnetA", "subnetB"),
        "SecurityGroups", List.of("groupA"),
        "AssignPublicIp", "DISABLED")), parameters.get("NetworkConfiguration"));
    assertEquals(Map.of("ContainerOverrides", List.of(Map.of(
        "Name", "test-container",
        "Environment", List.of(Map.of("Name", "EcsTaskStep", "Value", "somePayload"))))), parameters.get("Overrides"));
  }

  @Test
  void assignPublicIpFromConfiguration() {
    Config.instance.ECS_ASSIGN_PUBLIC_IP = "ENABLED";

    Map<String, Object> network = (Map<String, Object>) new EcsTaskStep().getEcsTaskConfig("somePayload").get("NetworkConfiguration");

    assertEquals("ENABLED", ((Map<String, Object>) network.get("AwsvpcConfiguration")).get("AssignPublicIp"));
  }

  @Test
  void completeConfiguration() {
    assertDoesNotThrow(EcsTaskStep::checkConfiguration);
  }

  @Test
  void missingTaskDefinition() {
    Config.instance.ECS_TASK_DEFINITION = null;

    IllegalStateException e = assertThrows(IllegalStateException.class, EcsTaskStep::checkConfiguration);
    assertTrue(e.getMessage().contains("ECS_TASK_DEFINITION"), e.getMessage());
  }

  @Test
  void emptySecurityGroups() {
    //A deployment without ECS tasks sets the setting to an empty value
    Config.instance.ECS_SECURITY_GROUPS = "";

    IllegalStateException e = assertThrows(IllegalStateException.class, EcsTaskStep::checkConfiguration);
    assertTrue(e.getMessage().contains("ECS_SECURITY_GROUPS"), e.getMessage());
  }

  @Test
  void missingContainerName() {
    Config.instance.ECS_CONTAINER_NAME = "";

    IllegalStateException e = assertThrows(IllegalStateException.class, EcsTaskStep::checkConfiguration);
    assertTrue(e.getMessage().contains("ECS_CONTAINER_NAME"), e.getMessage());
  }
}
