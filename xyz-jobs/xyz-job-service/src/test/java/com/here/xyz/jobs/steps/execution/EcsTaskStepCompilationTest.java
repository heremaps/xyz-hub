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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.util.service.BaseConfig;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EcsTaskStepCompilationTest {
  private com.here.xyz.jobs.steps.Config previousStepConfig;
  private Config previousConfig;
  private BaseConfig previousBaseConfig;

  @BeforeEach
  void setUp() {
    previousStepConfig = com.here.xyz.jobs.steps.Config.instance;
    previousConfig = Config.instance;
    previousBaseConfig = BaseConfig.instance;
    new Config();
    Config.instance.LOCALSTACK_ENDPOINT = null;
    Config.instance.ECS_CLUSTER_ARN = "test-cluster";
    Config.instance.ECS_TASK_DEFINITION = "test-steps:1";
    Config.instance.ECS_CONTAINER_NAME = "test-container";
    Config.instance.ECS_SUBNETS = "subnetA,subnetB";
    Config.instance.ECS_SECURITY_GROUPS = "groupA";
  }

  @AfterEach
  void tearDown() {
    com.here.xyz.jobs.steps.Config.instance = previousStepConfig;
    Config.instance = previousConfig;
    BaseConfig.instance = previousBaseConfig;
  }

  @Test
  void stateMachineWithEcsState() throws Exception {
    EcsTaskStep step = new EcsTaskStep().withAdditionalEcsInfo("someInfo");
    StepGraph graph = new CompilationStepGraph().addExecution(step);
    CompilationStepGraph.enrich(graph, "someJob");

    String definition = new GraphTransformer(null, null, false, Map.of()).compileToStateMachine("someJob", graph);

    Map<String, Object> states = (Map<String, Object>) XyzSerializable.deserialize(definition, Map.class).get("States");
    Map<String, Object> state = (Map<String, Object>) states.get("EcsTaskStep." + step.getId());
    assertEquals("arn:aws:states:::ecs:runTask.sync", state.get("Resource"));

    Map<String, Object> parameters = (Map<String, Object>) state.get("Parameters");
    assertEquals("test-cluster", parameters.get("Cluster"));
    assertEquals("test-steps:1", parameters.get("TaskDefinition"));
    assertEquals("FARGATE", parameters.get("LaunchType"));
    assertEquals(Map.of("AwsvpcConfiguration", Map.of(
        "Subnets", List.of("subnetA", "subnetB"),
        "SecurityGroups", List.of("groupA"),
        "AssignPublicIp", "DISABLED")), parameters.get("NetworkConfiguration"));

    Map<String, Object> containerOverride = ((List<Map<String, Object>>) ((Map<String, Object>) parameters.get("Overrides"))
        .get("ContainerOverrides")).get(0);
    assertEquals("test-container", containerOverride.get("Name"));
    Map<String, Object> variable = ((List<Map<String, Object>>) containerOverride.get("Environment")).get(0);
    assertEquals("EcsTaskStep", variable.get("Name"));

    //The container reads the step from that variable (EcsTaskStep.main())
    EcsTaskStep restored = assertInstanceOf(EcsTaskStep.class,
        XyzSerializable.deserialize((String) variable.get("Value"), LambdaBasedStep.class));
    assertEquals(step.getId(), restored.getId());
    assertEquals("someJob", restored.getJobId());
    assertEquals("someInfo", restored.getAdditionalEcsInfo());
  }

  @Test
  void stateMachineWithoutEcsConfiguration() {
    //E.g. a service that has no ECS tasks configured
    Config.instance.ECS_TASK_DEFINITION = "";
    StepGraph graph = new CompilationStepGraph().addExecution(new EcsTaskStep());
    CompilationStepGraph.enrich(graph, "someJob");

    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new GraphTransformer(null, null, false, Map.of()).compileToStateMachine("someJob", graph));
    assertTrue(e.getMessage().contains("ECS_TASK_DEFINITION"), e.getMessage());
  }
}
