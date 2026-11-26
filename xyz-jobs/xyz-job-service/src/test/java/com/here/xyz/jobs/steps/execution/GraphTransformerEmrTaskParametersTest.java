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
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.util.ARN;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GraphTransformerEmrTaskParametersTest {
  private static final String APPLICATION_ID = "app-1";
  private static final String EXECUTION_ROLE_ARN = "arn:aws:iam::000000000000:role/test";
  private static final String JAR_URL = "s3://test-bucket/test.jar";
  private static final String INPUT_PATH = "s3://test-bucket/in/";
  private static final String OUTPUT_PATH = "s3://test-bucket/out/";
  private static final String SPARK_PARAMS = "--class Test";
  private static final String LOG_GROUP_NAME = "/test/log-group";
  private static final Map<String, List<String>> DEFAULT_LOG_TYPES = Map.of(
      "SPARK_DRIVER", List.of("STDERR", "STDOUT"),
      "SPARK_EXECUTOR", List.of("STDERR", "STDOUT"));
  private static final Map<String, List<String>> DRIVER_LOG_TYPES = Map.of("SPARK_DRIVER", List.of("STDERR", "STDOUT"));

  @BeforeAll
  public static void setUp() {
    if (Config.instance == null)
      new Config();
  }

  @Test
  public void defaultParametersAreUnchanged() {
    RunEmrJob step = emrStep();

    assertEquals(Map.of(
        "Name", step.getEmrJobName(),
        "ApplicationId", APPLICATION_ID,
        "ExecutionRoleArn", EXECUTION_ROLE_ARN,
        "ExecutionTimeoutMinutes", 12 * 60,
        "Tags", step.getTags(),
        "ConfigurationOverrides", Map.of(
            "MonitoringConfiguration", Map.of(
                "CloudWatchLoggingConfiguration", Map.of(
                    "Enabled", true,
                    "LogTypes", Map.of(
                        "SPARK_DRIVER", List.of("STDERR", "STDOUT"),
                        "SPARK_EXECUTOR", List.of("STDERR", "STDOUT")
                    )
                )
            )
        ),
        "JobDriver", Map.of(
            "SparkSubmit", Map.of(
                "EntryPoint", JAR_URL,
                "EntryPointArguments", List.of(INPUT_PATH, OUTPUT_PATH),
                "SparkSubmitParameters", SPARK_PARAMS
            )
        )
    ), GraphTransformer.emrTaskParameters(step));
  }

  @Test
  public void logGroupIsSet() {
    RunEmrJob step = emrStep().withCloudWatchLogGroupName(LOG_GROUP_NAME);

    assertEquals(Map.of("Enabled", true, "LogGroupName", LOG_GROUP_NAME, "LogTypes", DEFAULT_LOG_TYPES),
        cloudWatchLoggingConfiguration(GraphTransformer.emrTaskParameters(step)));
  }

  @Test
  public void driverLogsOnly() {
    RunEmrJob step = emrStep().withCloudWatchLogTypes(DRIVER_LOG_TYPES);

    Map<String, Object> cloudWatchLoggingConfiguration = cloudWatchLoggingConfiguration(GraphTransformer.emrTaskParameters(step));
    assertEquals(DRIVER_LOG_TYPES, cloudWatchLoggingConfiguration.get("LogTypes"));
    assertFalse(cloudWatchLoggingConfiguration.containsKey("LogGroupName"));
  }

  @Test
  public void compiledStateMachineContainsTheLoggingConfiguration() {
    URI previousLocalstackEndpoint = Config.instance.LOCALSTACK_ENDPOINT;
    Config.instance.LOCALSTACK_ENDPOINT = null;
    try {
      RunEmrJob step = emrStep().withCloudWatchLogGroupName(LOG_GROUP_NAME).withCloudWatchLogTypes(DRIVER_LOG_TYPES);

      String stateMachineDefinition = new GraphTransformer(new ARN("arn:aws:lambda:us-east-1:000000000000:function:test"), null,
          false, Map.of()).compileToStateMachine("test", new StepGraph().addExecution(step));

      JsonObject cloudWatchLoggingConfiguration = new JsonObject(stateMachineDefinition)
          .getJsonObject("States")
          .getJsonObject("RunEmrJob." + step.getId())
          .getJsonObject("Parameters")
          .getJsonObject("ConfigurationOverrides")
          .getJsonObject("MonitoringConfiguration")
          .getJsonObject("CloudWatchLoggingConfiguration");
      assertEquals(LOG_GROUP_NAME, cloudWatchLoggingConfiguration.getString("LogGroupName"));
      assertEquals(new JsonObject().put("SPARK_DRIVER", new JsonArray().add("STDERR").add("STDOUT")),
          cloudWatchLoggingConfiguration.getJsonObject("LogTypes"));
    }
    finally {
      Config.instance.LOCALSTACK_ENDPOINT = previousLocalstackEndpoint;
    }
  }

  private static RunEmrJob emrStep() {
    return new RunEmrJob()
        .withApplicationId(APPLICATION_ID)
        .withExecutionRoleArn(EXECUTION_ROLE_ARN)
        .withJarUrl(JAR_URL)
        .withPositionalScriptParams(List.of(INPUT_PATH, OUTPUT_PATH))
        .withSparkParams(SPARK_PARAMS)
        .withJobId("job1");
  }

  private static Map<String, Object> cloudWatchLoggingConfiguration(Map<String, Object> emrTaskParameters) {
    Map<String, Object> configurationOverrides = (Map<String, Object>) emrTaskParameters.get("ConfigurationOverrides");
    Map<String, Object> monitoringConfiguration = (Map<String, Object>) configurationOverrides.get("MonitoringConfiguration");
    return (Map<String, Object>) monitoringConfiguration.get("CloudWatchLoggingConfiguration");
  }
}
