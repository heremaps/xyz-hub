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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.cloudwatchevents.model.CloudWatchEventsException;
import software.amazon.awssdk.services.cloudwatchevents.model.DeleteRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.PutTargetsResponse;
import software.amazon.awssdk.services.cloudwatchevents.model.RemoveTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ResourceNotFoundException;
import software.amazon.awssdk.services.cloudwatchevents.model.RuleState;
import software.amazon.awssdk.services.cloudwatchevents.model.Target;

@Disabled("Requires AWS credentials and access to CloudWatch Events. By using localstack the size validation does " +
        "not happens. Please configure a real job-step lambda (LAMBDA_ARN) in a AWS environment to run this test.")
public class LambdaBasedStepStateCheckTriggerIT {

  private CloudWatchEventsClient eventsClient;
  private String ruleName;
  private static final String LAMBDA_ARN = "TO_SET";

  @BeforeEach
  public void setup() {
    eventsClient = CloudWatchEventsClient.builder()
        .region(Region.EU_WEST_1)
        //.endpointOverride(URI.create("http://" + localstackHost + ":4566"))
        .build();

    ruleName = "it-state-check-" + UUID.randomUUID();
    eventsClient.putRule(PutRuleRequest.builder()
        .name(ruleName)
        .state(RuleState.ENABLED)
        .scheduleExpression("rate(1 minute)")
        .description("IT for CloudWatch target input size")
        .build());
  }

  @AfterEach
  public void cleanup() {
    if (eventsClient == null)
      return;

    try {
      eventsClient.removeTargets(RemoveTargetsRequest.builder().rule(ruleName).ids("state-check").build());
    }
    catch (Exception ignored) {
      // Best effort cleanup.
    }

    try {
      eventsClient.deleteRule(DeleteRuleRequest.builder().name(ruleName).build());
    }
    catch (ResourceNotFoundException ignored) {
      // Already gone.
    }
    catch (Exception ignored) {
      // Best effort cleanup.
    }

    eventsClient.close();
  }

  @Test
  public void putTargetFailsWithLargeStateCheckInputButSucceedsAfterCompaction() {
    String oversizedPayload = buildOversizedStateCheckPayload();
    assertTrue(oversizedPayload.length() > 8192, "Test payload must exceed CloudWatch target input limit.");

    CloudWatchEventsException ex = assertThrows(CloudWatchEventsException.class, () ->
        eventsClient.putTargets(PutTargetsRequest.builder()
            .rule(ruleName)
            .targets(Target.builder()
                .id("state-check-test")
                .arn(LAMBDA_ARN)
                .input(oversizedPayload)
                .build())
            .build()));

    assertTrue(ex.getMessage().contains("8192") || ex.getMessage().contains("targets"),
        "Expected validation to fail because target input exceeds 8192 bytes.");

    String compactedPayload = LambdaBasedStep.compactStateCheckInput(oversizedPayload);
    assertTrue(compactedPayload.length() < oversizedPayload.length(), "Compaction should reduce payload size.");
    assertTrue(compactedPayload.length() <= 8192, LAMBDA_ARN);

    PutTargetsResponse response = eventsClient.putTargets(PutTargetsRequest.builder()
        .rule(ruleName)
        .targets(Target.builder()
            .id("state-check")
            .arn(LAMBDA_ARN)
            .input(compactedPayload)
            .build())
        .build());

    assertEquals(0, response.failedEntryCount());
  }

  private static String buildOversizedStateCheckPayload() {
    String largeSpatialFilter = "x".repeat(12_000 );

    JsonObject payload = new JsonObject()
        .put("type", "STATE_CHECK")
        .put("step", new JsonObject()
            .put("type", "CopySpace")
            .put("id", "s_bahygt")
            .put("jobId", "okjcygbdjn")
            .put("taskToken", "token")
            .put("executionId", "arn:aws:states:us-east-1:000000000000:execution:job-large:job-large")
            .put("status", new JsonObject().put("state", "RUNNING"))
            .put("spatialFilter", new JsonObject()
                .put("geometry", new JsonObject()
                    .put("type", "MultiPolygon")
                    .put("coordinates", largeSpatialFilter))));

    return payload.encode();
  }
}

