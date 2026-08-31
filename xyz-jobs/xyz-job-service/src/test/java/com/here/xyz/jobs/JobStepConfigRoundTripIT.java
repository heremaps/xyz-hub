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

package com.here.xyz.jobs;

import static com.here.xyz.jobs.datasets.files.FileFormat.EntityPerLine.Feature;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.datasets.FileOutputSettings;
import com.here.xyz.jobs.datasets.Files;
import com.here.xyz.jobs.datasets.files.GeoJson;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JobStepConfigRoundTripIT extends JobTest {

  private static final String JOBS_TABLE = "xyz-jobs-local";
  private static final String STEPS_TABLE = "xyz-job-steps-local";
  private static final String DYNAMO_ENDPOINT = "http://" + System.getProperty("job.host", "localhost") + ":8000";

  private final int featureCount = 20;

  @BeforeEach
  public void setUp() {
    createSpace(new Space().withId(SPACE_ID).withVersionsToKeep(10), false);
    putRandomFeatureCollectionToSpace(SPACE_ID, featureCount);
  }

  @Test
  public void jobStepGraphRoundTripsThroughTheStepConfigSplit() throws Exception {

    Job exportJob = buildExportJob();
    createSelfRunningJob(exportJob);
    checkSucceededJob(exportJob, featureCount);
    String jobId = exportJob.getId();

    //Load it back via the admin endpoint
    Map job = getJob(jobId, true);
    Map steps = (Map) job.get("steps");
    assertNotNull(steps, "the loaded job must have a step graph");

    List<String> hydratedStepIds = new ArrayList<>();
    assertFullyHydrated(steps, hydratedStepIds);
    assertFalse(hydratedStepIds.isEmpty(), "the step graph must contain steps");

    int overallStepCount = ((Number) ((Map) job.get("status")).get("overallStepCount")).intValue();
    assertEquals(overallStepCount, hydratedStepIds.size(), "every step of the job must be re-hydrated from the step-config table");

    DynamoDB db = documentClient();

    Item rawJobItem = db.getTable(JOBS_TABLE).getItem("id", jobId);
    assertNotNull(rawJobItem, "the job item must still be present after a successful run");
    assertTrue(rawJobItem.toJSON().contains("\"$ref\""),
        "the stored job item must reference its step configs ($ref), not inline them");

    List<Item> stepItems = queryStepConfigs(db, jobId);
    assertEquals(overallStepCount, stepItems.size(), "the step-config table must hold one item per step of the job");
    for (Item stepItem : stepItems) {
      assertEquals(jobId, stepItem.getString("jobId"), "each step item must be partitioned by its jobId");
      assertNotNull(stepItem.getString("id"), "each step item must carry its step id");
      assertNotNull(stepItem.getMap("step"), "each step item must carry its full step config");
      assertNotNull(stepItem.getString("state"), "each step item must carry its state");
    }

    deleteJob(jobId);
    createdJobs.remove(jobId);
    assertStepConfigsGone(db, jobId);
  }

  private Job buildExportJob() {
    return new Job()
        .withId(JOB_ID)
        .withDescription("Step-config split round-trip test")
        .withSource(new DatasetDescription.Space<>().withId(SPACE_ID).withVersionRef(new Ref("HEAD")))
        .withTarget(new Files<>().withOutputSettings(new FileOutputSettings().withFormat(new GeoJson().withEntityPerLine(Feature))));
  }

  @SuppressWarnings("unchecked")
  private static void assertFullyHydrated(Object node, List<String> stepIds) {
    if (!(node instanceof Map<?, ?> map))
      return;
    assertFalse(map.containsKey("$ref"), "no $ref placeholder should remain after de-referencing");
    if (map.get("executions") instanceof List<?> executions)
      executions.forEach(execution -> assertFullyHydrated(execution, stepIds));
    else if (map.get("id") != null) {
      assertNotNull(map.get("type"), "a hydrated step must carry its type");
      assertNotNull(map.get("status"), "a hydrated step must carry its status");
      assertTrue(map.size() > 2, "a hydrated step must carry its full config, not just an id");
      stepIds.add((String) map.get("id"));
    }
  }

  private static DynamoDB documentClient() {
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(new EndpointConfiguration(DYNAMO_ENDPOINT, "us-west-1"))
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
        .build();
    return new DynamoDB(client);
  }

  private static List<Item> queryStepConfigs(DynamoDB db, String jobId) {
    Table stepTable = db.getTable(STEPS_TABLE);
    List<Item> items = new ArrayList<>();
    stepTable.query("jobId", jobId).forEach(items::add);
    return items;
  }

  private static void assertStepConfigsGone(DynamoDB db, String jobId) throws InterruptedException {
    for (int attempt = 0; attempt < 30; attempt++) {
      if (queryStepConfigs(db, jobId).isEmpty())
        return;
      Thread.sleep(500);
    }
    assertTrue(queryStepConfigs(db, jobId).isEmpty(), "deleting the job must remove its step-config items (no orphans)");
  }
}
