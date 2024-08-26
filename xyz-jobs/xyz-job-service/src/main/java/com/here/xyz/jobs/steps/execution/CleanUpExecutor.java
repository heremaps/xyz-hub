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

import static com.here.xyz.jobs.util.AwsClients.cloudwatchEventsClient;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.config.DynamoJobConfigClient;
import com.here.xyz.jobs.steps.Step;
import io.vertx.core.Future;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.cloudwatchevents.model.DeleteRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ListRulesRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.ListRulesResponse;
import software.amazon.awssdk.services.cloudwatchevents.model.ListTargetsByRuleRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.RemoveTargetsRequest;
import software.amazon.awssdk.services.cloudwatchevents.model.Rule;
import software.amazon.awssdk.services.cloudwatchevents.model.Target;

public class CleanUpExecutor {
  private static final Logger logger = LogManager.getLogger();
  private static final long CHECK_PERIOD_IN_MIN = 30;
  private static final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
  private static final CleanUpExecutor instance = new CleanUpExecutor();
  private static final int CLEANUP_JOBS_OLDER_THAN = 14 * 24 * 60 * 60 * 1000;//14 Days
  //TODO: Think about caching

  public static CleanUpExecutor getInstance() {
    return instance;
  }

  public void start() {
    exec.scheduleAtFixedRate(() -> cleanUp(), 0, CHECK_PERIOD_IN_MIN, MINUTES);
  }

  private void cleanUp() {
    DynamoJobConfigClient.getInstance().loadNonResumableFailedJobsOlderThan(CLEANUP_JOBS_OLDER_THAN)
        .onSuccess(jobs -> {
          logger.info("Start cleanup of StateMachines ...");
          deleteStateMachines(jobs);

          logger.info("Start cleanup of CW Event Rules ...");
          retrieveAvailableCloudwatchRules()
              .compose(availableCwRules -> deleteOrphanedRulesIncludingTargets(availableCwRules, jobs));
        });
  }

  private Future<List<String>> retrieveAvailableCloudwatchRules() {
    //TODO: Asyncify!
    ListRulesResponse listRulesResponse = cloudwatchEventsClient().listRules(
        ListRulesRequest
            .builder()
            .namePrefix(LambdaBasedStep.HEART_BEAT_PREFIX)
            .build());

    try {
      return Future.succeededFuture(listRulesResponse.rules().stream().map(Rule::name).toList());
    }
    catch (Exception e) {
      logger.warn(e);
      return Future.failedFuture("Can't get Cloudwatch Rules");
    }
  }

  private void deleteStateMachines(List<Job> jobs) {
    jobs.stream().forEach(job -> JobExecutor.getInstance().deleteExecution(job.getExecutionId())
        .onSuccess(deleted -> {
          if (deleted)
            logger.info("[{}] Deleted state machine of execution {}", job.getId(), job.getExecutionId());
        })
        .onFailure(e -> logger.info("[{}] Can't delete state machine of execution {}", job.getId(), job.getExecutionId(), e)));
  }

  private Future<Void> deleteOrphanedRulesIncludingTargets(List<String> availableCwRules, List<Job> jobs) {
    jobs.stream().forEach(
        job -> {
          List<String> stepIds = job.getSteps().stepStream().map(Step::getGlobalStepId).toList();
          stepIds.forEach(stepId -> {
            String ruleName = LambdaBasedStep.HEART_BEAT_PREFIX + stepId;
            if (availableCwRules.contains(ruleName))
              deleteCloudWatchRule(ruleName, job.getId());
          });
        });
    return Future.succeededFuture();
  }

  private void deleteCloudWatchRule(String ruleName, String jobId) {
    try {
      deleteCloudWatchRuleTargets(ruleName);
      cloudwatchEventsClient().deleteRule(DeleteRuleRequest.builder().name(ruleName).build());
      logger.info("[{}] Deleted Cloudwatch-Rule and Target {}", jobId, ruleName);
    }
    catch (Exception e) {
      logger.warn("Cant delete rule {}!", ruleName, e);
    }
  }

  private Future<Void> deleteCloudWatchRuleTargets(String ruleName) {
    List<String> targetIds = cloudwatchEventsClient().listTargetsByRule(
            ListTargetsByRuleRequest.builder().rule(ruleName).build())
        .targets().stream().map(Target::id).toList();

    cloudwatchEventsClient().removeTargets(RemoveTargetsRequest.builder().ids(targetIds).rule(ruleName).build());
    return Future.succeededFuture();
  }
}
