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

import io.vertx.core.Future;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionRequest;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionResponse;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryRequest;
import software.amazon.awssdk.services.sfn.model.HistoryEvent;
import software.amazon.awssdk.services.sfn.model.StateEnteredEventDetails;

import java.util.HashMap;
import java.util.Map;

import static com.here.xyz.jobs.util.AwsClientFactory.asyncSfnClient;
import static com.here.xyz.jobs.util.AwsClientFactory.sfnClient;
import static software.amazon.awssdk.services.sfn.model.HistoryEventType.TASK_STATE_ENTERED;

public class SFNInspector {
  private static final int PAGE_SIZE = 1000;

  public enum StepProgress {
    NOT_REACHED,
    ENTERED,
    SCHEDULED,
    STARTED,
    SUBMITTED,
    SUCCEEDED;

    public boolean hasEntered() {
      return this.ordinal() >= ENTERED.ordinal();
    }

    public boolean hasStartedExecution() {
      return this.ordinal() >= STARTED.ordinal();
    }

    public boolean hasBeenSubmitted() {
      return this.ordinal() >= SUBMITTED.ordinal();
    }

    public boolean isSucceeded() {
      return this.ordinal() == SUCCEEDED.ordinal();
    }

    public boolean isResumable() {
      // TASK_STARTED can occur although the worker did not effectively begin step logic.
      // Require TASK_SUBMITTED to avoid resuming steps that never really executed.
      return !isSucceeded() && hasBeenSubmitted();
    }
  }

  /**
   * Fetches the execution history for the provided executionArn and goes back in the event history
   * until hitting "TaskStateEntered".
   * Then extracts the causing step ID from stateEnteredEventDetails.name field.
   *
   * @param executionArn The execution ARN of the state machine
   * @return The ID of the causing step if found
   */
  public static Future<String> findCausingStepIdInHistory(String executionArn) {
    return findCausingStepFailureInHistory(executionArn).map(StepFailureInfo::stepId);
  }

  /**
   * Fetches the causing step together with error information supplied by Step Functions. The failure information acts as a fallback
   * when a Lambda step could not synchronize its status with the job service.
   *
   * @param executionArn The ARN of the state machine execution
   * @return The causing step and the most specific failure details available in the event chain
   */
  public static Future<StepFailureInfo> findCausingStepFailureInHistory(String executionArn) {
    return getFullExecutionHistoryByPaging(executionArn, null, new HashMap<>())
            .map(eventsById -> {
              //Last element in the history should be the failing event
              HistoryEvent failingEvent = eventsById.values().stream()
                      .max((a, b) -> Long.compare(a.id(), b.id()))
                      .orElse(null);
              String error = null;
              String cause = null;

              //we start from there and go backwards until finding a TaskStateEntered event
              while (failingEvent != null && failingEvent.type() != TASK_STATE_ENTERED) {
                FailureDetails details = failureDetails(failingEvent);
                if (details != null) {
                  if (details.error() != null && !details.error().isBlank())
                    error = details.error();
                  if (details.cause() != null && !details.cause().isBlank())
                    cause = details.cause();
                }

                if (failingEvent.previousEventId() == null || failingEvent.previousEventId() <= 0) {
                  failingEvent = null;
                  break;
                }
                failingEvent = eventsById.get(failingEvent.previousEventId());
              }

              String causingStepName = failingEvent != null
                      && failingEvent.type() == TASK_STATE_ENTERED
                      && failingEvent.stateEnteredEventDetails() != null
                      && failingEvent.stateEnteredEventDetails().name() != null
                      && failingEvent.stateEnteredEventDetails().name().contains(".")
                      ? failingEvent.stateEnteredEventDetails().name()
                      : null;

              if (causingStepName == null) {
                throw new IllegalStateException("Causing stepId not found in SFN execution with ARN: " + executionArn);
              }

              return new StepFailureInfo(getStepIdFromStepName(causingStepName), error, cause);
            });
  }

  private static FailureDetails failureDetails(HistoryEvent event) {
    return switch (event.type()) {
      case TASK_FAILED -> event.taskFailedEventDetails() == null ? null
          : new FailureDetails(event.taskFailedEventDetails().error(), event.taskFailedEventDetails().cause());
      case LAMBDA_FUNCTION_FAILED -> event.lambdaFunctionFailedEventDetails() == null ? null
          : new FailureDetails(event.lambdaFunctionFailedEventDetails().error(), event.lambdaFunctionFailedEventDetails().cause());
      case EXECUTION_FAILED -> event.executionFailedEventDetails() == null ? null
          : new FailureDetails(event.executionFailedEventDetails().error(), event.executionFailedEventDetails().cause());
      default -> null;
    };
  }

  public record StepFailureInfo(String stepId, String error, String cause) {}

  private record FailureDetails(String error, String cause) {}

  public static Future<StepProgress> findStepProgressInHistory(
          String executionArn, String stepClassName, String stepId) {
    String fullStepName = getStepName(stepClassName, stepId);

    return getFullExecutionHistoryByPaging(executionArn, null, new HashMap<>())
            .map(eventsById -> resolveStepProgress(eventsById, fullStepName));
  }

  public static Future<Boolean> checkIfStepWasRunningBefore(
          String executionArn, String stepClassName, String stepId) {
    return findStepProgressInHistory(executionArn, stepClassName, stepId)
            .map(StepProgress::isResumable);
  }

  private static Future<Map<Long, HistoryEvent>> getFullExecutionHistoryByPaging(
          String executionArn,
          String nextToken,
          Map<Long, HistoryEvent> eventsById) {

    GetExecutionHistoryRequest.Builder requestBuilder = GetExecutionHistoryRequest.builder()
            .executionArn(executionArn)
            .maxResults(PAGE_SIZE);

    if (nextToken != null && !nextToken.isBlank()) {
      requestBuilder.nextToken(nextToken);
    }

    return Future.fromCompletionStage(asyncSfnClient().getExecutionHistory(requestBuilder.build()))
            .compose(executionHistory -> {
              for (HistoryEvent event : executionHistory.events()) {
                eventsById.put(event.id(), event);
              }

              String newNextToken = executionHistory.nextToken();
              if (newNextToken != null && !newNextToken.isBlank()) {
                return getFullExecutionHistoryByPaging(executionArn, newNextToken, eventsById);
              }

              return Future.succeededFuture(eventsById);
            });
  }

  private static StepProgress resolveStepProgress(Map<Long, HistoryEvent> eventsById, String fullStepName) {
    StepProgress best = StepProgress.NOT_REACHED;

    for (HistoryEvent event : eventsById.values()) {
      StepProgress progress = resolveProgressForEvent(event, eventsById, fullStepName);
      if (progress.ordinal() > best.ordinal()) {
        best = progress;
      }
    }

    return best;
  }

  private static StepProgress resolveProgressForEvent(
          HistoryEvent event,
          Map<Long, HistoryEvent> eventsById,
          String fullStepName) {

    if (event == null || event.type() == null) {
      return StepProgress.NOT_REACHED;
    }

    switch (event.type()) {
      case TASK_STATE_ENTERED:
        return matchesEnteredEvent(event, fullStepName)
                ? StepProgress.ENTERED
                : StepProgress.NOT_REACHED;

      case TASK_SCHEDULED:
        return matchesByWalkingBackToEntered(event, eventsById, fullStepName, StepProgress.SCHEDULED);

      case TASK_STARTED:
        return matchesByWalkingBackToEntered(event, eventsById, fullStepName, StepProgress.STARTED);

      case TASK_SUBMITTED:
        return matchesByWalkingBackToEntered(event, eventsById, fullStepName, StepProgress.SUBMITTED);

      case TASK_SUCCEEDED:
        return matchesByWalkingBackToEntered(event, eventsById, fullStepName, StepProgress.SUCCEEDED);

      default:
        return StepProgress.NOT_REACHED;
    }
  }

  private static boolean matchesEnteredEvent(HistoryEvent event, String fullStepName) {
    StateEnteredEventDetails details = event.stateEnteredEventDetails();
    return details != null && fullStepName.equals(details.name());
  }

  private static StepProgress matchesByWalkingBackToEntered(
          HistoryEvent event,
          Map<Long, HistoryEvent> eventsById,
          String fullStepName,
          StepProgress progressIfMatched) {

    HistoryEvent enteredEvent = findOwningTaskStateEntered(event, eventsById);
    if (enteredEvent == null) {
      return StepProgress.NOT_REACHED;
    }

    return matchesEnteredEvent(enteredEvent, fullStepName)
            ? progressIfMatched
            : StepProgress.NOT_REACHED;
  }

  /**
   * Walk backward via previousEventId until the owning TASK_STATE_ENTERED is found.
   * Expected Chain:
   * TASK_SUBMITTED -> TASK_STARTED   -> TASK_SCHEDULED -> TASK_STATE_ENTERED
   */
  private static HistoryEvent findOwningTaskStateEntered(
          HistoryEvent event,
          Map<Long, HistoryEvent> eventsById) {

    HistoryEvent current = event;

    while (current != null
            && current.previousEventId() != null
            && current.previousEventId() > 0) {

      HistoryEvent previous = eventsById.get(current.previousEventId());
      if (previous == null) {
        return null;
      }

      if (previous.type() == TASK_STATE_ENTERED) {
        return previous;
      }

      current = previous;
    }

    return null;
  }

  public static ExecutionStatus getSFNExecutionStatus(String executionArn) {
    DescribeExecutionRequest request = DescribeExecutionRequest.builder()
            .executionArn(executionArn)
            .build();

    DescribeExecutionResponse response = sfnClient().describeExecution(request);
    //RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED | PENDING_REDRIVE
    return response.status();
  }

  private static String getStepName(String stepClassName, String stepId) {
    return stepClassName + "." + stepId;
  }

  private static String getStepIdFromStepName(String stepName) {
    return stepName.substring(stepName.indexOf(".") + 1);
  }
}