package com.here.xyz.jobs.steps.execution;

import io.vertx.core.Future;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionRequest;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionResponse;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryRequest;
import software.amazon.awssdk.services.sfn.model.HistoryEvent;
import software.amazon.awssdk.services.sfn.model.HistoryEventType;

import java.util.HashMap;
import java.util.Map;

import static com.here.xyz.jobs.util.AwsClientFactory.asyncSfnClient;
import static com.here.xyz.jobs.util.AwsClientFactory.sfnClient;
import static software.amazon.awssdk.services.sfn.model.HistoryEventType.TASK_STATE_ENTERED;
import static software.amazon.awssdk.services.sfn.model.HistoryEventType.TASK_STARTED;

public class SFNInspector {
  private final static int PAGE_SIZE = 1000;

  /**
   * Fetches the execution history for the provided executionArn and goes back in the event history
   * until hitting "TaskStateEntered".
   * Then extracts the causing step ID from stateEnteredEventDetails.name field.
   *
   * @param executionArn The execution ARN of the state machine
   * @return The ID of the causing step if found, `null` otherwise
   */
  public static Future<String> findCausingStepIdInHistory(String executionArn){
    return getFullExecutionHistoryByPaging(executionArn, null, new HashMap<>())
        .compose(eventsById -> {
          //Last element in the history should be the failing event
          HistoryEvent failingEvent = eventsById.values().stream()
              .max((a, b) -> Long.compare(a.id(), b.id()))
              .orElse(null);

          //we start from there and go backwards until finding a TaskStateEntered event
          while (failingEvent != null
              && failingEvent.previousEventId() > 0
              && failingEvent.type() != TASK_STATE_ENTERED) {
            failingEvent = eventsById.get(failingEvent.previousEventId());
          }

          //This should be the causing step event, if it exists
          String causingStepName = failingEvent != null
              && failingEvent.type() == TASK_STATE_ENTERED
              && failingEvent.stateEnteredEventDetails() != null
              && failingEvent.stateEnteredEventDetails().name().contains(".")
              ? failingEvent.stateEnteredEventDetails().name()
              : null;

          if (causingStepName == null)
            return Future.failedFuture(new RuntimeException("Causing stepId not found in SFN execution with ARN: " + executionArn));

          return Future.succeededFuture(getStepIdFromStepName(causingStepName));
        });
  }

  private static Future<Map<Long, HistoryEvent>> getFullExecutionHistoryByPaging(String executionArn, String nextToken,
                                                                                 Map<Long, HistoryEvent> eventsById) {
    GetExecutionHistoryRequest.Builder requestBuilder = GetExecutionHistoryRequest.builder()
        .executionArn(executionArn)
        .maxResults(PAGE_SIZE);

    if (nextToken != null && !nextToken.isBlank())
      requestBuilder.nextToken(nextToken);

    return Future.fromCompletionStage(asyncSfnClient().getExecutionHistory(requestBuilder.build()))
        .compose(executionHistory -> {
          for (HistoryEvent event : executionHistory.events())
            eventsById.put(event.id(), event);

          String newNextToken = executionHistory.nextToken();
          if (newNextToken != null && !newNextToken.isBlank())
            return getFullExecutionHistoryByPaging(executionArn, newNextToken, eventsById);

          return Future.succeededFuture(eventsById);
        });
  }

  /**
   * Checks whether the execution history contains a TaskStateEntered event whose
   * stateEnteredEventDetails.name matches the provided stepId.
   *
   * @param executionArn The execution ARN of the state machine.
   * @param stepId The step identifier to look for (matches stateEnteredEventDetails.name).
   * @return Future that resolves to true if such an event exists, otherwise false.
   */
  public static Future<Boolean> findStartedStepFunctionExecutionInHistory(String executionArn, String stepClassName, String stepId) {
    return containsEventTypeInHistory(executionArn, getStepName(stepClassName, stepId), TASK_STARTED, null);
  }

  private static Future<Boolean> containsEventTypeInHistory(String executionArn, String fullStepName,
                                                            HistoryEventType eventType, String nextToken) {
    GetExecutionHistoryRequest.Builder requestBuilder = GetExecutionHistoryRequest.builder()
        .executionArn(executionArn)
        .maxResults(PAGE_SIZE);

    if (nextToken != null && !nextToken.isBlank())
      requestBuilder.nextToken(nextToken);

    return Future.fromCompletionStage(asyncSfnClient().getExecutionHistory(requestBuilder.build()))
        .compose(executionHistory -> {
          boolean found = executionHistory.events().stream()
              .filter(event -> event.type() == eventType)
              .anyMatch(event -> event.stateEnteredEventDetails() != null
                  && fullStepName.equals(event.stateEnteredEventDetails().name()));

          if (found)
            return Future.succeededFuture(true);

          String newNextToken = executionHistory.nextToken();
          if (newNextToken != null && !newNextToken.isBlank())
            return containsEventTypeInHistory(executionArn, fullStepName, eventType, newNextToken);

          return Future.succeededFuture(false);
        });
  }

  public static ExecutionStatus getSFNExecutionStatus(String executionArn){
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
