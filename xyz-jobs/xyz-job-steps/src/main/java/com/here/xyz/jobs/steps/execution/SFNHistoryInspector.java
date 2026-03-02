package com.here.xyz.jobs.steps.execution;

import io.vertx.core.Future;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryRequest;
import software.amazon.awssdk.services.sfn.model.HistoryEvent;

import java.util.List;

import static com.here.xyz.jobs.util.AwsClientFactory.asyncSfnClient;

public class SFNHistoryInspector {

  /**
   * Fetches the execution history for the provided executionArn and goes back in the event history
   * until hitting "TaskStateEntered".
   * Then extracts the causing step ID from stateEnteredEventDetails.name field.
   *
   * @param executionArn The execution ARN of the state machine
   * @return The ID of the causing step if found, `null` otherwise
   */
  public static Future<String> loadCausingStepId(String executionArn){
    return Future.fromCompletionStage(asyncSfnClient().getExecutionHistory(GetExecutionHistoryRequest.builder()
                    .executionArn(executionArn)
                    .build()))
            .compose(executionHistory -> {
              List<HistoryEvent> events = executionHistory.events();
              HistoryEvent failingEvent = events.get(events.size() - 1);
              while (failingEvent != null && failingEvent.previousEventId() > 0 && !"TaskStateEntered".equals(failingEvent.type().toString())) {
                long causingEventId = failingEvent.previousEventId();
                failingEvent = events.stream().filter(event -> event.id().equals(causingEventId)).findAny().orElse(null);
              }
              String causingStepName = failingEvent != null && "TaskStateEntered".equals(failingEvent.type().toString())
                      && failingEvent.stateEnteredEventDetails() != null && failingEvent.stateEnteredEventDetails().name().contains(".")
                      ? failingEvent.stateEnteredEventDetails().name() : null;
              if (causingStepName == null)
                return Future.failedFuture(new RuntimeException("Causing stepId not found in SFN execution with ARN: " + executionArn));
              String causingStepId = causingStepName.substring(causingStepName.indexOf(".") + 1);
              return Future.succeededFuture(causingStepId);
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
  public static Future<Boolean> findStepFunctionExecutionInHistory(String executionArn, String stepId) {
    return Future.fromCompletionStage(asyncSfnClient().getExecutionHistory(GetExecutionHistoryRequest.builder()
            .executionArn(executionArn)
            .build()))
            .map(executionHistory -> executionHistory.events().stream()
                .filter(event -> "TaskStateEntered".equals(event.type().toString()))
                .anyMatch(event -> event.stateEnteredEventDetails() != null
                    && stepId.equals(event.stateEnteredEventDetails().name())));
  }
}
