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

import com.here.xyz.jobs.util.AwsClientFactory;
import io.vertx.core.Future;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryRequest;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryResponse;
import software.amazon.awssdk.services.sfn.model.HistoryEvent;
import software.amazon.awssdk.services.sfn.model.HistoryEventType;
import software.amazon.awssdk.services.sfn.model.StateEnteredEventDetails;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SFNInspectorTest {

  private static final String EXECUTION_ARN = "arn:aws:states:us-east-1:000000000000:execution:job-test:job-test";

  private SfnAsyncClient originalAsyncSfnClient;

  @BeforeEach
  void setUp() throws Exception {
    originalAsyncSfnClient = getAsyncSfnClient();
  }

  @AfterEach
  void tearDown() throws Exception {
    setAsyncSfnClient(originalAsyncSfnClient);
  }

  @Test
  void findStepProgressInHistoryShouldUsePagingAndResolveSucceeded() throws Exception {
    List<GetExecutionHistoryRequest> seenRequests = new ArrayList<>();

    setAsyncSfnClient(createAsyncSfnClient(request -> {
      seenRequests.add(request);
      if (request.nextToken() == null) {
        return GetExecutionHistoryResponse.builder()
            .events(
                entered(1L, "ExportSpaceToFiles.lgiateettp"),
                event(2L, 1L, HistoryEventType.TASK_SCHEDULED)
            )
            .nextToken("page-2")
            .build();
      }

      return GetExecutionHistoryResponse.builder()
          .events(
              event(3L, 2L, HistoryEventType.TASK_STARTED),
              event(4L, 3L, HistoryEventType.TASK_SUBMITTED),
              event(5L, 4L, HistoryEventType.TASK_SUCCEEDED)
          )
          .build();
    }));

    SFNInspector.StepProgress progress = await(SFNInspector.findStepProgressInHistory(EXECUTION_ARN, "ExportSpaceToFiles", "lgiateettp"));

    assertEquals(SFNInspector.StepProgress.SUCCEEDED, progress);
    assertEquals(2, seenRequests.size());
    assertEquals(1000, seenRequests.get(0).maxResults());
    assertEquals("page-2", seenRequests.get(1).nextToken());
  }

  @Test
  void checkIfStepWasRunningBeforeShouldReturnTrueWhenStartedButNotSucceeded() throws Exception {
    setAsyncSfnClient(createAsyncSfnClient(request -> GetExecutionHistoryResponse.builder()
        .events(
            entered(1L, "TaskedImportFilesToSpace.lgiateettp"),
            event(2L, 1L, HistoryEventType.TASK_SCHEDULED),
            event(3L, 2L, HistoryEventType.TASK_STARTED),
            event(4L, 3L, HistoryEventType.TASK_SUBMITTED)
        )
        .build()));

    Boolean resumable = await(SFNInspector.checkIfStepWasRunningBefore(EXECUTION_ARN, "TaskedImportFilesToSpace", "lgiateettp"));

    assertTrue(resumable);
  }

  @Test
  void checkIfStepWasRunningBeforeShouldReturnFalseWhenSucceeded() throws Exception {
    setAsyncSfnClient(createAsyncSfnClient(request -> GetExecutionHistoryResponse.builder()
            .events(
                    entered(1L, "TaskedImportFilesToSpace.lgiateettp"),
                    event(20L, 1L, HistoryEventType.TASK_SCHEDULED),
                    event(30L, 20L, HistoryEventType.TASK_STARTED),
                    event(40L, 30L, HistoryEventType.TASK_SUBMITTED),
                    event(50L, 40L, HistoryEventType.TASK_SUCCEEDED)
            )
            .build()));

    Boolean resumable = await(SFNInspector.checkIfStepWasRunningBefore(EXECUTION_ARN, "TaskedImportFilesToSpace", "lgiateettp"));

    assertFalse(resumable);
  }

  @Test
  void findStepProgressInHistoryShouldReturnNotReachedWhenChainIsBroken() throws Exception {
    setAsyncSfnClient(createAsyncSfnClient(request -> GetExecutionHistoryResponse.builder()
        .events(
            entered(1L, "TaskedImportFilesToSpace.lgiateettp"),
            event(2L, 99L, HistoryEventType.TASK_STARTED)
        )
        .build()));

    SFNInspector.StepProgress progress = await(SFNInspector.findStepProgressInHistory(EXECUTION_ARN, "ImportStep", "lgiateettp"));

    assertEquals(SFNInspector.StepProgress.NOT_REACHED, progress);
  }

  @Test
  void findCausingStepIdInHistoryShouldReturnStepIdFromTaskStateEntered() throws Exception {
    setAsyncSfnClient(createAsyncSfnClient(request -> GetExecutionHistoryResponse.builder()
        .events(
            entered(10L, "TaskedImportFilesToSpace.s_fnjkyu"),
            event(11L, 10L, HistoryEventType.TASK_SCHEDULED),
            event(12L, 11L, HistoryEventType.TASK_STARTED),
            event(13L, 12L, HistoryEventType.TASK_SUBMITTED),
            event(14L, 13L, HistoryEventType.TASK_FAILED)
        )
        .build()));

    String stepId = await(SFNInspector.findCausingStepIdInHistory(EXECUTION_ARN));

    assertEquals("s_fnjkyu", stepId);
  }

  @Test
  void findCausingStepIdInHistoryShouldFailWhenNoCausingTaskStateEnteredExists() throws Exception {
    setAsyncSfnClient(createAsyncSfnClient(request -> GetExecutionHistoryResponse.builder()
        .events(event(1L, 0L, HistoryEventType.EXECUTION_FAILED))
        .build()));

    RuntimeException error = assertThrows(RuntimeException.class,
        () -> await(SFNInspector.findCausingStepIdInHistory(EXECUTION_ARN)));

    assertTrue(error.getMessage().contains("Causing stepId not found"));
  }

  private static HistoryEvent entered(long id, String stepName) {
    return HistoryEvent.builder()
        .id(id)
        .type(HistoryEventType.TASK_STATE_ENTERED)
        .stateEnteredEventDetails(StateEnteredEventDetails.builder().name(stepName).build())
        .build();
  }

  private static HistoryEvent event(long id, long previousEventId, HistoryEventType type) {
    return HistoryEvent.builder()
        .id(id)
        .previousEventId(previousEventId)
        .type(type)
        .build();
  }

  private static <T> T await(Future<T> future) throws Exception {
    try {
      return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception ex) {
        throw ex;
      }
      if (cause instanceof Error err) {
        throw err;
      }
      throw e;
    }
  }

  private static SfnAsyncClient createAsyncSfnClient(
      java.util.function.Function<GetExecutionHistoryRequest, GetExecutionHistoryResponse> historyResponder) {

    return (SfnAsyncClient) Proxy.newProxyInstance(
        SfnAsyncClient.class.getClassLoader(),
        new Class[]{SfnAsyncClient.class},
        (proxy, method, args) -> {
          if ("getExecutionHistory".equals(method.getName())) {
            return CompletableFuture.completedFuture(historyResponder.apply((GetExecutionHistoryRequest) args[0]));
          }
          throw new UnsupportedOperationException("Method not supported in test proxy: " + method.getName());
        });
  }

  private static SfnAsyncClient getAsyncSfnClient() throws Exception {
    Field field = AwsClientFactory.class.getDeclaredField("asyncSfnClient");
    field.setAccessible(true);
    return (SfnAsyncClient) field.get(null);
  }

  private static void setAsyncSfnClient(SfnAsyncClient client) throws Exception {
    Field field = AwsClientFactory.class.getDeclaredField("asyncSfnClient");
    field.setAccessible(true);
    field.set(null, client);
  }
}

