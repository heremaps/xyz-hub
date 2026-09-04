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

package com.here.xyz.jobs.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

class EmrErrorInfoResolverTest {
  private static Vertx originalVertx;

  @BeforeAll
  static void initializeVertx() {
    originalVertx = Core.vertx;
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
  }

  @AfterAll
  static void closeVertx() throws Exception {
    if (originalVertx == null)
      Core.vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    Core.vertx = originalVertx;
  }

  @Test
  void extractsMissingProfileErrorFromNestedStackTrace() {
    String stackTrace = """
        java.lang.RuntimeException: Failed to process a microbatch.
        \tat example.Workflow.run(Workflow.scala:77)
        Caused by: com.here.cs.features.operation.exceptions.ConfigurationNotFoundException:
        E316053 [Not_Existing_Profile_for_test] Configuration not found for profile.
        \tat com.here.cs.configuration.ConfigurationProvider.provide(ConfigurationProvider.java:52)
        """;

    assertEquals("E316053 [Not_Existing_Profile_for_test] Configuration not found for profile.",
        EmrErrorInfoResolver.extractMissingProfileError(List.of(stackTrace)));
  }

  @Test
  void preservesArbitraryProfileName() {
    String message = "Caused by: ConfigurationNotFoundException: E316053 [customer-map-profile-v7] Configuration not found for profile.";

    assertEquals("E316053 [customer-map-profile-v7] Configuration not found for profile.",
        EmrErrorInfoResolver.extractMissingProfileError(List.of(message)));
  }

  @Test
  void ignoresUnrelatedErrors() {
    assertNull(EmrErrorInfoResolver.extractMissingProfileError(List.of(
        "E316052 [profile] Some other configuration error.",
        "java.lang.IllegalStateException: Spark failed")));
  }

  @Test
  void retriesUntilTheDriverLogBecomesAvailable() throws Exception {
    AtomicInteger reads = new AtomicInteger();
    EmrErrorInfoResolver resolver = new EmrErrorInfoResolver((applicationId, jobRunId) ->
        reads.incrementAndGet() < 3 ? List.of() : List.of(
            "E316053 [late-profile] Configuration not found for profile."), new long[] {0, 1, 1});

    assertEquals("E316053 [late-profile] Configuration not found for profile.",
        await(resolver.resolveCause("app", "run", "generic state details")));
    assertEquals(3, reads.get());
  }

  @Test
  void fallsBackToStateDetailsWhenNoMatchingErrorExists() throws Exception {
    EmrErrorInfoResolver resolver = new EmrErrorInfoResolver(
        (applicationId, jobRunId) -> List.of("Unrelated driver failure"), new long[] {0});

    assertEquals("generic state details", await(resolver.resolveCause("app", "run", "generic state details")));
  }

  @Test
  void fallsBackToStateDetailsWhenCloudWatchIsUnavailable() throws Exception {
    EmrErrorInfoResolver resolver = new EmrErrorInfoResolver((applicationId, jobRunId) -> {
      throw new IllegalStateException("Access denied");
    }, new long[] {0});

    assertEquals("generic state details", await(resolver.resolveCause("app", "run", "generic state details")));
  }

  @Test
  void discoversRetryAttemptStreamsAndPrefersDriverStderr() {
    List<Object> requests = new ArrayList<>();
    String stdoutStream = "/applications/app/jobs/run/SPARK_DRIVER/stdout";
    String retryStderrStream = "/applications/app/jobs/run/attempts/2/SPARK_DRIVER/stderr";
    CloudWatchLogsClient client = cloudWatchClient(requests, stdoutStream, retryStderrStream);
    EmrErrorInfoResolver.CloudWatchEmrLogReader reader = new EmrErrorInfoResolver.CloudWatchEmrLogReader(client);

    List<String> messages = reader.read("app", "run");

    DescribeLogStreamsRequest describeRequest = (DescribeLogStreamsRequest) requests.get(0);
    assertEquals(EmrErrorInfoResolver.LOG_GROUP, describeRequest.logGroupName());
    assertEquals("/applications/app/jobs/run", describeRequest.logStreamNamePrefix());
    GetLogEventsRequest firstLogRequest = (GetLogEventsRequest) requests.get(1);
    assertEquals(retryStderrStream, firstLogRequest.logStreamName());
    assertTrue(messages.contains("E316053 [retry-profile] Configuration not found for profile."));
  }

  private static CloudWatchLogsClient cloudWatchClient(List<Object> requests, String stdoutStream, String retryStderrStream) {
    return (CloudWatchLogsClient) Proxy.newProxyInstance(CloudWatchLogsClient.class.getClassLoader(),
        new Class<?>[] {CloudWatchLogsClient.class}, (proxy, method, args) -> {
          if ("describeLogStreams".equals(method.getName())) {
            requests.add(args[0]);
            return DescribeLogStreamsResponse.builder()
                .logStreams(
                    LogStream.builder().logStreamName(stdoutStream).lastEventTimestamp(300L).build(),
                    LogStream.builder().logStreamName(retryStderrStream).lastEventTimestamp(100L).build())
                .build();
          }
          if ("getLogEvents".equals(method.getName())) {
            GetLogEventsRequest request = (GetLogEventsRequest) args[0];
            requests.add(request);
            return GetLogEventsResponse.builder()
                .events(OutputLogEvent.builder().message(request.logStreamName().contains("stderr")
                    ? "E316053 [retry-profile] Configuration not found for profile."
                    : "stdout").build())
                .build();
          }
          if ("serviceName".equals(method.getName()))
            return "CloudWatch Logs";
          if ("close".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException(method.getName());
        });
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }
}
