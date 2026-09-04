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

import static com.here.xyz.jobs.util.AwsClientFactory.cloudwatchLogsClient;

import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

class EmrErrorInfoResolver {
  static final String LOG_GROUP = "/aws/emr-serverless";
  static final int MAX_LOG_BYTES = 256 * 1024;
  static final int MAX_CAUSE_LENGTH = 2_048;
  private static final long[] DEFAULT_RETRY_DELAYS_MS = {0, 1_000, 2_000, 4_000};
  private static final Pattern MISSING_PROFILE_ERROR = Pattern.compile(
      "E316053\\s+(?:\\[[^\\r\\n]*?]\\s*)?Configuration not found for profile\\.?",
      Pattern.CASE_INSENSITIVE);
  private static final Logger logger = LogManager.getLogger();

  private final EmrLogReader logReader;
  private final long[] retryDelaysMs;

  EmrErrorInfoResolver() {
    this(new CloudWatchEmrLogReader(cloudwatchLogsClient()), DEFAULT_RETRY_DELAYS_MS);
  }

  EmrErrorInfoResolver(EmrLogReader logReader, long[] retryDelaysMs) {
    this.logReader = logReader;
    this.retryDelaysMs = retryDelaysMs.clone();
  }

  Future<String> resolveCause(String applicationId, String jobRunId, String fallbackCause) {
    Promise<String> promise = Promise.promise();
    resolveCause(applicationId, jobRunId, fallbackCause, 0, promise);
    return promise.future();
  }

  private void resolveCause(String applicationId, String jobRunId, String fallbackCause, int attempt, Promise<String> promise) {
    long delay = retryDelaysMs[attempt];
    Runnable lookup = () -> Core.vertx.executeBlocking(blockingPromise -> {
      try {
        blockingPromise.complete(extractMissingProfileError(logReader.read(applicationId, jobRunId)));
      }
      catch (Exception e) {
        blockingPromise.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        logger.warn("Could not read EMR driver logs for application {} and job run {}. Falling back to EMR state details.",
            applicationId, jobRunId, ar.cause());
        promise.tryComplete(fallbackCause);
      }
      else if (ar.result() != null)
        promise.tryComplete((String) ar.result());
      else if (attempt + 1 < retryDelaysMs.length)
        resolveCause(applicationId, jobRunId, fallbackCause, attempt + 1, promise);
      else
        promise.tryComplete(fallbackCause);
    });

    if (delay == 0)
      lookup.run();
    else
      Core.vertx.setTimer(delay, ignored -> lookup.run());
  }

  static String extractMissingProfileError(List<String> messages) {
    if (messages == null)
      return null;

    for (String message : messages) {
      if (message == null)
        continue;
      Matcher matcher = MISSING_PROFILE_ERROR.matcher(message);
      if (matcher.find())
        return truncate(matcher.group());
    }
    return null;
  }

  private static String truncate(String cause) {
    return cause.length() <= MAX_CAUSE_LENGTH ? cause : cause.substring(0, MAX_CAUSE_LENGTH);
  }

  @FunctionalInterface
  interface EmrLogReader {
    List<String> read(String applicationId, String jobRunId);
  }

  static class CloudWatchEmrLogReader implements EmrLogReader {
    private static final int MAX_STREAM_PAGES = 5;
    private static final int STREAM_PAGE_SIZE = 50;
    private static final int EVENT_LIMIT = 10_000;
    private final CloudWatchLogsClient client;

    CloudWatchEmrLogReader(CloudWatchLogsClient client) {
      this.client = client;
    }

    @Override
    public List<String> read(String applicationId, String jobRunId) {
      String streamPrefix = "/applications/" + applicationId + "/jobs/" + jobRunId;
      List<LogStream> streams = findDriverStreams(streamPrefix);
      List<String> messages = new ArrayList<>();
      int bytesRead = 0;

      for (LogStream stream : streams) {
        List<OutputLogEvent> events = client.getLogEvents(GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(stream.logStreamName())
                .startFromHead(false)
                .limit(EVENT_LIMIT)
                .build())
            .events();

        for (int i = events.size() - 1; i >= 0 && bytesRead < MAX_LOG_BYTES; i--) {
          String message = events.get(i).message();
          if (message == null)
            continue;
          bytesRead += message.getBytes(StandardCharsets.UTF_8).length;
          if (bytesRead <= MAX_LOG_BYTES)
            messages.add(message);
        }
        if (bytesRead >= MAX_LOG_BYTES)
          break;
      }
      return messages;
    }

    private List<LogStream> findDriverStreams(String streamPrefix) {
      List<LogStream> streams = new ArrayList<>();
      String nextToken = null;
      int page = 0;
      do {
        DescribeLogStreamsResponse response = client.describeLogStreams(DescribeLogStreamsRequest.builder()
            .logGroupName(LOG_GROUP)
            .logStreamNamePrefix(streamPrefix)
            .limit(STREAM_PAGE_SIZE)
            .nextToken(nextToken)
            .build());
        streams.addAll(response.logStreams());
        nextToken = response.nextToken();
      } while (nextToken != null && ++page < MAX_STREAM_PAGES);

      return streams.stream()
          .filter(stream -> stream.logStreamName() != null && stream.logStreamName().contains("SPARK_DRIVER"))
          .sorted(Comparator
              .comparing((LogStream stream) -> !stream.logStreamName().toLowerCase().contains("stderr"))
              .thenComparing(LogStream::lastEventTimestamp, Comparator.nullsLast(Comparator.reverseOrder())))
          .toList();
    }
  }
}
