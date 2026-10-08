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

import static com.here.xyz.jobs.config.StatisticsConfigClient.KIND_IMPORT;
import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.STATISTICS;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.config.StatisticsConfigClient;
import com.here.xyz.jobs.config.StatisticsConfigClient.StatisticsConfig;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.Step.Visibility;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.pagination.Page;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Records the {@link FeatureStatistics} of a succeeded job's import steps in the statistics table, one item per layer and version.
 * Never fails the job: problems are only logged.
 */
public class JobStatisticsRecorder {
  private static final Logger logger = LogManager.getLogger();
  static final String TOTAL = "total";
  private static final int PAGE_SIZE = 100;

  private static StatisticsConfigClient client;

  /**
   * @return The client of the configured table, or null if none is configured (nothing is recorded then)
   */
  private static synchronized StatisticsConfigClient client() {
    if (client == null && Config.instance != null && Config.instance.STATISTICS_DYNAMODB_TABLE_ARN != null) {
      client = new StatisticsConfigClient(Config.instance.STATISTICS_DYNAMODB_TABLE_ARN);
      client.initLocalTable();
    }
    return client;
  }

  /**
   * Records the statistics of the job. The returned future always succeeds: a failure (e.g., writing to DynamoDB) is only logged.
   */
  public static Future<Void> record(Job job) {
    try {
      return record(job.getId(), () -> collect(job.getId(), job.getSteps().stepStream(), JobStatisticsRecorder::loadStatistics,
          System.currentTimeMillis()), client());
    }
    catch (Exception e) {
      logger.warn("[{}] Could not record the statistics of the job", job.getId(), e);
      return Future.succeededFuture();
    }
  }

  static Future<Void> record(String jobId, Callable<List<StatisticsConfig>> collector, StatisticsConfigClient client) {
    if (client == null)
      return Future.succeededFuture();

    return Core.vertx.executeBlocking(() -> {
          List<StatisticsConfig> statistics = collector.call();
          client.storeStatistics(statistics);
          return statistics.size();
        })
        .<Void>map(count -> {
          if (count > 0)
            logger.info("[{}] Recorded the statistics of {} layer version(s)", jobId, count);
          return null;
        })
        .recover(t -> {
          logger.warn("[{}] Could not record the statistics of the job", jobId, t);
          return Future.succeededFuture();
        });
  }

  /**
   * Sums the statistics of the import steps per space and version: rows per operation (patch) or one total (full release), and bytes.
   */
  static List<StatisticsConfig> collect(String jobId, Stream<Step> steps, Function<Step, List<Output>> loader, long recordedAtMillis) {
    Map<LayerVersion, Map<String, Long>> counts = new LinkedHashMap<>();

    steps.forEach(step -> {
      if (!(step instanceof TaskedImportFilesToSpace importStep) || importStep.getSpaceId() == null)
        return;
      Map<String, String> outputMetadata = step.getOutputMetadata();
      String operation = outputMetadata == null ? null : outputMetadata.get("operation");
      String key = operation == null || operation.isBlank() ? TOTAL : operation.toLowerCase();

      for (Output output : loader.apply(step)) {
        if (!(output instanceof FeatureStatistics statistics))
          continue;
        Long version = numericVersion(statistics.getVersionRef());
        if (version == null)
          continue; //Without the version the statistics can not be assigned to a version of the layer
        Map<String, Long> layerCounts = counts.computeIfAbsent(new LayerVersion(importStep.getSpaceId(), version),
            k -> new LinkedHashMap<>());
        layerCounts.merge(key, statistics.getFeatureCount(), Long::sum);
        layerCounts.merge("bytes", statistics.getByteSize(), Long::sum);
      }
    });

    return counts.entrySet().stream()
        .map(e -> new StatisticsConfig()
            .withSpaceId(e.getKey().spaceId())
            .withVersion(e.getKey().version())
            .withKind(KIND_IMPORT)
            .withJobId(jobId)
            .withRequested(e.getValue())
            .withRecordedAt(recordedAtMillis))
        .toList();
  }

  private record LayerVersion(String spaceId, long version) {}

  /**
   * @return The version of the ref, or null if it is not a single numeric version
   */
  static Long numericVersion(Ref ref) {
    if (ref == null || !ref.isSingleVersion() || ref.isHead() || ref.isTag())
      return null;
    long version = ref.getVersion();
    return version >= 0 ? version : null;
  }

  private static List<Output> loadStatistics(Step step) {
    List<Output> outputs = new ArrayList<>();
    //USER for a patch, SYSTEM for a full release (where the export re-publishes them)
    for (Visibility visibility : Visibility.values()) {
      String pageToken = null;
      do {
        Page<Output> page = step.loadOutputsPage(visibility, STATISTICS, PAGE_SIZE, pageToken);
        outputs.addAll(page.getItems());
        pageToken = page.getNextPageToken();
      } while (pageToken != null);
    }
    return outputs;
  }
}
