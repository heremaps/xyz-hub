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

package com.here.xyz.jobs.config;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.XyzSerializable;
import com.here.xyz.util.ARN;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Client for storing and loading the feature statistics of finished jobs in DynamoDB, so they outlive the job and its S3 outputs.
 * One item per space, version and kind; items expire a year after they were recorded.
 */
public class StatisticsConfigClient {
  public static final String KIND_IMPORT = "import";
  private static final long TTL_SECONDS = 365L * 24 * 3600;

  private final Table statisticsTable;
  private final DynamoClient dynamoClient;

  public StatisticsConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn, null);
    statisticsTable = dynamoClient.db.getTable(new ARN(tableArn).getResourceWithoutType());
  }

  /**
   * Stores the given statistics, one item per space, version and kind (batched).
   *
   * @param statistics the statistics to persist; a {@code null}/empty list is a no-op
   */
  public void storeStatistics(List<StatisticsConfig> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return;
    }
    List<WriteRequest> writeRequests = statistics.stream()
        .map(config -> new WriteRequest(new PutRequest(ItemUtils.toAttributeValues(toItem(config)))))
        .toList();
    dynamoClient.batchWrite(statisticsTable.getTableName(), writeRequests);
  }

  /**
   * Loads all recorded statistics of the given space (all versions and kinds).
   *
   * @param spaceId the space ({@code <catalogHrn>:<layerId>}) whose statistics to load
   * @return the statistics (empty if none)
   */
  public List<StatisticsConfig> loadStatisticsForSpace(String spaceId) {
    List<StatisticsConfig> statistics = new LinkedList<>();
    statisticsTable.query("spaceId", spaceId)
        .pages()
        .forEach(page -> page.forEach(item -> statistics.add(XyzSerializable.fromMap(item.asMap(), StatisticsConfig.class))));
    return statistics;
  }

  /**
   * Creates the statistics table when running against a local DynamoDB.
   */
  public void initLocalTable() {
    if (dynamoClient.isLocal()) {
      dynamoClient.createTable(statisticsTable.getTableName(), "spaceId:S,sk:S,catalogHrn:S,jobId:S", "spaceId,sk",
          List.of(new IndexDefinition("catalogHrn"), new IndexDefinition("jobId")), "expiresAt");
    }
  }

  private Item toItem(StatisticsConfig config) {
    String spaceId = config.getSpaceId();
    int split = spaceId.lastIndexOf(':');
    return Item.fromMap(config
        //The zero padding makes the versions sort numerically
        .withSk(String.format("%010d#%s", config.getVersion(), config.getKind()))
        .withCatalogHrn(split > 0 ? spaceId.substring(0, split) : spaceId)
        .withLayerId(split > 0 ? spaceId.substring(split + 1) : spaceId)
        .withExpiresAt(config.getRecordedAt() / 1000 + TTL_SECONDS)
        .toMap());
  }

  /**
   * POJO mirroring one statistics item. {@code requested} holds the job's feature statistics: rows per operation ({@code added},
   * {@code updated}, {@code deleted}) or one {@code total}, plus {@code bytes}. {@code sk}, {@code catalogHrn}, {@code layerId} and
   * {@code expiresAt} are derived when storing.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StatisticsConfig implements XyzSerializable {

    @JsonProperty
    private String spaceId;
    @JsonProperty
    private String sk;
    @JsonProperty
    private String catalogHrn;
    @JsonProperty
    private String layerId;
    @JsonProperty
    private long version;
    @JsonProperty
    private String kind;
    @JsonProperty
    private String jobId;
    @JsonProperty
    private Map<String, Long> requested;
    @JsonProperty
    private long recordedAt;
    @JsonProperty
    private long expiresAt;

    public String getSpaceId() {
      return spaceId;
    }

    public void setSpaceId(String spaceId) {
      this.spaceId = spaceId;
    }

    public StatisticsConfig withSpaceId(String spaceId) {
      setSpaceId(spaceId);
      return this;
    }

    public String getSk() {
      return sk;
    }

    public void setSk(String sk) {
      this.sk = sk;
    }

    public StatisticsConfig withSk(String sk) {
      setSk(sk);
      return this;
    }

    public String getCatalogHrn() {
      return catalogHrn;
    }

    public void setCatalogHrn(String catalogHrn) {
      this.catalogHrn = catalogHrn;
    }

    public StatisticsConfig withCatalogHrn(String catalogHrn) {
      setCatalogHrn(catalogHrn);
      return this;
    }

    public String getLayerId() {
      return layerId;
    }

    public void setLayerId(String layerId) {
      this.layerId = layerId;
    }

    public StatisticsConfig withLayerId(String layerId) {
      setLayerId(layerId);
      return this;
    }

    public long getVersion() {
      return version;
    }

    public void setVersion(long version) {
      this.version = version;
    }

    public StatisticsConfig withVersion(long version) {
      setVersion(version);
      return this;
    }

    public String getKind() {
      return kind;
    }

    public void setKind(String kind) {
      this.kind = kind;
    }

    public StatisticsConfig withKind(String kind) {
      setKind(kind);
      return this;
    }

    public String getJobId() {
      return jobId;
    }

    public void setJobId(String jobId) {
      this.jobId = jobId;
    }

    public StatisticsConfig withJobId(String jobId) {
      setJobId(jobId);
      return this;
    }

    public Map<String, Long> getRequested() {
      return requested;
    }

    public void setRequested(Map<String, Long> requested) {
      this.requested = requested;
    }

    public StatisticsConfig withRequested(Map<String, Long> requested) {
      setRequested(requested);
      return this;
    }

    public long getRecordedAt() {
      return recordedAt;
    }

    public void setRecordedAt(long recordedAt) {
      this.recordedAt = recordedAt;
    }

    public StatisticsConfig withRecordedAt(long recordedAt) {
      setRecordedAt(recordedAt);
      return this;
    }

    public long getExpiresAt() {
      return expiresAt;
    }

    public void setExpiresAt(long expiresAt) {
      this.expiresAt = expiresAt;
    }

    public StatisticsConfig withExpiresAt(long expiresAt) {
      setExpiresAt(expiresAt);
      return this;
    }
  }
}
