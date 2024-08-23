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

package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Connector {

  /**
   * The unique identifier of this connector configuration.
   */
  public String id;
  /**
   * Whether this connector is activated.
   * If this flag is set to false, no connector client will be available for it. That means no requests can be performed to the connector.
   */
  public boolean active = true;
  /**
   * Whether to skip the automatic disabling of this connector even when being not healthy.
   * If this flag is set to true, the connector will keep accepting requests even if its health-check is not OK.
   */
  public boolean skipAutoDisable;
  /**
   * Whether the connector is a trusted connector. Trusted connectors will receive more information than normal connectors. This might be
   * confidential information about the incoming query.
   */
  public boolean trusted = false;
  /**
   * Arbitrary parameters to be provided to the remote function with the event.
   */
  public Map<String, Object> params;
  /**
   * Arbitrary parameters to be provided to the remote function with the event.
   */
  public StorageCapabilities capabilities = new StorageCapabilities();
  /**
   * The connection and throttling settings.
   */
  public ConnectionSettings connectionSettings = new ConnectionSettings();
  /**
   * The default event types to register the connector for. Can be overridden in the space definition by the space creator.
   */
  public List<String> defaultEventTypes;
  /**
   * A list of email addresses of responsible owners for this connector.
   * These email addresses will be used to send potential health warnings and other notifications.
   */
  public List<String> contactEmails;
  /**
   * The identifier of the owner of this connector, most likely the HERE account ID.
   */
  public String owner;
  public Map<String, Set<String>> allowedEventTypes;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StorageCapabilities {

    /**
     * If the lambda supports pre-serialization.
     */
    public boolean preserializedResponseSupport;

    /**
     * If the lambda supports relocation events.
     */
    public boolean relocationSupport;

    /**
     * The maximum size of the payload, which the connector accepts as uncompressed data.
     */
    public int maxUncompressedSize = Integer.MAX_VALUE;

    /**
     * The maximum size of the event, which this connector can directly receive.
     */
    public int maxPayloadSize = 6 * 1024 * 1024;

    /**
     * Whether searching by properties is supported. (Only applicable for storage connectors)
     */
    public boolean propertySearch;

    /**
     * Whether it's supported to configure the searchableProperties of spaces. (Only applicable for storage connectors) See:
     * {@link Space#getSearchableProperties()}
     */
    public boolean searchablePropertiesConfiguration;

    /**
     * Whether automatic caching configuration for spaces is supported.
     */
    public boolean enableAutoCache;

    /**
     * A list of supported clustering types / algorithms. (Only applicable for storage connectors)
     */
    public List<String> clusteringTypes;

    /**
     * Whether the storage connector provides reports about its utilization of the underlying storage engine.
     */
    public boolean storageUtilizationReporting;

    public boolean mvtSupport;

    /**
     * Whether the storage connector supports the extends feature which allow spaces to extend content from another.
     */
    public boolean extensionSupport;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StorageCapabilities that = (StorageCapabilities) o;
      return preserializedResponseSupport == that.preserializedResponseSupport
          && relocationSupport == that.relocationSupport
          && maxUncompressedSize == that.maxUncompressedSize
          && maxPayloadSize == that.maxPayloadSize
          && propertySearch == that.propertySearch
          && searchablePropertiesConfiguration == that.searchablePropertiesConfiguration
          && enableAutoCache == that.enableAutoCache
          && Objects.equals(clusteringTypes, that.clusteringTypes)
          && storageUtilizationReporting == that.storageUtilizationReporting
          && mvtSupport == that.mvtSupport
          && extensionSupport == that.extensionSupport;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConnectionSettings {

    /**
     * The maximal amount of concurrent connector instances to use.
     */
    public int maxConnections = 512;
    private int minConnections = 0;

    public int maxConnectionsPerRequester = 100;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConnectionSettings that = (ConnectionSettings) o;
      return minConnections == that.minConnections &&
          maxConnections == that.maxConnections && maxConnectionsPerRequester == that.maxConnectionsPerRequester;
    }

    /**
     * @return The minimal amount of concurrent connector connections to be guaranteed.
     */
    public int getMinConnections() {
      return minConnections;
    }
  }
}
