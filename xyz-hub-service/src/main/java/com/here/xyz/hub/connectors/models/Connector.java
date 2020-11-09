/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.connectors.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.rest.admin.Node;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The connector configuration.
 */
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
   * If this flag is set to true the connector will keep accepting requests even if its health-check is not OK.
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
   * A map of remote functions which may be connected by this connector.
   * The remote function to be used should be determined by the "environment ID" of the service.
   *
   * @see Service#getEnvironmentIdentifier()
   */
  public Map<String, RemoteFunctionConfig> remoteFunctions = new HashMap<>();

  /**
   * Arbitrary parameters to be provided to the remote function with the event.
   */
  public RemoteFunctionConfig remoteFunction;

  /**
   * Returns the remote function pool ID to be used for this Service environment.
   * @return
   */
  public static String getRemoteFunctionPoolId() {
    if (Service.configuration != null && Service.configuration.REMOTE_FUNCTION_POOL_ID != null) {
      return Service.configuration.REMOTE_FUNCTION_POOL_ID;
    }
    return Service.getEnvironmentIdentifier();
  }

  /**
   * @see Service#getEnvironmentIdentifier()
   *
   * @return The according remote function for this environment or - if there is no special function
   * for this environment - any remote function from the {@link #remoteFunctions} map.
   */
  @JsonIgnore
  public RemoteFunctionConfig getRemoteFunction() {
    if (remoteFunctions.isEmpty()) throw new RuntimeException("No remote functions are defined for connector with ID " + id);

    String rfPoolId = getRemoteFunctionPoolId();
    if (!remoteFunctions.containsKey(rfPoolId)) throw new RuntimeException("No matching remote function is defined for connector with ID "
        + id + " and remote-function pool ID " + rfPoolId);

    return remoteFunctions.get(rfPoolId);
  }

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
   * Returns the maximal amount of requests to queue.
   *
   * @return the maximal amount of requests to queue.
   */
  @JsonIgnore
  @Deprecated
  public int queueSize() {
    return connectionSettings.maxConnections * 32;
  }

  public <T extends Connector> boolean equalTo(T other) {
    return (other != null
        && Objects.equals(id, other.id)
        && active == other.active
        && trusted == other.trusted)
        && Objects.equals(params, other.params)
        && Objects.equals(capabilities, other.capabilities)
        && Objects.equals(remoteFunctions, other.remoteFunctions)
        && Objects.equals(connectionSettings, other.connectionSettings)
        && Objects.equals(defaultEventTypes, other.defaultEventTypes)
        && Objects.equals(skipAutoDisable, other.skipAutoDisable);
  }

  @JsonIgnore
  public int getMinConnectionsPerInstance() {
    return connectionSettings.getMinConnections() / Node.count();
  }

  @JsonIgnore
  public int getMaxConnectionsPerInstance() {
    return connectionSettings.maxConnections / Node.count();
  }

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
     * Whether it's supported to configure the searchableProperties of spaces. (Only applicable for storage connectors) See: {@link
     * Space#getSearchableProperties()}
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StorageCapabilities that = (StorageCapabilities) o;
      return preserializedResponseSupport == that.preserializedResponseSupport &&
          relocationSupport == that.relocationSupport &&
          maxUncompressedSize == that.maxUncompressedSize &&
          maxPayloadSize == that.maxPayloadSize &&
          propertySearch == that.propertySearch &&
          searchablePropertiesConfiguration == that.searchablePropertiesConfiguration &&
          enableAutoCache == that.enableAutoCache &&
          Objects.equals(clusteringTypes, that.clusteringTypes);
    }

  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConnectionSettings {

    /**
     * The maximal amount of concurrent connector instances to use.
     */
    public int maxConnections = 32;
    private int minConnections = 0;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConnectionSettings that = (ConnectionSettings) o;
      return minConnections == that.minConnections &&
          maxConnections == that.maxConnections;
    }

    /**
     * @return The minimal amount of concurrent connector connections to be guaranteed.
     */
    public int getMinConnections() {
      return minConnections;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonTypeInfo(use = Id.NAME, property = "type")
  @JsonSubTypes({
      @JsonSubTypes.Type(value = Embedded.class, name = "Embedded"),
      @JsonSubTypes.Type(value = AWSLambda.class, name = "AWSLambda"),
      @JsonSubTypes.Type(value = Http.class, name = "Http")
  })
  public abstract static class RemoteFunctionConfig {

    /**
     * The ID of this remote function.
     */
    public String id;
    /**
     * The number of containers to keep warmed up.
     */
    public int warmUp;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoteFunctionConfig that = (RemoteFunctionConfig) o;
      return warmUp == that.warmUp &&
          Objects.equals(id, that.id) &&
          warmUp == that.warmUp;
    }

    @JsonTypeName(value = "AWSLambda")
    public static class AWSLambda extends RemoteFunctionConfig {

      /**
       * The ARN of the AWS lambda main function to be called for the respective operation.
       */
      public String lambdaARN;

      /**
       * The ARN of an AWS IAM Role granting the permission to call the lambda function specified in {@link #lambdaARN}. The referenced role
       * needs to allow this service to assume it by adding this service' role ARN as principle into its trust policy.
       *
       * See: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html#delegation
       */
      public String roleARN;

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AWSLambda awsLambda = (AWSLambda) o;
        return lambdaARN.equals(awsLambda.lambdaARN) &&
            Objects.equals(roleARN, awsLambda.roleARN);
      }

      @Override
      public int hashCode() {
        return Objects.hash(lambdaARN, roleARN);
      }
    }

    @JsonTypeName(value = "Embedded")
    public static class Embedded extends RemoteFunctionConfig {

      public String className;
      public Map<String, String> env;

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Embedded)) return false;
        if (!super.equals(o)) return false;
        Embedded embedded = (Embedded) o;
        return className.equals(embedded.className) &&
            Objects.equals(env, embedded.env);
      }

      @Override
      public int hashCode() {
        return Objects.hash(className, env);
      }
    }

    @JsonTypeName(value = "Http")
    public static class Http extends RemoteFunctionConfig {

      /**
       * The URL of the endpoint to POST events to.
       */
      public URL url;

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Http)) return false;
        if (!super.equals(o)) return false;
        Http http = (Http) o;
        return url.equals(http.url);
      }

      @Override
      public int hashCode() {
        return Objects.hash(url);
      }
    }
  }

}
