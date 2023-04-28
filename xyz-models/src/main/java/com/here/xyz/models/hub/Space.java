/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The space configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Space")
public class Space {

  /**
   * Create new blank space object.
   */
  public Space() {
  }

  /**
   * Create new space initialized with the given identifier.
   *
   * @param id the identifier.
   */
  public Space(@NotNull String id) {
    this.id = id;
  }

  /**
   * All spaces, beware to modify this map or any of its values. This is a read-only map!
   *
   * <p>This map need to be updated by a background process of the host.
   */
  private static final ConcurrentHashMap<@NotNull String, @NotNull Space> spaces = new ConcurrentHashMap<>();

  /**
   * Returns the space with the given ID.
   *
   * @param id the space-id.
   * @return The space with the given ID; if any.
   */
  public static @Nullable Space getById(@NotNull String id) {
    return spaces.get(id);
  }

  /**
   * Beta release date: 2018-10-01T00:00Z[UTC]
   */
  private final long DEFAULT_TIMESTAMP = 1538352000000L;

  /**
   * The unique identifier of the space.
   */
  @JsonProperty
  @JsonView(All.class)
  private String id;

  /**
   * A human-readable title of the space.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private String title;

  /**
   * A human-readable description of the space, and its content.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private String description;

  /**
   * If set to true, every authenticated user can read the features in the space.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_DEFAULT)
  private boolean shared = false;

  /**
   * Copyright information for the data in the space.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  private List<Copyright> copyright;

  /**
   * Information about the license bound to the data within the space. For valid keywords see {@link License}.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  private License license;

  /**
   * Arbitrary parameters to be added into the event. They can be used by all event handlers in the pipeline.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  @JsonView(View.Protected.class)
  public Map<@NotNull String, Object> params;

  /**
   * The list of connector IDs (configured event handlers) to be added to the task (event handler pipeline) in order.
   */
  @JsonProperty
  @JsonView(All.class)
  public @Nullable List<@NotNull String> connectors;

  /**
   * The identifier of the owner of this space, most likely the HERE account ID.
   */
  @JsonProperty
  @JsonView(All.class)
  private String owner;

  /**
   * Allows to temporary or permanently disable history.
   */
  @JsonProperty
  @JsonView(All.class)
  private boolean enableHistory = true;

  /**
   * The maximum days of history to keep; {@code null} means forever.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private Integer maxHistoryDays;

  /**
   * List of packages that this space belongs to.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  private List<@NotNull String> packages;

  /**
   * Arbitrary properties added to the space, this includes the standard {@link XyzNamespace}.
   */
  @JsonProperty
  @JsonView(All.class)
  private Properties properties;

  /**
   * Indicates if the space is in a read-only mode.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_DEFAULT)
  private boolean readOnly = false;

  /**
   * A map defined by the user to index feature-properties to make them searchable and sortable. The key is the name of the index to create,
   * the value describes the properties to index including their ordering in the index. Properties not being indexes still can be searched,
   * but the result can be bad.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  private Map<@NotNull String, @NotNull Index> indices;

  /**
   * A map defined by the user to apply constraints on feature-properties to prevent illegal values. Note that creating constraints later
   * will fail, if the space does not fulfill the constraint.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  private Map<@NotNull String, @NotNull Constraint> constraints;

  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public @NotNull Space withId(final String id) {
    setId(id);
    return this;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(final String title) {
    this.title = title;
  }

  public @NotNull Space withTitle(final String title) {
    setTitle(title);
    return this;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public @NotNull Space withDescription(final String description) {
    setDescription(description);
    return this;
  }

  public boolean isShared() {
    return shared;
  }

  public void setShared(final boolean shared) {
    this.shared = shared;
  }

  public @NotNull Space withShared(final boolean shared) {
    setShared(shared);
    return this;
  }

  public List<Copyright> getCopyright() {
    return copyright;
  }

  public void setCopyright(final List<Copyright> copyright) {
    this.copyright = copyright;
  }

  public @NotNull Space withCopyright(final List<Copyright> copyright) {
    setCopyright(copyright);
    return this;
  }

  public License getLicense() {
    return license;
  }

  public void setLicense(final License license) {
    this.license = license;
  }

  public @NotNull Space withLicense(final License license) {
    setLicense(license);
    return this;
  }

  public @Nullable List<@NotNull String> getConnectorIds() {
    return connectors;
  }

  public @NotNull List<@NotNull String> connectorIds() {
    List<@NotNull String> connectors = this.connectors;
    if (connectors == null) {
      this.connectors = connectors = new ArrayList<>();
    }
    return connectors;
  }

  public void setConnectorIds(@Nullable List<@NotNull String> connectorIds) {
    connectors = connectorIds;
  }

  public void setConnectorIds(@NotNull String... connectorIds) {
    if (connectorIds != null) {
      connectors = new ArrayList<>(connectorIds.length);
      Collections.addAll(connectors, connectorIds);
    } else {
      connectors = null;
    }
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(final String owner) {
    this.owner = owner;
  }

  public boolean isEnableHistory() {
    return enableHistory;
  }

  public void setEnableHistory(final boolean enableHistory) {
    this.enableHistory = enableHistory;
  }

  public @Nullable Integer getMaxHistoryDays() {
    return maxHistoryDays;
  }

  public void setMaxHistoryDays(@Nullable Integer days) {
    this.maxHistoryDays = days;
  }

  public List<@NotNull String> getPackages() {
    return packages;
  }

  public @NotNull List<@NotNull String> packages() {
    List<@NotNull String> packages = this.packages;
    if (packages == null) {
      this.packages = packages = new ArrayList<>();
    }
    return packages;
  }

  public void setPackages(final List<@NotNull String> packages) {
    this.packages = packages;
  }

  public @Nullable Properties getProperties() {
    return properties;
  }

  public @NotNull Properties properties() {
    Properties properties = this.properties;
    if (properties == null) {
      this.properties = properties = new Properties();
    }
    return properties;
  }

  public void setProperties(@Nullable Properties properties) {
    this.properties = properties;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(final boolean readOnly) {
    this.readOnly = readOnly;
  }

  public @Nullable Map<@NotNull String, @NotNull Index> getIndices() {
    return indices;
  }

  public void setIndices(@Nullable Map<@NotNull String, @NotNull Index> indices) {
    this.indices = indices;
  }

  public @Nullable Map<@NotNull String, @NotNull Constraint> getConstraints() {
    return constraints;
  }

  public void setConstraints(@Nullable Map<@NotNull String, @NotNull Constraint> constraints) {
    this.constraints = constraints;
  }
}
