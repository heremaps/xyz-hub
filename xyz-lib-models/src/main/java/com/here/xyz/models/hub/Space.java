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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Typed;
import com.here.xyz.View;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The space configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Space")
public final class Space extends Feature implements Typed {

  /**
   * Create new space initialized with the given identifier.
   *
   * @param id the identifier.
   */
  @JsonCreator
  public Space(@JsonProperty @NotNull String id) {
    super(id);
  }

  /**
   * Beta release date: 2018-10-01T00:00Z[UTC]
   */
  private final long DEFAULT_TIMESTAMP = 1538352000000L;

  /**
   * A human-readable title of the space.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private String title;

  /**
   * The collection into which to place the features. The {@link #getId() id} reflects the unique identifier under which the space is
   * referred externally, the collection refers to the storage internally and is only understood by the storage connector. If the client
   * does not explicitly set the collection name, then it will be set to the same as the space identifier.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private String collection;

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
   * Allows to temporary or permanently disable history.
   */
  @JsonProperty
  @JsonView(All.class)
  private boolean history = true;

  /**
   * The maximum days of history to keep; {@code null} means forever.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private Integer maxHistoryAge;

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

  /**
   * If set, then the owner of all features in this space forcefully set to this value.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  private String forceOwner;

  public String getCollection() {
    return collection != null ? collection : getId();
  }

  public void setCollection(final String collection) {
    this.collection = collection;
  }

  public @NotNull Space withCollection(final String collection) {
    setCollection(collection);
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

  public @NotNull List<@NotNull String> useConnectorIds() {
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

  public boolean hasHistory() {
    return history;
  }

  public void setHistory(final boolean enableHistory) {
    this.history = enableHistory;
  }

  public @Nullable Integer getMaxHistoryAge() {
    return maxHistoryAge;
  }

  public void setMaxHistoryAge(@Nullable Integer days) {
    this.maxHistoryAge = days;
  }

  public List<@NotNull String> getPackages() {
    return packages;
  }

  public @NotNull List<@NotNull String> usePackages() {
    List<@NotNull String> packages = this.packages;
    if (packages == null) {
      this.packages = packages = new ArrayList<>();
    }
    return packages;
  }

  public void setPackages(final List<@NotNull String> packages) {
    this.packages = packages;
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

  public @Nullable String getForceOwner() {
    return forceOwner;
  }

  public void setForceOwner(final String forceOwner) {
    this.forceOwner = forceOwner;
  }

  public @NotNull Space withForceOwner(final String forceOwner) {
    setForceOwner(forceOwner);
    return this;
  }
}
