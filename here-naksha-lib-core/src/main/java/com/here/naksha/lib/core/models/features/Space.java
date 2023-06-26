/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.models.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.Copyright;
import com.here.naksha.lib.core.models.License;
import com.here.naksha.lib.core.models.PipelineComponent;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.geojson.implementation.Properties;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.indexing.Constraint;
import com.here.naksha.lib.core.models.indexing.Index;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The space configuration. A space is an event-pipeline accessible via the REST API.
 */
@SuppressWarnings("unused")
@JsonTypeName(value = "Space")
public final class Space extends PipelineComponent implements Typed {

  /**
   * Create new space initialized with the given identifier.
   *
   * @param id            the identifier.
   * @param eventHandlers the list of event handler identifiers to form the event-pipeline.
   * @param packages      the packages this feature is part of.
   */
  @JsonCreator
  public Space(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(EVENT_HANDLERS) @NotNull List<@NotNull String> eventHandlers,
      @JsonProperty(PACKAGES) @Nullable List<@NotNull String> packages) {
    super(id, eventHandlers, packages);
  }

  /**
   * Create new space with the given identifier.
   *
   * @param id the identifier of the space.
   */
  public Space(@NotNull String id) {
    super(id, new ArrayList<>(), null);
  }

  /**
   * Create new space initialized with the given identifier.
   *
   * @param catalogId the catalog identifier.
   * @param name      the name of the space, unique within the catalog.
   */
  public Space(@Nullable String catalogId, @NotNull String name) {
    super(catalogId != null ? (catalogId + ":" + name) : name, new ArrayList<>(), null);
  }

  /**
   * Create new space initialized with the given identifier.
   *
   * @param catalogId     the catalog identifier.
   * @param name          the name of the space, unique within the catalog.
   * @param eventHandlers the list of event handler identifiers to form the event-pipeline.
   */
  public Space(@Nullable String catalogId, @NotNull String name, @NotNull List<@NotNull String> eventHandlers) {
    super(catalogId + ":" + name, eventHandlers, null);
  }

  /**
   * Set the full qualified space identifier.
   *
   * @param catalogId the catalog-identifier (being the prefix of the ID).
   * @param name      the name of the space.
   */
  public void setId(@Nullable String catalogId, @NotNull String name) {
    this.catalogId = catalogId;
    this.name = name;
    if (catalogId == null) {
      this.id = name;
    } else {
      this.id = catalogId + ":" + name;
    }
  }

  @Override
  @JsonSetter
  public void setId(@NotNull String id) {
    final int catEnd = id.lastIndexOf(':');
    if (catEnd > 0) {
      this.catalogId = id.substring(0, catEnd);
      this.name = id.substring(catEnd + 1);
    } else {
      this.catalogId = null;
      this.name = id;
    }
    this.id = id;
  }

  /**
   * Beta release date: 2018-10-01T00:00Z[UTC]
   */
  private final long DEFAULT_TIMESTAMP = 1538352000000L;

  /**
   * The catalog identifier.
   */
  @AvailableSince(INaksha.v2_0_3)
  @JsonProperty
  private @Nullable String catalogId;

  /**
   * The name of the space, must be unique within a given catalog.
   */
  @AvailableSince(INaksha.v2_0_3)
  @JsonProperty
  private @NotNull String name;

  /**
   * A human-readable title of the space.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  private String title;

  /**
   * The collection into which to place the features. The {@link #getId() id} reflects the unique identifier under which the space is
   * referred externally, the collection refers to the storage internally and is only understood by the storage connector. If the client
   * does not explicitly set the collection name, then it will be set to the same as the space identifier.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  private String collectionId;

  /**
   * A human-readable description of the space, and its content.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  private String description;

  /**
   * If set to true, every authenticated user can read the features in the space.
   */
  @JsonProperty
  @JsonInclude(Include.NON_DEFAULT)
  private boolean shared = false;

  /**
   * Copyright information for the data in the space.
   */
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private List<Copyright> copyright;

  /**
   * Information about the license bound to the data within the space. For valid keywords see {@link License}.
   */
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private License license;

  /**
   * Allows to temporary or permanently disable history.
   */
  @JsonProperty
  private boolean history = true;

  /**
   * The maximum days of history to keep; {@code null} means forever.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  private Integer maxHistoryAge;

  /**
   * List of packages that this space belongs to.
   */
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private List<@NotNull String> packages;

  /**
   * Arbitrary properties added to the space, this includes the standard {@link XyzNamespace}.
   */
  @JsonProperty
  private Properties properties;

  /**
   * Indicates if the space is in a read-only mode.
   */
  @JsonProperty
  @JsonInclude(Include.NON_DEFAULT)
  private boolean readOnly = false;

  /**
   * A map defined by the user to index feature-properties to make them searchable and sortable. The key is the name of the index to create,
   * the value describes the properties to index including their ordering in the index. Properties not being indexes still can be searched,
   * but the result can be bad.
   */
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private Map<@NotNull String, @NotNull Index> indices;

  /**
   * A map defined by the user to apply constraints on feature-properties to prevent illegal values. Note that creating constraints later
   * will fail, if the space does not fulfill the constraint.
   */
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private Map<@NotNull String, @NotNull Constraint> constraints;

  /**
   * If set, then the owner of all features in this space forcefully set to this value.
   */
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private String forceOwner;

  /**
   * Returns the catalog identifier to which this space belongs.
   *
   * @return the catalog identifier to which this space belongs; if any.
   */
  public @Nullable String getCatalogId() {
    return catalogId;
  }

  /**
   * Returns the name of the space.
   *
   * @return the name of the space.
   */
  public @NotNull String getName() {
    return name;
  }

  /**
   * Returns the collection identifier of the collection in which to persist the space; if any.
   *
   * @return the collection identifier.
   */
  public @NotNull String getCollectionId() {
    return collectionId != null ? collectionId : getId();
  }

  public void setCollectionId(final @Nullable String collection) {
    this.collectionId = collection;
  }

  public @NotNull Space withCollection(final @Nullable String collectionId) {
    setCollectionId(collectionId);
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
