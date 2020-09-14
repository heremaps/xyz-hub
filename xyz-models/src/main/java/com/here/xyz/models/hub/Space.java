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

package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The space configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Space")
@SuppressWarnings("unused")
public class Space {

  /**
   * Beta release date: 2018-10-01T00:00Z[UTC]
   */
  private final long DEFAULT_TIMESTAMP = 1538352000000L;

  /**
   * The unique identifier of the space.
   */
  @JsonView({Public.class, Static.class})
  private String id;

  /**
   * A human readable title of the space.
   */
  @JsonView({Public.class, Static.class})
  private String title;

  /**
   * A human readable description of the space and it's content.
   */
  @JsonView({Public.class, Static.class})
  private String description;

  /**
   * If set to true, every authenticated user can read the features in the space.
   */
  @JsonInclude(Include.NON_DEFAULT)
  @JsonView({Public.class, Static.class})
  private boolean shared = false;

  /**
   * Copyright information for the data in the space.
   */
  @JsonInclude(Include.NON_EMPTY)
  @JsonView({Public.class, Static.class})
  private List<Copyright> copyright;

  /**
   * Information about the license bound to the data within the space. For valid keywords see {@link License}.
   */
  @JsonInclude(Include.NON_EMPTY)
  @JsonView({Public.class, Static.class})
  private License license;

  /**
   * The storage connector configuration.
   */
  @JsonView({WithConnectors.class, Static.class})
  private ConnectorRef storage;

  /**
   * The event listeners configuration. A listening connector get's only *informed* about events, but the XYZ hub doesn't expect / wait for
   * any response.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonView({WithConnectors.class, Static.class})
  @JsonDeserialize(using = ConnectorDeserializer.class)
  private Map<String, List<ListenerConnectorRef>> listeners;

  /**
   * The event processors configuration. A processing connector get's the specified events and can re-process them synchronously. The XYZ
   * Hub waits for a response.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonView({WithConnectors.class, Static.class})
  @JsonDeserialize(using = ConnectorDeserializer.class)
  private Map<String, List<ListenerConnectorRef>> processors;

  /**
   * The identifier of the owner of this space, most likely the HERE account ID.
   */
  @JsonView({Public.class, Static.class})
  private String owner;

  /**
   * The maximum amount of seconds of how long to hold objects of this Space in a cache.
   */
  @JsonInclude(Include.NON_DEFAULT)
  @JsonView({Internal.class, Static.class})
  private int cacheTTL = -1;

  /**
   * An arbitrary client configuration with hints or settings for the client, for example rendering instructions.
   */
  @JsonView({Public.class, Static.class})
  @JsonInclude(Include.NON_EMPTY)
  private Map<String, Object> client;

  /**
   * If true, every state of the feature, will be assigned a UUID value.
   */
  @JsonView({Public.class, Static.class})
  @JsonInclude(Include.NON_DEFAULT)
  private boolean enableUUID = false;

  /**
   * If true, history get created
   */
  @JsonView({Public.class, Static.class})
  @JsonInclude(Include.NON_DEFAULT)
  private boolean enableHistory = false;

  /**
   * Can be used to control how many versions should get hold in the history. -1 means infinite
   */
  @JsonView({Public.class, Static.class})
  @JsonInclude(Include.NON_EMPTY)
  private Integer maxVersionCount;

  /**
   * List of packages that this space belongs to.
   */
  @JsonInclude(Include.NON_EMPTY)
  @JsonView({Public.class, Static.class})
  private List<String> packages;

  /**
   * An additional identifier specifying a context of the owner.
   */
  @JsonView({Public.class, Static.class})
  @JsonInclude(Include.NON_NULL)
  private String cid;

  /**
   * The list of tags to describe this Space.
   */
  @JsonView({Public.class, Static.class})
  @JsonInclude(Include.NON_EMPTY)
  private List<String> tags;

  /**
   * The creation timestamp.
   */
  @JsonView({Public.class, Static.class})
  private long createdAt = DEFAULT_TIMESTAMP;

  /**
   * The last update timestamp.
   */
  @JsonView({Public.class, Static.class})
  private long updatedAt = DEFAULT_TIMESTAMP;

  /**
   * Indicates if the space is in a read-only mode.
   */
  @JsonInclude(Include.NON_DEFAULT)
  @JsonView({Public.class, Static.class})
  private boolean readOnly = false;

  /**
   * A map defined by the user which tells which of the feature-properties to make searchable. The key is the name of the property and the
   * value is a boolean flag telling whether the property should be searchable or not. Also nested properties can be referenced by
   * specifying a path with dots (e.g. "my.prop"). Setting the value to {@code false} the property won't be made searchable at all. (even if
   * some auto-indexing algorithm would chose the property to be searchable)
   */
  @JsonInclude(Include.NON_EMPTY)
  @JsonView({Public.class, Static.class})
  private Map<String, Boolean> searchableProperties;

  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public Space withId(final String id) {
    this.id = id;
    return this;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(final String title) {
    this.title = title;
  }

  public Space withTitle(final String title) {
    this.title = title;
    return this;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public Space withDescription(final String description) {
    this.description = description;
    return this;
  }

  public boolean isShared() {
    return shared;
  }

  public void setShared(final boolean shared) {
    this.shared = shared;
  }

  public Space withShared(final boolean shared) {
    this.shared = shared;
    return this;
  }

  public List<Copyright> getCopyright() {
    return copyright;
  }

  public void setCopyright(final List<Copyright> copyright) {
    this.copyright = copyright;
  }

  public Space withCopyright(final List<Copyright> copyright) {
    this.copyright = copyright;
    return this;
  }

  public License getLicense() {
    return license;
  }

  public void setLicense(final License license) {
    this.license = license;
  }

  public Space withLicense(final License license) {
    this.license = license;
    return this;
  }

  public ConnectorRef getStorage() {
    return storage;
  }

  public void setStorage(final ConnectorRef storage) {
    this.storage = storage;
  }

  public Space withStorage(final ConnectorRef storage) {
    this.storage = storage;
    return this;
  }

  public Map<String, List<ListenerConnectorRef>> getListeners() {
    return listeners;
  }

  public void setListeners(final Map<String, List<ListenerConnectorRef>> listeners) {
    this.listeners = listeners;
  }

  public Space withListeners(final Map<String, List<ListenerConnectorRef>> listeners) {
    this.listeners = listeners;
    return this;
  }

  public Map<String, List<ListenerConnectorRef>> getProcessors() {
    return processors;
  }

  public void setProcessors(final Map<String, List<ListenerConnectorRef>> processors) {
    this.processors = processors;
  }

  public Space withProcessors(final Map<String, List<ListenerConnectorRef>> processors) {
    this.processors = processors;
    return this;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(final String owner) {
    this.owner = owner;
  }

  public Space withOwner(final String owner) {
    this.owner = owner;
    return this;
  }

  public int getCacheTTL() {
    return cacheTTL;
  }

  public void setCacheTTL(final int cacheTTL) {
    this.cacheTTL = cacheTTL;
  }

  public Space withCacheTTL(final int cacheTTL) {
    this.cacheTTL = cacheTTL;
    return this;
  }

  public Map<String, Object> getClient() {
    return client;
  }

  public void setClient(final Map<String, Object> client) {
    this.client = client;
  }

  public Space withClient(final Map<String, Object> client) {
    this.client = client;
    return this;
  }

  public boolean isEnableUUID() {
    return enableUUID;
  }

  public void setEnableUUID(final boolean enableUUID) {
    this.enableUUID = enableUUID;
  }

  public Space withEnableUUID(final boolean enableUUID) {
    this.enableUUID = enableUUID;
    return this;
  }

  public boolean isEnableHistory() {
    return enableHistory;
  }

  public void setEnableHistory(final boolean enableHistory) {
    this.enableHistory = enableHistory;
  }

  public Space withEnableHistory(final boolean enableHistory) {
    this.enableHistory = enableHistory;
    return this;
  }

  public Integer getMaxVersionCount() {
    return maxVersionCount;
  }

  public void setMaxVersionCount(final Integer maxVersionCount) {
    this.maxVersionCount = maxVersionCount;
  }

  public Space withMaxVersionCount(final Integer maxVersionCount) {
    this.maxVersionCount = maxVersionCount;
    return this;
  }

  public List<String> getPackages() {
    return packages;
  }

  public void setPackages(final List<String> packages) {
    this.packages = packages;
  }

  public Space withPackages(final List<String> packages) {
    this.packages = packages;
    return this;
  }

  public String getCid() {
    return cid;
  }

  public void setCid(final String cid) {
    this.cid = cid;
  }

  public Space withCid(final String cid) {
    this.cid = cid;
    return this;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(final List<String> tags) {
    this.tags = tags;
  }

  public Space withTags(final List<String> tags) {
    this.tags = tags;
    return this;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(final long createdAt) {
    this.createdAt = createdAt;
  }

  public Space withCreatedAt(final long createdAt) {
    this.createdAt = createdAt;
    return this;
  }

  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(final long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public Space withUpdatedAt(final long updatedAt) {
    this.updatedAt = updatedAt;
    return this;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(final boolean readOnly) {
    this.readOnly = readOnly;
  }

  public Space withReadOnly(final boolean readOnly) {
    this.readOnly = readOnly;
    return this;
  }

  public Map<String, Boolean> getSearchableProperties() {
    return searchableProperties;
  }

  public void setSearchableProperties(final Map<String, Boolean> searchableProperties) {
    this.searchableProperties = searchableProperties;
  }

  public Space withSearchableProperties(final Map<String, Boolean> searchableProperties) {
    this.searchableProperties = searchableProperties;
    return this;
  }

  @SuppressWarnings("WeakerAccess")
  public static class Public {

  }

  @SuppressWarnings("WeakerAccess")
  public static class WithConnectors extends Public {

  }

  public static class Internal extends WithConnectors {

  }

  /**
   * Used for properties which are intended to be persisted.
   */
  public static class Static {

  }

  /**
   * The reference to a connector configuration.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConnectorRef {

    /**
     * The ID of the connector to be used.
     */
    @JsonInclude(Include.NON_NULL)
    private String id;

    /**
     * Arbitrary parameters to be provided to the connector.
     */
    private Map<String, Object> params;

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    @SuppressWarnings("WeakerAccess")
    public ConnectorRef withId(final String id) {
      this.id = id;
      return this;
    }

    public Map<String, Object> getParams() {
      return params;
    }

    public void setParams(final Map<String, Object> params) {
      this.params = params;
    }

    @SuppressWarnings("WeakerAccess")
    public ConnectorRef withParams(final Map<String, Object> params) {
      this.params = params;
      return this;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ListenerConnectorRef extends ConnectorRef {

    /**
     * The types of events the listener-connector is registered for.
     */
    private List<String> eventTypes;

    /**
     * The order of when the listener-connector is called.
     */
    @JsonInclude(Include.NON_NULL)
    private Integer order;

    @SuppressWarnings("WeakerAccess")
    public List<String> getEventTypes() {
      return eventTypes;
    }

    public void setEventTypes(final List<String> eventTypes) {
      this.eventTypes = eventTypes;
    }

    @SuppressWarnings("WeakerAccess")
    public ListenerConnectorRef withEventTypes(final List<String> eventTypes) {
      this.eventTypes = eventTypes;
      return this;
    }

    public Integer getOrder() {
      return order;
    }

    @SuppressWarnings("WeakerAccess")
    public void setOrder(final Integer  order) {
      this.order = order;
    }

    @SuppressWarnings("WeakerAccess")
    public ListenerConnectorRef withOrder(final Integer order) {
      this.order = order;
      return this;
    }
  }

  /**
   * The copyright information object.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Copyright {

    /**
     * The copyright label to be displayed by the client.
     */
    @JsonInclude(Include.NON_NULL)
    private String label;

    /**
     * The description text for the label to be displayed by the client.
     */
    @JsonInclude(Include.NON_NULL)
    private String alt;

    public String getLabel() {
      return label;
    }

    public void setLabel(final String label) {
      this.label = label;
    }

    public Copyright withLabel(final String label) {
      this.label = label;
      return this;
    }

    public String getAlt() {
      return alt;
    }

    public void setAlt(final String alt) {
      this.alt = alt;
    }

    public Copyright withAlt(final String alt) {
      this.alt = alt;
      return this;
    }
  }

  public static class License {

    //Source: https://github.com/shinnn/spdx-license-ids/blob/master/index.json
    //Information about the licenses can be found here: https://spdx.org/licenses/ OR https://choosealicense.com/licenses/<keyword>
    private static List<String> allowedKeywords = Arrays.asList(
        "AFL-3.0",
        "Apache-2.0",
        "Artistic-2.0",
        "BSL-1.0",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "BSD-3-Clause-Clear",
        "CC0-1.0",
        "CC-BY-4.0",
        "CC-BY-SA-4.0",
        "WTFPL",
        "ECL-1.0",
        "ECL-2.0",
        "EUPL-1.1",
        "AGPL-3.0-only",
        "GPL-2.0-only",
        "GPL-3.0-only",
        "LGPL-2.1-only",
        "LGPL-3.0-only",
        "ISC",
        "LPPL-1.3c",
        "MS-PL",
        "MIT",
        "MPL-2.0",
        "OSL-3.0",
        "PostgreSQL",
        "OFL-1.1",
        "NCSA",
        "Unlicense",
        "Zlib",
        "ODbL-1.0"
    );

    @JsonValue
    private String keyword;

    public String getKeyword() {
      return keyword;
    }

    public void setKeyword(final String keyword) {
      this.keyword = keyword;
    }

    public License withKeyword(final String keyword) {
      this.keyword = keyword;
      return this;
    }

    @JsonCreator
    public static License forKeyword(String keyword) {
      License l = new License();
      if (!allowedKeywords.contains(keyword)) {
        throw new IllegalArgumentException(
            "\"" + keyword + "\" is not a valid license keyword. Allowed keywords are: " + String.join(", ", allowedKeywords));
      }
      l.keyword = keyword;
      return l;
    }
  }
}
