/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.models.geojson.implementation.namespaces;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Action;
import java.text.Normalizer;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class XyzActivityLog implements XyzSerializable {

  //public static final String XYZ_NAMESPACE = "@ns:com:here:xyz";
  //public static final String CREATED_AT = "createdAt";
  //public static final String UUID = "uuid";
  //public static final String PUUID = "puuid";
  //public static final String MUUID = "muuid";
  //public static final String REVISION = "revision";
  //public static final String SPACE = "space";
  //public static final String UPDATED_AT = "updatedAt";
  public static final String ID = "id";
  public static final String ORIGINAL = "original";
  public static final String ACTION = "action";
  public static final String INVALIDATED_AT = "invalidatedAt";

  public XyzActivityLog() {
    this.original = new Original();
  }

  /**
   * The Original tag.
   */
  @JsonProperty(ORIGINAL)
  private @NotNull Original original;

  /**
   * The space ID the feature belongs to.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  private String space;*/

  /**
   * The space ID the feature belongs to.
   */
  @JsonProperty(ID)
  @JsonView(View.All.class)
  private String id;

  /**
   * * * The timestamp, when a feature was created.
   */
  @JsonProperty(INVALIDATED_AT)
  @JsonView(View.All.class)
  private long invalidatedAt;

  /**
   * The collection the feature belongs to.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  private String collection;

  *//**
   * The timestamp, when a feature was created.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  private long createdAt;

  *//**
   * The timestamp, when a feature was last updated.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  private long updatedAt;

  *//**
   * The transaction number of this state.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  private String txn;

  *//**
   * The uuid of the feature, when the client modifies the feature, it must not modify the uuid.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  private String uuid;

  *//**
   * The previous uuid of the feature.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String puuid;

  *//**
   * The merge uuid of the feature.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String muuid;

  *//**
   * The list of tags attached to the feature.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_EMPTY)
  private List<@NotNull String> tags;*/

  /**
   * The operation that lead to the current state of the namespace. Should be a value from {@link Action}.
   */
  @JsonProperty(ACTION)
  @JsonView(View.All.class)
  private String action;

  /**
   * The version of the feature, the first version (1) will always be in the state CREATED.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  private long version = 0L;

  *//**
   * The author (user or application) that created the current revision of the feature.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String author;

  *//**
   * The application that create the current revision of the feature.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String app_id;

  *//**
   * The identifier of the owner of this connector.
   *//*
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String owner;*/

  /*public @Nullable String getSpace() {
    return space;
  }

  public void setSpace(@Nullable String space) {
    this.space = space;
  }

  public @NotNull XyzActivityLog withSpace(@Nullable String space) {
    setSpace(space);
    return this;
  }

  public @Nullable String getCollection() {
    return collection;
  }

  public void setCollection(@Nullable String collection) {
    this.collection = collection;
  }

  public @NotNull XyzActivityLog withCollection(@Nullable String collection) {
    setCollection(collection);
    return this;
  }*/

  public @Nullable String getAction() {
    return action;
  }

  public void setAction(@Nullable String action) {
    this.action = action;
  }

  public void setAction(@NotNull Action action) {
    this.action = action.toString();
  }

  public @NotNull XyzActivityLog withAction(@Nullable String action) {
    setAction(action);
    return this;
  }

  public @NotNull XyzActivityLog withAction(@NotNull Action action) {
    setAction(action);
    return this;
  }

  public long getInvalidatedAt() {
    return invalidatedAt;
  }

  public void setInvalidatedAt(long invalidatedAt) {
    this.invalidatedAt = invalidatedAt;
  }

/*
  public long getCreatedAt() {
    return createdAt;
  }


  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public @NotNull XyzActivityLog withCreatedAt(long createdAt) {
    setCreatedAt(createdAt);
    return this;
  }


  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public @NotNull XyzActivityLog withUpdatedAt(long updatedAt) {
    setUpdatedAt(updatedAt);
    return this;
  }


  public @Nullable String getTxn() {
    return txn;
  }


  public void setTxn(@Nullable String txn) {
    this.txn = txn;
  }

  public @NotNull XyzActivityLog withTxn(@Nullable String txn) {
    setTxn(txn);
    return this;
  }

  public @Nullable String getUuid() {
    return uuid;
  }

  public void setUuid(@Nullable String uuid) {
    this.uuid = uuid;
  }

  public @NotNull XyzActivityLog withUuid(@Nullable String uuid) {
    setUuid(uuid);
    return this;
  }

  public @Nullable String getPuuid() {
    return puuid;
  }

  public void setPuuid(@Nullable String puuid) {
    this.puuid = puuid;
  }

  public @NotNull XyzActivityLog withPuuid(@Nullable String puuid) {
    setPuuid(puuid);
    return this;
  }


  public @Nullable String getMuuid() {
    return muuid;
  }


  public void setMuuid(@Nullable String muuid) {
    this.muuid = muuid;
  }

  public @NotNull XyzActivityLog withMuuid(@Nullable String muuid) {
    setMuuid(muuid);
    return this;
  }

  public @Nullable List<@NotNull String> getTags() {
    return tags;
  }

  public void setTags(@Nullable List<@NotNull String> tags) {
    if (tags != null) {
      for (int SIZE = tags.size(), i = 0; i < SIZE; i++) {
        tags.set(i, normalizeTag(tags.get(i)));
      }
    }
    this.tags = tags;
  }

  public @NotNull XyzActivityLog withTags(@Nullable List<@NotNull String> tags) {
    setTags(tags);
    return this;
  }

  *//**
   * Returns 'true' if the tag was added, 'false' if it was already present.
   *
   * @return true if the tag was added; false otherwise.
   *//*
  public boolean addTag(String tag) {
    if (getTags() == null) {
      setTags(new ArrayList<>());
    }
    if (getTags().contains(tag)) {
      return false;
    }
    return getTags().add(tag);
  }

  */

  /**
   * Returns 'true' if the tag was removed, 'false' if it was not present.
   *
   * @return true if the tag was removed; false otherwise.
   *//*
  public boolean removeTag(String tag) {
    if (getTags() == null) {
      return false;
    }
    if (!getTags().contains(tag)) {
      return false;
    }
    return getTags().remove(tag);
  }*/
  public boolean isDeleted() {
    return Action.DELETE.equals(getAction());
  }

  public void setDeleted(boolean deleted) {
    if (deleted) {
      setAction(Action.DELETE);
    }
  }

  public @NotNull XyzActivityLog withDeleted(boolean deleted) {
    setDeleted(deleted);
    return this;
  }

  /*public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public @NotNull XyzActivityLog withVersion(long version) {
    setVersion(version);
    return this;
  }

  public @Nullable String getAuthor() {
    return author;
  }

  public void setAuthor(@Nullable String author) {
    this.author = author;
  }

  public @NotNull XyzActivityLog withAuthor(@Nullable String author) {
    setAuthor(author);
    return this;
  }

  public @Nullable String getAppId() {
    return app_id;
  }

  public void setAppId(@Nullable String app_id) {
    this.app_id = app_id;
  }

  public @NotNull XyzActivityLog withAppId(@Nullable String app_id) {
    setAppId(app_id);
    return this;
  }

  public @Nullable String getOwner() {
    return owner;
  }

  public void setOwner(@Nullable String owner) {
    this.owner = owner;
  }*/

  public @Nullable String getId() {
    return id;
  }

  public void setId(@Nullable String id) {
    this.id = id;
  }

  public @NotNull Original getOriginal() {
    return original;
  }

  public void setOrigin(@NotNull Original original) {
    this.original = original;
  }

}
