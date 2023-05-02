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

package com.here.xyz.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import com.here.xyz.XyzSerializable;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class XyzNamespace implements XyzSerializable {

  public static final String XYZ_NAMESPACE = "@ns:com:here:xyz";

  /**
   * The space ID the feature belongs to.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private String space;

  /**
   * The collection the feature belongs to.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private String collection;

  /**
   * The timestamp, when a feature was created.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private long createdAt;

  /**
   * The timestamp, when a feature was last updated.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private long updatedAt;

  /**
   * The transaction number of this state.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private String txn;

  /**
   * The uuid of the feature, when the client modifies the feature, it must not modify the uuid.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private String uuid;

  /**
   * The previous uuid of the feature.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String puuid;

  /**
   * The merge uuid of the feature.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String muuid;

  /**
   * The list of tags attached to the feature.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_EMPTY)
  private List<@NotNull String> tags;

  /**
   * The operation that lead to the current state of the namespace. Should be a value from {@link Action}.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private String action;

  /**
   * The version of the feature, the first version (1) will always be in the state CREATED.
   */
  @JsonProperty
  @JsonView(View.All.class)
  private long version = 0L;

  /**
   * The author that changed the feature in the current revision.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String author;

  /**
   * The application that changed the feature in the current revision.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String app_id;

  /**
   * The identifier of the owner of this connector.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_NULL)
  private String owner;

  /**
   * A method to normalize and lower case a tag.
   *
   * @param tag the tag.
   * @return the normalized and lower cased version of it.
   */
  public static @NotNull String normalizeTag(final @NotNull String tag) {
    if (tag.length() == 0) {
      return tag;
    }
    final char first = tag.charAt(0);
    if (first == '@') // All tags starting with an at-sign, will not be modified in any way.
    {
      return tag;
    }

    String normalized = Normalizer.normalize(tag, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
    // All tags starting with a tilde or sharp will not be lower cased, same as deprecated "ref_" prefix.
    if (first != '~' && !tag.startsWith("ref_") && !tag.startsWith("sourceID_")) {
      normalized = normalized.toLowerCase();
    }

    return normalized;
  }

  /**
   * A method to normalize a list of tags.
   *
   * @param tags a list of tags.
   * @return the same list, just that the content is normalized.
   */

  public static @Nullable List<@NotNull String> normalizeTags(final @Nullable List<@NotNull String> tags) {
    if (tags != null) {
      for (int SIZE = tags.size(), i = 0; i < SIZE; i++) {
        tags.set(i, normalizeTag(tags.get(i)));
      }
    }
    return tags;
  }

  /**
   * This method is a hot-fix for an issue of plenty of frameworks. For example vertx does automatically URL decode query parameters (as
   * certain other frameworks may as well). This is often very hard to fix, even while RFC-3986 is very clear about that reserved characters
   * may have semantic meaning when not being URI encoded and MUST be URI encoded to take away the meaning. Therefore there normally must be
   * a way to detect if a reserved character in a query parameter was originally URI encoded or not, because in the later case it may have a
   * semantic meaning.
   * <p>
   * As many frameworks fail to follow this very important detail, this method fixes tags for all those frameworks, effectively it removes
   * the commas from tags and splits the tags by the comma. Therefore a comma is not allowed as part of a tag.
   *
   * @param tags The list of tags, will be modified if any tag contains a comma (so may extend).
   * @see [https://tools.ietf.org/html/rfc3986#section-2.2]
   */

  public static void fixNormalizedTags(final @NotNull List<@NotNull String> tags) {
    int j = 0;
    StringBuilder sb = null;
    while (j < tags.size()) {
      String tag = tags.get(j);
      if (tag.indexOf(',') >= 0) {
        // If there is a comma in the tag, we need to split it into multiple tags or at least remove the comma
        // (when it is the first or last character).
        if (sb == null) {
          sb = new StringBuilder(256);
        }

        boolean isModified = false;
        for (int i = 0; i < tag.length(); i++) {
          final char c = tag.charAt(i);
          if (c != ',') {
            sb.append(c);
          } else if (sb.length() == 0) {
            // The first character is a comma, we ignore that.
            isModified = true;
          } else {
            // All characters up to the comma will be added as new tag. Then parse the rest as replacement for the current tag.
            tags.add(sb.toString());
            sb.setLength(0);
            isModified = true;
          }
        }

        if (isModified) {
          if (sb.length() > 0) {
            tags.set(j, sb.toString());
            j++;
          } else {
            tags.remove(j);
            // We must not increment j, because we need to re-read "j" as it now is a new tag!
          }
        }
      } else {
        // When there is no comma in the tag, just continue with the next tag.
        j++;
      }
    }
  }

  public @Nullable String getSpace() {
    return space;
  }

  public void setSpace(@Nullable String space) {
    this.space = space;
  }

  public @NotNull XyzNamespace withSpace(@Nullable String space) {
    setSpace(space);
    return this;
  }

  public @Nullable String getCollection() {
    return collection;
  }

  public void setCollection(@Nullable String collection) {
    this.collection = collection;
  }

  public @NotNull XyzNamespace withCollection(@Nullable String collection) {
    setCollection(collection);
    return this;
  }

  public @Nullable String getAction() {
    return action;
  }

  public void setAction(@Nullable String action) {
    this.action = action;
  }

  public void setAction(@NotNull Action action) {
    this.action = action.toString();
  }


  public @NotNull XyzNamespace withAction(@Nullable String action) {
    setAction(action);
    return this;
  }

  public @NotNull XyzNamespace withAction(@NotNull Action action) {
    setAction(action);
    return this;
  }


  public long getCreatedAt() {
    return createdAt;
  }


  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public @NotNull XyzNamespace withCreatedAt(long createdAt) {
    setCreatedAt(createdAt);
    return this;
  }


  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public @NotNull XyzNamespace withUpdatedAt(long updatedAt) {
    setUpdatedAt(updatedAt);
    return this;
  }


  public @Nullable String getTxn() {
    return txn;
  }


  public void setTxn(@Nullable String txn) {
    this.txn = txn;
  }

  public @NotNull XyzNamespace withTxn(@Nullable String txn) {
    setTxn(txn);
    return this;
  }

  public @Nullable String getUuid() {
    return uuid;
  }

  public void setUuid(@Nullable String uuid) {
    this.uuid = uuid;
  }

  public @NotNull XyzNamespace withUuid(@Nullable String uuid) {
    setUuid(uuid);
    return this;
  }

  public @Nullable String getPuuid() {
    return puuid;
  }

  public void setPuuid(@Nullable String puuid) {
    this.puuid = puuid;
  }

  public @NotNull XyzNamespace withPuuid(@Nullable String puuid) {
    setPuuid(puuid);
    return this;
  }


  public @Nullable String getMuuid() {
    return muuid;
  }


  public void setMuuid(@Nullable String muuid) {
    this.muuid = muuid;
  }

  public @NotNull XyzNamespace withMuuid(@Nullable String muuid) {
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

  public @NotNull XyzNamespace withTags(@Nullable List<@NotNull String> tags) {
    setTags(tags);
    return this;
  }

  /**
   * Returns 'true' if the tag was added, 'false' if it was already present.
   *
   * @return true if the tag was added; false otherwise.
   */
  public boolean addTag(String tag) {
    if (getTags() == null) {
      setTags(new ArrayList<>());
    }
    if (getTags().contains(tag)) {
      return false;
    }
    return getTags().add(tag);
  }

  /**
   * Returns 'true' if the tag was removed, 'false' if it was not present.
   *
   * @return true if the tag was removed; false otherwise.
   */
  public boolean removeTag(String tag) {
    if (getTags() == null) {
      return false;
    }
    if (!getTags().contains(tag)) {
      return false;
    }
    return getTags().remove(tag);
  }

  public boolean isDeleted() {
    return Action.DELETE.equals(getAction());
  }

  public void setDeleted(boolean deleted) {
    if (deleted) {
      setAction(Action.DELETE);
    }
  }

  public @NotNull XyzNamespace withDeleted(boolean deleted) {
    setDeleted(deleted);
    return this;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public @NotNull XyzNamespace withVersion(long version) {
    setVersion(version);
    return this;
  }

  public @Nullable String getAuthor() {
    return author;
  }

  public void setAuthor(@Nullable String author) {
    this.author = author;
  }

  public @NotNull XyzNamespace withAuthor(@Nullable String author) {
    setAuthor(author);
    return this;
  }

  public @Nullable String getAppId() {
    return app_id;
  }

  public void setAppId(@Nullable String app_id) {
    this.app_id = app_id;
  }

  public @NotNull XyzNamespace withAppId(@Nullable String app_id) {
    setAppId(app_id);
    return this;
  }

  public @Nullable String getOwner() {
    return owner;
  }

  public void setOwner(@Nullable String owner) {
    this.owner = owner;
  }


  public @NotNull XyzNamespace withOwner(@Nullable String owner) {
    setOwner(owner);
    return this;
  }
}
