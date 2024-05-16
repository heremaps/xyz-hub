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
package com.here.naksha.lib.core.models.geojson.implementation.namespaces;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.util.json.JsonObject;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The properties stored as value for the {@link XyzProperties#XYZ_NAMESPACE @ns:com:here:xyz} key in the {@link XyzProperties properties}
 * of features managed by Naksha. Except for the {@code tags} and {@code crid} all these values are read-only.
 */
@SuppressWarnings("unused")
public class XyzNamespace extends JsonObject {

  /**
   * The name of the createAt property.
   */
  public static final String CREATED_AT = "createdAt";

  /**
   * The timestamp in Epoch-Millis, when the feature was created.
   */
  @JsonProperty(CREATED_AT)
  @JsonInclude(Include.NON_DEFAULT)
  private long createdAt;

  /**
   * The name of the updatedAt property.
   */
  public static final String UPDATED_AT = "updatedAt";

  /**
   * The transaction start timestamp in Epoch-Millis, when the feature was updated.
   */
  @JsonProperty(UPDATED_AT)
  @JsonInclude(Include.NON_DEFAULT)
  private long updatedAt;

  /**
   * The name of the rt_ts property.
   */
  public static final String RT_UTS = "rt_ts";

  /**
   * The realtime timestamp in Epoch-Millis, when the feature was updated.
   */
  @JsonProperty(RT_UTS)
  @JsonInclude(Include.NON_DEFAULT)
  private long rt_ts;

  /**
   * The name of the txn property.
   */
  public static final String TXN = "txn";

  /**
   * The transaction number of this feature state.
   */
  @JsonProperty(TXN)
  @JsonInclude(Include.NON_DEFAULT)
  private long txn;

  /**
   * The name of the txn_next property.
   */
  public static final String TXN_NEXT = "txn_next";

  /**
   * The transaction number of the next version of this feature. This is zero, if this is currently the latest version.
   */
  @JsonProperty(TXN_NEXT)
  @JsonInclude(Include.NON_DEFAULT)
  private long txn_next;

  /**
   * The name of the txn_uuid property.
   */
  public static final String TXN_UUID = "txn_uuid";

  /**
   * The GUID of the transaction of which this state of the feature is part.
   */
  @JsonProperty(TXN_UUID)
  @JsonInclude(Include.NON_EMPTY)
  private String txn_uuid;

  /**
   * The name of the uuid property.
   */
  public static final String UUID = "uuid";

  /**
   * The uuid of the current state of the feature. When the client modifies the feature, it must not modify the uuid. For change requests
   * the uuid is read and used to identify the base state that was modified.
   */
  @JsonProperty(UUID)
  @JsonInclude(Include.NON_EMPTY)
  private String uuid;

  /**
   * The name of the puuid property.
   */
  public static final String PUUID = "puuid";

  /**
   * The uuid of the previous feature state; {@code null} if this feature is new and has no previous state.
   */
  @JsonProperty(PUUID)
  @JsonInclude(Include.NON_EMPTY)
  private String puuid;

  /**
   * The name of the action property.
   */
  public static final String ACTION = "action";

  /**
   * The operation that lead to the current state of the namespace. Should be a value from {@link EXyzAction}.
   */
  @JsonProperty(ACTION)
  private EXyzAction action;

  /**
   * The name of the version property.
   */
  public static final String VERSION = "version";

  /**
   * The version of the feature, the first version (1) will always have action {@link EXyzAction#CREATE}.
   */
  @JsonProperty(VERSION)
  @JsonInclude(Include.NON_DEFAULT)
  private long version;

  /**
   * The name of the app_id property.
   */
  public static final String APP_ID = "app_id";

  /**
   * The application that create the current revision of the feature.
   */
  @JsonProperty(APP_ID)
  @JsonInclude(Include.NON_EMPTY)
  private String app_id;

  /**
   * The name of the author property.
   */
  public static final String AUTHOR = "author";

  /**
   * The author (user or application) that created the current revision of the feature.
   */
  @JsonProperty(AUTHOR)
  @JsonInclude(Include.NON_EMPTY)
  private String author;

  /**
   * The name of the author_ts property.
   */
  public static final String AUTHOR_TS = "author_ts";

  /**
   * The epoch timestamp in milliseconds when the author did the last edit.
   */
  @JsonProperty(AUTHOR_TS)
  @JsonInclude(Include.NON_DEFAULT)
  private long author_ts;

  /**
   * The name of the tags property.
   */
  public static final String TAGS = "tags";

  /**
   * The list of tags attached to the feature.
   */
  @JsonProperty(TAGS)
  @JsonInclude(Include.NON_EMPTY)
  private List<@NotNull String> tags;

  /**
   * The name of the crid property.
   */
  public static final String CRID = "crid";

  /**
   * The customer reference identifier.
   */
  @JsonProperty(CRID)
  @JsonInclude(Include.NON_EMPTY)
  private String crid;

  /**
   * The name of the grid property.
   */
  public static final String GRID = "grid";

  /**
   * The geometry reference identifier, which is basically a Geo-Hash in level 14. This is automatically calculated based upon the
   * {@link XyzFeature#getReferencePoint() reference point}, if no {@link XyzFeature#getReferencePoint() reference point} is available, the
   * value is calculated from the centroid of the {@link XyzFeature#getGeometry() geometry}, if no geometry is available, then it is
   * calculated from the {@link XyzFeature#getId() id} of the feature.
   */
  @JsonProperty(GRID)
  @JsonInclude(Include.NON_EMPTY)
  private String grid;

  /**
   * The name of the extend property.
   */
  public static final String EXTEND = "extend";

  /**
   * The maximal extension of the feature in milliseconds. Features being points of that have no geometry, will have an extension of zero.
   * This can be used to limit features returned in higher zoom level, so that only features that are at least one pixel in size are
   * returned (only useful in combination with bounding box queries).
   */
  @JsonProperty(EXTEND)
  @JsonInclude(Include.NON_DEFAULT)
  private long extend;

  /**
   * The name of the stream_id property.
   */
  public static final String STREAM_ID = "stream_id";

  /**
   * The stream-identifier that was used to create this feature state.
   */
  @JsonProperty(STREAM_ID)
  @JsonInclude(Include.NON_EMPTY)
  private String stream_id;

  private static final char[] TO_LOWER;
  private static final char[] AS_IS;

  static {
    TO_LOWER = new char[128 - 32];
    AS_IS = new char[128 - 32];
    for (char c = 32; c < 128; c++) {
      AS_IS[c - 32] = c;
      TO_LOWER[c - 32] = Character.toLowerCase(c);
    }
  }

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
    // All tags starting with an at-sign, will not be modified in any way.
    if (first == '@') {
      return tag;
    }

    // Normalize the tag.
    final String normalized = Normalizer.normalize(tag, Form.NFD);

    // All tags starting with a tilde, sharp, or the deprecated "ref_" / "sourceID_" prefix will not
    // be lower cased.
    final char[] MAP =
        first == '~' || first == '#' || normalized.startsWith("ref_") || normalized.startsWith("sourceID_")
            ? AS_IS
            : TO_LOWER;
    final StringBuilder sb = new StringBuilder(normalized.length());
    for (int i = 0; i < normalized.length(); i++) {
      // Note: This saves one branch, and the array-size check, because 0 - 32 will become 65504.
      final char c = (char) (normalized.charAt(i) - 32);
      if (c < MAP.length) {
        sb.append(MAP[c]);
      }
    }
    return sb.toString();
  }

  /**
   * A method to normalize a list of tags.
   *
   * @param tags a list of tags.
   * @return the same list, just that the content is normalized.
   */
  public static @Nullable List<@NotNull String> normalizeTags(final @Nullable List<@NotNull String> tags) {
    final int SIZE;
    if (tags != null && (SIZE = tags.size()) > 0) {
      for (int i = 0; i < SIZE; i++) {
        tags.set(i, normalizeTag(tags.get(i)));
      }
    }
    return tags;
  }

  /**
   * This method is a hot-fix for an issue of plenty of frameworks. For example vertx does automatically URL decode query parameters (as
   * certain other frameworks may as well). This is often hard to fix, even while RFC-3986 is clear about that reserved characters may have
   * semantic meaning when not being URI encoded and MUST be URI encoded to take away the meaning. Therefore, there normally must be a way
   * to detect if a reserved character in a query parameter was originally URI encoded or not, because in the later case it may have a
   * semantic meaning.
   *
   * <p>As many frameworks fail to follow this important detail, this method fixes tags for all
   * those frameworks, effectively it removes the commas from tags and splits the tags by the comma. Therefore, a comma is not allowed as
   * part of a tag.
   *
   * @param tags The list of tags, will be modified if any tag contains a comma (so may extend).
   * @see <a href="https://tools.ietf.org/html/rfc3986#section-2.2">https://tools.ietf.org/html/rfc3986#section-2.2</a>
   */
  @Deprecated
  public static void fixNormalizedTags(final @NotNull List<@NotNull String> tags) {
    int j = 0;
    StringBuilder sb = null;
    while (j < tags.size()) {
      String tag = tags.get(j);
      if (tag.indexOf(',') >= 0) {
        // If there is a comma in the tag, we need to split it into multiple tags or at least remove
        // the comma
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
            // All characters up to the comma will be added as new tag. Then parse the rest as
            // replacement for the current tag.
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

  /**
   * Returns the string underlying the action enumeration value.
   *
   * @return the string underlying the action enumeration value.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable String rawAction() {
    return action == null ? null : action.toString();
  }

  /**
   * Returns the action.
   *
   * @return The action.
   */
  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable EXyzAction getAction() {
    return action;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setAction(@Nullable String action) {
    this.action = EXyzAction.get(EXyzAction.class, action);
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setAction(@NotNull EXyzAction action) {
    this.action = action;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withAction(@Nullable String action) {
    setAction(action);
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withAction(@NotNull EXyzAction action) {
    setAction(action);
    return this;
  }

  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_7)
  public long getCreatedAt() {
    return createdAt;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withCreatedAt(long createdAt) {
    setCreatedAt(createdAt);
    return this;
  }

  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_7)
  public long getUpdatedAt() {
    return updatedAt;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withUpdatedAt(long updatedAt) {
    setUpdatedAt(updatedAt);
    return this;
  }

  /**
   * Returns the realtime update timestamp as epoch milliseconds.
   *
   * @return the realtime update timestamp as epoch milliseconds.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonIgnore
  public long getRealTimeUpdatedAt() {
    return rt_ts;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setRealTimeUpdatedAt(long updatedAt) {
    this.rt_ts = updatedAt;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withRealTimeUpdatedAt(long updatedAt) {
    setRealTimeUpdatedAt(updatedAt);
    return this;
  }

  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_8)
  public long getTxn() {
    return txn;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public void setTxn(long txn) {
    this.txn = txn;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public @NotNull XyzNamespace withTxn(long txn) {
    setTxn(txn);
    return this;
  }

  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_8)
  public long getTxnNext() {
    return txn_next;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public void setTxnNext(long txn_next) {
    this.txn_next = txn_next;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public @NotNull XyzNamespace withTxnNext(long txn_next) {
    setTxnNext(txn_next);
    return this;
  }

  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_8)
  public @Nullable String getTxnUuid() {
    return txn_uuid;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public void setTxnUuid(@Nullable String txn_uuid) {
    this.txn_uuid = txn_uuid;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public @NotNull XyzNamespace withTxnUuid(@Nullable String txn_uuid) {
    setTxnUuid(txn_uuid);
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonIgnore
  public @Nullable String getUuid() {
    return uuid;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setUuid(@Nullable String uuid) {
    this.uuid = uuid;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withUuid(@Nullable String uuid) {
    setUuid(uuid);
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonIgnore
  public @Nullable String getPuuid() {
    return puuid;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setPuuid(@Nullable String puuid) {
    this.puuid = puuid;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withPuuid(@Nullable String puuid) {
    setPuuid(puuid);
    return this;
  }

  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable List<@NotNull String> getTags() {
    return tags;
  }

  /**
   * Set the tags to the given array.
   *
   * @param tags      The tags to set.
   * @param normalize {@code true} if the given tags should be normalized; {@code false}, if they are already normalized.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace setTags(@Nullable List<@NotNull String> tags, boolean normalize) {
    if (normalize) {
      final int SIZE;
      if (tags != null && (SIZE = tags.size()) > 0) {
        for (int i = 0; i < SIZE; i++) {
          tags.set(i, normalizeTag(tags.get(i)));
        }
      }
    }
    this.tags = tags;
    return this;
  }

  /**
   * Returns 'true' if the tag added, 'false' if it was already present.
   *
   * @param tag       The tag to add.
   * @param normalize {@code true} if the tag should be normalized; {@code false} otherwise.
   * @return true if the tag added; false otherwise.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public boolean addTag(@NotNull String tag, boolean normalize) {
    List<@NotNull String> thisTags = getTags();
    if (thisTags == null) {
      thisTags = this.tags = new ArrayList<>();
    }
    if (normalize) {
      tag = normalizeTag(tag);
    }
    if (!thisTags.contains(tag)) {
      thisTags.add(tag);
      return true;
    }
    return false;
  }

  /**
   * Add the given tags.
   *
   * @param tags      The tags to add.
   * @param normalize {@code true} if the given tags should be normalized; {@code false}, if they are already normalized.
   * @return this.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace addTags(@Nullable List<@NotNull String> tags, boolean normalize) {
    final int SIZE;
    List<@NotNull String> thisTags = this.tags;
    if (thisTags == null) {
      thisTags = this.tags = new ArrayList<>();
    }
    if (tags != null && tags.size() > 0) {
      if (normalize) {
        for (final @NotNull String s : tags) {
          final String tag = normalizeTag(s);
          if (!thisTags.contains(tag)) {
            thisTags.add(tag);
          }
        }
      } else {
        thisTags.addAll(tags);
      }
    }
    return this;
  }

  /**
   * Add and normalize all given tags.
   *
   * @param tags The tags to normalize and add.
   * @return this.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace addAndNormalizeTags(@NotNull String... tags) {
    final int SIZE;
    List<@NotNull String> thisTags = this.tags;
    if (thisTags == null) {
      thisTags = this.tags = new ArrayList<>();
    }
    if (tags != null && tags.length > 0) {
      for (final @NotNull String s : tags) {
        final String tag = normalizeTag(s);
        if (!thisTags.contains(tag)) {
          thisTags.add(tag);
        }
      }
    }
    return this;
  }

  /**
   * Returns 'true' if the tag was removed, 'false' if it was not present.
   *
   * @param tag       The normalized tag to remove.
   * @param normalize {@code true} if the tag should be normalized before trying to remove; {@code false} if the tag is normalized.
   * @return true if the tag was removed; false otherwise.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public boolean removeTag(@NotNull String tag, boolean normalize) {
    final List<@NotNull String> thisTags = getTags();
    if (thisTags == null) {
      return false;
    }
    if (normalize) {
      tag = normalizeTag(tag);
    }
    return thisTags.remove(tag);
  }

  /**
   * Removes the given tags.
   *
   * @param tags      The tags to remove.
   * @param normalize {@code true} if the tags should be normalized before trying to remove; {@code false} if the tags are normalized.
   * @return this.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace removeTags(@Nullable List<@NotNull String> tags, boolean normalize) {
    final List<@NotNull String> thisTags = getTags();
    if (thisTags == null || thisTags.size() == 0 || tags == null || tags.size() == 0) {
      return this;
    }
    if (normalize) {
      for (@NotNull String tag : tags) {
        tag = normalizeTag(tag);
        thisTags.remove(tag);
      }
    } else {
      thisTags.removeAll(tags);
    }
    return this;
  }

  /**
   * Removes tags starting with prefix
   *
   * @param prefix string prefix.
   * @return this.
   */
  @AvailableSince(NakshaVersion.v2_0_11)
  public @NotNull XyzNamespace removeTagsWithPrefix(final String prefix) {
    final List<@NotNull String> thisTags = getTags();
    if (thisTags == null || thisTags.isEmpty() || prefix == null) {
      return this;
    }

    thisTags.removeIf(tag -> tag.startsWith(prefix));
    return this;
  }

  /**
   * Removes tags starting with given list of prefixes
   *
   * @param prefixes list of tag prefixes
   * @return this.
   */
  @AvailableSince(NakshaVersion.v2_0_13)
  public @NotNull XyzNamespace removeTagsWithPrefixes(final @Nullable List<String> prefixes) {
    if (prefixes != null) {
      for (final @Nullable String prefix : prefixes) {
        if (prefix != null) removeTagsWithPrefix(prefix);
      }
    }
    return this;
  }

  /**
   * Tests whether this state refers to a deleted feature.
   *
   * @return {@code true} if the feature is in the deleted state; {@code false} otherwise.
   */
  @JsonIgnore
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_7)
  public boolean isDeleted() {
    return EXyzAction.DELETE.equals(getAction());
  }

  /**
   * Returns the change-version of this feature. The first (initial) state is always {@link EXyzAction#CREATE} and always has the version
   * {@code 1}.
   *
   * @return the change-version of this feature.
   */
  @JsonIgnore
  @AvailableSince(NakshaVersion.v2_0_7)
  public long getVersion() {
    return version;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setVersion(long version) {
    this.version = version;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withVersion(long version) {
    setVersion(version);
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable String getAuthor() {
    return author;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setAuthor(@Nullable String author) {
    this.author = author;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withAuthor(@Nullable String author) {
    setAuthor(author);
    return this;
  }

  /**
   * The epoch time in milliseconds when the feature was modified by the author.
   *
   * @return epoch time in milliseconds when the feature was modified by the author.
   */
  @AvailableSince(NakshaVersion.v2_0_8)
  @JsonInclude(Include.NON_DEFAULT)
  public long getAuthorTime() {
    return author_ts;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public void setAuthorTime(long author_ts) {
    this.author_ts = author_ts;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public @NotNull XyzNamespace withAuthor(long author_ts) {
    setAuthorTime(author_ts);
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(APP_ID)
  public @Nullable String getAppId() {
    return app_id;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(APP_ID)
  public void setAppId(@Nullable String app_id) {
    this.app_id = app_id;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull XyzNamespace withAppId(@Nullable String app_id) {
    setAppId(app_id);
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  @JsonProperty(STREAM_ID)
  public @Nullable String getStreamId() {
    return stream_id;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  @JsonProperty(STREAM_ID)
  public void setStreamId(@Nullable String streamId) {
    this.stream_id = streamId;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public @NotNull XyzNamespace withStreamId(@Nullable String streamId) {
    setStreamId(streamId);
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public long getExtend() {
    return extend;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public void setExtend(long extend) {
    this.extend = extend;
  }

  @AvailableSince(NakshaVersion.v2_0_8)
  public @NotNull XyzNamespace withExtend(long extend) {
    setExtend(extend);
    return this;
  }
}
