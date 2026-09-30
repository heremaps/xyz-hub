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

package com.here.xyz.hub.config;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoTagConfigClient;
import com.here.xyz.hub.config.jdbc.JDBCTagConfigClient;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class TagConfigClient implements Initializable {
  private static final Logger logger = LogManager.getLogger();

  /*
   * Single instance, like the other config clients: this is reached per request for tag-addressed
   * refs, and each call used to build a new AmazonDynamoDBAsyncClient. The JDBC variant already was.
   */
  private static final class InstanceHolder {
    private static final TagConfigClient instance = Service.configuration.TAGS_DYNAMODB_TABLE_ARN != null
        ? new DynamoTagConfigClient(Service.configuration.TAGS_DYNAMODB_TABLE_ARN)
        : JDBCTagConfigClient.getInstance();
  }

  public static TagConfigClient getInstance() {
    return InstanceHolder.instance;
  }
  public abstract Future<Tag> getTag(Marker marker, String id, String spaceId);

  public abstract Future<List<Tag>> getTags(Marker marker, String id, List<String> spaceIds);

  public abstract Future<List<Tag>> getTagsByTagId(Marker marker, String tagId);

  public abstract Future<List<Tag>> getTags(Marker marker, String spaceId, boolean includeSystemTags);

  public abstract Future<List<Tag>> getTags(Marker marker, List<String> spaceIds);

  public abstract Future<List<Tag>> getAllTags(Marker marker);

  public abstract Future<Void> storeTag(Marker marker, Tag tag);

  public abstract Future<Tag> deleteTag(Marker marker, String id, String spaceId);

  public abstract Future<List<Tag>> deleteTagsForSpace(Marker marker, String spaceId);

  /**
   * Protects a version, which a version-bound extension or a branch reads, from being purged by a system tag, as all purges respect tags.
   */
  public Future<Void> tagVersion(Marker marker, String spaceId, Ref versionRef, String taggedBy) {
    return storeTag(marker, new Tag()
        .withId(versionTagId(versionRef, taggedBy))
        .withSpaceId(spaceId)
        .withVersionRef(versionRef)
        .withSystem(true)
        .withDescription("Keeps the version tagged by " + taggedBy)
        .withCreatedAt(Core.currentTimeMillis()));
  }

  public Future<Void> untagVersion(Marker marker, String spaceId, Ref versionRef, String taggedBy) {
    return deleteTag(marker, versionTagId(versionRef, taggedBy), spaceId).mapEmpty();
  }

  /**
   * @return The ID of the system tag which keeps the version: {@code <branchId>_<version>_<taggedBy>}, e.g. main_42_1f0c9a2b3d4e5f60.
   */
  public static String versionTagId(Ref versionRef, String taggedBy) {
    return versionRef.getBranch() + "_" + versionRef.getVersion() + "_" + Hasher.getHash(taggedBy).substring(0, 16);
  }
}

