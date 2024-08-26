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

package com.here.xyz.hub.config.jdbc;

import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.SCHEMA;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.TagConfigClient;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.Future;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class JDBCTagConfigClient extends TagConfigClient {
  private static final Logger logger = LogManager.getLogger();

  private static JDBCTagConfigClient instance;
  private static final String TAG_TABLE = "xyz_tags";
  private final JDBCConfigClient client = new JDBCConfigClient(SCHEMA, TAG_TABLE, Service.configuration);

  public static TagConfigClient getInstance() {
    if (instance == null)
      instance = new JDBCTagConfigClient();
    return instance;
  }

  @Override
  public Future<Tag> getTag(Marker marker, String id, String spaceId) {
    SQLQuery query = client.getQuery("SELECT id, space, version, system FROM ${schema}.${table} "
            + "WHERE id = #{tagId} AND space = #{spaceId}")
        .withNamedParameter("tagId", id)
        .withNamedParameter("spaceId", spaceId);

    return client.run(query, rs -> rs.next() ? resultSetToTag(rs) : null);
  }

  private static Tag resultSetToTag(ResultSet rs) throws SQLException {
    return new Tag()
        .withId(rs.getString("id"))
        .withSpaceId(rs.getString("space"))
        .withVersion(rs.getLong("version"))
        .withSystem(rs.getBoolean("system"));
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String tagId, List<String> spaceIds) {
    return _getTags(marker, new SQLQuery("WHERE id = #{tagId} AND space = ANY(#{spaceIds})")
        .withNamedParameter("tagId", tagId)
        .withNamedParameter("spaceIds", spaceIds.toArray(new String[0])));
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String spaceId, boolean includeSystemTags) {
    return _getTags(marker, new SQLQuery("WHERE space = #{spaceId} ${{includeSystemTags}}")
        .withNamedParameter("spaceId", spaceId)
        .withQueryFragment("includeSystemTags", includeSystemTags ? "" : "AND system = false"));
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, List<String> spaceIds) {
    return _getTags(marker, new SQLQuery("WHERE space = ANY(#{spaceIds})")
        .withNamedParameter("spaceIds", spaceIds.toArray(new String[0])));
  }

  @Override
  public Future<List<Tag>> getTagsByTagId(Marker marker, String tagId) {
    return _getTags(marker, new SQLQuery("WHERE id = #{tagId}").withNamedParameter("tagId", tagId));
  }

  @Override
  public Future<List<Tag>> getAllTags(Marker marker) {
    return _getTags(marker, new SQLQuery(""));
  }

  private Future<List<Tag>> _getTags(Marker marker, SQLQuery whereClause) {
    SQLQuery query = client.getQuery("SELECT id, space, version, system FROM ${schema}.${table} ${{whereClause}}")
        .withQueryFragment("whereClause", whereClause);

    return client.run(query, rs -> {
      List<Tag> tags = new ArrayList<>();
      while (rs.next())
        tags.add(resultSetToTag(rs));
      return tags;
    });
  }

  @Override
  public Future<Void> storeTag(Marker marker, Tag tag) {
    final SQLQuery query = client.getQuery("INSERT INTO ${schema}.${table} "
            + "(id, space, version, system) VALUES (#{tagId}, #{spaceId}, #{version}, #{system}) "
            + "ON CONFLICT (id,space) DO "
            + "UPDATE SET id = #{tagId}, space = #{spaceId}, version = #{version}, system = #{system}")
        .withNamedParameter("tagId", tag.getId())
        .withNamedParameter("spaceId", tag.getSpaceId())
        .withNamedParameter("version", tag.getVersion())
        .withNamedParameter("system", tag.isSystem());

    return client.write(query).mapEmpty();
  }

  @Override
  public Future<Tag> deleteTag(Marker marker, String id, String spaceId) {
    final SQLQuery query = client.getQuery("DELETE FROM ${schema}.${table} WHERE id = #{tagId} AND space = #{spaceId}")
        .withNamedParameter("tagId", id)
        .withNamedParameter("spaceId", spaceId);

    return getTag(marker, id, spaceId).compose(tag -> client.write(query).map(v -> tag));
  }

  @Override
  public Future<List<Tag>> deleteTagsForSpace(Marker marker, String spaceId) {
    final SQLQuery query = client.getQuery("DELETE FROM ${schema}.${table} WHERE space = #{spaceId}")
        .withNamedParameter("spaceId", spaceId);
    return getTags(marker, spaceId, true)
        .compose(tags -> client.write(query).map(tags));
  }

  @Override
  public Future<Void> init() {
    return client.init()
        .compose(v -> initTable());
  }

  private Future<Void> initTable() {
    return client.write(client.getQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} "
            + "(id TEXT, space TEXT, version BIGINT, system boolean, PRIMARY KEY(id, space))"))
        .onFailure(e -> logger.error("Can not create table {}!", TAG_TABLE, e))
        .mapEmpty();
  }
}
