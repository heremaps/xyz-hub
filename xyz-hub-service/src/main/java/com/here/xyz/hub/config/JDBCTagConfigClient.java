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

package com.here.xyz.hub.config;

import static com.here.xyz.hub.config.JDBCConfig.TAG_TABLE;

import com.here.xyz.models.hub.Tag;
import com.here.xyz.psql.SQLQuery;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class JDBCTagConfigClient extends TagConfigClient {
  private static final Logger logger = LogManager.getLogger();

  private static JDBCTagConfigClient instance;
  SQLClient client;

  private JDBCTagConfigClient() {
    this.client = JDBCConfig.getClient();
  }

  public static TagConfigClient getInstance() {
    if (instance == null) {
      instance = new JDBCTagConfigClient();
    }
    return instance;
  }

  @Override
  public Future<Tag> getTag(Marker marker, String id, String spaceId) {
    Promise<Tag> p = Promise.promise();
    SQLQuery query = new SQLQuery("SELECT id, space, version FROM " + TAG_TABLE + " WHERE id = ? AND space = ?", id, spaceId);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        Optional<Tag> tag = out.result().getRows().stream().map(r->new Tag()
                .withId(r.getString("id"))
                .withSpaceId(r.getString("space"))
                .withVersion(r.getLong("version"))
        ).findFirst();
        if (tag.isPresent()) {
          p.complete(tag.get());
        }
        else
          p.complete();
      }
      else
        p.fail(out.cause());
    });
    return p.future();
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String id, List<String> spaceIds) {
    List<String> params = spaceIds.stream().map(e->"?").collect(Collectors.toList());
    SQLQuery query = new SQLQuery("WHERE id=?", id);
    query.append(new SQLQuery("AND space IN (" + StringUtils.join(params, ",")+")", spaceIds.toArray()));
    return _getTags(marker, query);
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, String spaceId) {
    return _getTags(marker, new SQLQuery("WHERE space=?", spaceId));
  }

  @Override
  public Future<List<Tag>> getTags(Marker marker, List<String> spaceIds) {
    List<String> params = spaceIds.stream().map(e->"?").collect(Collectors.toList());
    SQLQuery query = new SQLQuery("WHERE space IN (" + StringUtils.join(params, ",")+")", spaceIds.toArray());
    return _getTags(marker, query);
  }

  @Override
  public Future<List<Tag>> getAllTags(Marker marker) {
    return _getTags(marker, null);
  }

  private Future<List<Tag>> _getTags(Marker marker, SQLQuery whereClause) {
    Promise<List<Tag>> p = Promise.promise();
    SQLQuery query = new SQLQuery("SELECT id, space, version FROM " + TAG_TABLE);
    if(whereClause != null )
      query.append(whereClause);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        List<Tag> tag = out.result().getRows().stream().map(r->new Tag()
            .withId(r.getString("id"))
            .withSpaceId(r.getString("space"))
            .withVersion(r.getLong("version"))
        ).collect(Collectors.toList());
        p.complete(tag);
      }
      else
        p.fail(out.cause());
    });
    return p.future();
  }

  @Override
  public Future<Void> storeTag(Marker marker, Tag tag) {
    Promise<Void> p = Promise.promise();

    final SQLQuery query = new SQLQuery("INSERT INTO " + TAG_TABLE + " (id, space, version) VALUES (?, ?, ?) " +
        "ON CONFLICT (id,space) DO " +
        "UPDATE SET id = ?, space = ?, version = ?",
        tag.getId(), tag.getSpaceId(), tag.getVersion(),
        tag.getId(), tag.getSpaceId(), tag.getVersion());

    client.updateWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        p.complete();
      } else {
        p.fail(out.cause());
      }
    });
    return p.future();
  }

  @Override
  public Future<Tag> deleteTag(Marker marker, String id, String spaceId) {
    final SQLQuery query = new SQLQuery("DELETE FROM " + TAG_TABLE + " WHERE id = ? AND space = ?", id, spaceId);
    return getTag(marker, id, spaceId).compose(tag -> JDBCConfig.updateWithParams(query).map(tag));
  }

  @Override
  public Future<List<Tag>> deleteTagsForSpace(Marker marker, String spaceId) {
    final SQLQuery query = new SQLQuery("DELETE FROM " + TAG_TABLE + " WHERE space = ?", spaceId);
    return getTags(marker, spaceId)
        .compose(tags -> JDBCConfig.updateWithParams(query)
        .map(tags));
  }
}
