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

import static com.here.xyz.hub.config.JDBCConfig.READER_TABLE;
import static com.here.xyz.hub.config.JDBCConfig.SPACE_TABLE;

import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Reader;
import com.here.xyz.psql.SQLQuery;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.sql.SQLClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class JDBCReaderConfigClient extends ReaderConfigClient{
  private static final Logger logger = LogManager.getLogger();

  private static JDBCReaderConfigClient instance;
  SQLClient client;

  private JDBCReaderConfigClient() {
    this.client = JDBCConfig.getClient();
  }

  public static ReaderConfigClient getInstance() {
    if (instance == null) {
      instance = new JDBCReaderConfigClient();
    }
    return instance;
  }

  @Override
  public Future<Reader> getReader(Marker marker, String id, String spaceId) {
    Promise<Reader> p = Promise.promise();
    SQLQuery query = new SQLQuery("SELECT id, space, version FROM " + READER_TABLE + " WHERE id = ? AND space = ?", id, spaceId);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        Optional<Reader> reader = out.result().getRows().stream().map(r->new Reader()
                .withId(r.getString("id"))
                .withSpaceId(r.getString("space"))
                .withVersion(r.getLong("version"))
        ).findFirst();
        if (reader.isPresent()) {
          p.complete(reader.get());
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
  protected Future<List<Reader>> getReaders(Marker marker, List<String> spaceIds) {
    return null;
  }

  @Override
  public Future<Void> storeReader(Marker marker, Reader reader) {
    return null;
  }

  @Override
  public Future<Void> increaseVersion(Marker marker, String spaceId, String readerId) {
    return null;
  }

  @Override
  public Future<Reader> deleteReader(Marker marker, String spaceId, String reader) {
    return null;
  }

  @Override
  protected Future<Reader> deleteReaders(Marker marker, String spaceId) {
    return null;
  }

  @Override
  protected Future<List<Reader>> getAllReaders(Marker marker) {
    return null;
  }
}
