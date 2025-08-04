/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.psql.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.psql.query.helpers.versioning.GetMinVersion;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class IterateChangesets extends IterateFeatures<IterateChangesetsEvent, ChangesetCollection> {
  public static long DEFAULT_LIMIT = 1_000l;
  private long limit;
  private long startVersion = -1;
  private long minVersion;
  private IterateChangesetsEvent event; //TODO: Do not store the whole event during the request phase

  public IterateChangesets(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    super(event);
    this.event = event;
    limit = event.getLimit() <= 0 ? DEFAULT_LIMIT : event.getLimit();
    minVersion = event.getMinVersion();
  }

  @Override
  public ChangesetCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    //TODO: Only fetch the minDbVersion if requested startVersion < requested minVersion (otherwise the extra query would be useless)
    Long minDbVersion = new GetMinVersion<>(event).withDataSourceProvider(dataSourceProvider).run();
    minVersion = Math.min(minDbVersion, minVersion);
    startVersion = Math.max(event.getStartVersion(), minVersion);
    //TODO: Use range-ref instead of custom filter query in future
    //event.setRef(new Ref(Math.max(0, startVersion - 1) + ".." + (event.getEndVersion() == -1 ? HEAD : event.getEndVersion())));
    return super.run(dataSourceProvider);
  }

  //TODO: Use range-ref instead of custom filter query in future
  @Override
  protected SQLQuery buildFilterWhereClause(IterateChangesetsEvent event) {
    return new SQLQuery("${{startVersion}} ${{endVersion}}")
        .withQueryFragment("startVersion", new SQLQuery("version >= #{start}")
            .withNamedParameter("start", startVersion))
        .withQueryFragment("endVersion", event.getEndVersion() != -1
            ? new SQLQuery("AND version <= #{end}")
                .withNamedParameter("end", event.getEndVersion())
            : new SQLQuery(""));
  }

  @Override
  protected SQLQuery buildSelectClause(IterateChangesetsEvent event, int dataset) {
    return new SQLQuery("${{selectClauseWithoutExtraFields}}, operation, author")
        .withQueryFragment("selectClauseWithoutExtraFields", super.buildSelectClause(event, dataset));
  }

  @Override
  protected SQLQuery buildLimitFragment(IterateChangesetsEvent event) {
    //Query one more feature as requested, to be able to determine if we need to include a nextPageToken
    return new SQLQuery("LIMIT #{limit}").withNamedParameter("limit", event.getLimit() + 1);
  }

  public ChangesetCollection handle(ResultSet rs) throws SQLException {
    Map<Long, Changeset> versions = new HashMap<>();

    long numFeatures = 0;
    long startVersion = -1;
    long prevVersion = -1;
    boolean writeStarted = false;
    String author = null;
    long createdAt = -1;

    LazyParsableFeatureCollection inserts = new LazyParsableFeatureCollection();
    LazyParsableFeatureCollection updates = new LazyParsableFeatureCollection();
    LazyParsableFeatureCollection deletes = new LazyParsableFeatureCollection();

    while (rs.next()) {
      numFeatures++;
      //skip the additional added feature
      if (!rs.isFirst() && rs.isLast() && numFeatures > limit)
        break;

      String operation = rs.getString("operation");
      long version = rs.getLong("version");

      if (!writeStarted) {
        startVersion = version;
        writeStarted = true;
      }

      if (prevVersion != -1 && version > prevVersion) {
        versions.put(prevVersion, new Changeset()
            .withVersion(prevVersion)
            .withCreatedAt(createdAt)
            .withAuthor(author)
            .withInserted(inserts.build())
            .withUpdated(updates.build())
            .withDeleted(deletes.build()));

        author = null;
        createdAt = -1;
        inserts = new LazyParsableFeatureCollection();
        updates = new LazyParsableFeatureCollection();
        deletes = new LazyParsableFeatureCollection();
      }

      if (author == null) {
        author = rs.getString("author");
        try {
          Feature firstFeature = XyzSerializable.deserialize(rs.getString("jsondata"));
          createdAt = firstFeature.getProperties().getXyzNamespace().getUpdatedAt();
        }
        catch (JsonProcessingException e) {
          throw new SQLException("Can't read feature json from database!", e);
        }
      }

      try {
        switch (operation) {
          case "I":
          case "H":
            inserts.addFeature(content -> handleFeature(rs, content));
            break;
          case "U":
          case "J":
            updates.addFeature(content -> handleFeature(rs, content));
            break;
          case "D":
            deletes.addFeature(content -> handleFeature(rs, content));
            break;
        }
      }
      catch (ErrorResponseException e) {
        //TODO: Throw ErrorResponseException instead
        throw new SQLException(e.getMessage());
      }

      prevVersion = version;
    }

    if (writeStarted) {
      versions.put(prevVersion, new Changeset()
          .withVersion(prevVersion)
          .withCreatedAt(createdAt)
          .withAuthor(author)
          .withInserted(inserts.build())
          .withUpdated(updates.build())
          .withDeleted(deletes.build()));
    }

    //Only create a nextPageToken if there are further results
    String nextPageToken = null;
    if (numFeatures > 0 && numFeatures == limit + 1 && numFeatures > limit)
      nextPageToken = createNextPageToken();

    return new ChangesetCollection()
        .withStartVersion(startVersion)
        .withEndVersion(prevVersion)
        .withVersions(versions)
        .withNextPageToken(nextPageToken);
  }
}
