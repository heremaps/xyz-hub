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
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.helpers.versioning.GetHeadVersion;
import com.here.xyz.psql.query.helpers.versioning.GetMinVersion;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IterateChangesets extends IterateFeatures<IterateChangesetsEvent, ChangesetCollection> {
  public static long DEFAULT_LIMIT = 1_000l;
  private long limit;
  private IterateChangesetsEvent event; //TODO: Do not store the whole event during the request phase
  private long nextTokenVersion;
  private String nextTokenId;

  public IterateChangesets(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    super(event);
    this.event = event;
    limit = event.getLimit() <= 0 ? DEFAULT_LIMIT : event.getLimit();
  }

  @Override
  protected String buildOuterOrderByFragment(ContextAwareEvent event) {
    return this.buildOrderByFragment(event);
  }

  @Override
  protected String buildOrderByFragment(ContextAwareEvent event) {
    return "ORDER BY version, id";
  }

  @Override
  protected SQLQuery buildOffsetFilterFragment(IterateFeaturesEvent event, int dataset) {
    if (event.getNextPageToken() == null)
      return new SQLQuery("");

    TokenContent token = readTokenContent(event.getNextPageToken());
    return new SQLQuery("""
        AND (
          version >= #{tokenStartVersion} AND id > #{tokenStartId}
          OR
          version > #{tokenStartVersion}
        )
        """)
        .withNamedParameter("tokenStartVersion", token.startVersion)
        .withNamedParameter("tokenStartId", token.startId);
  }

  private TokenContent readTokenContent(String token) {
    if (token == null)
      return null;
    token = decodeToken(token);
    String[] tokenParts = token.split("_");
    return new TokenContent(Long.parseLong(tokenParts[0]), tokenParts[1]);
  }

  private record TokenContent(long startVersion, String startId) {
    public String toString() {
      return startVersion + "_" + startId;
    }
  }

  @Override
  protected String createNextPageToken() {
    return encodeToken(new TokenContent(nextTokenVersion, nextTokenId).toString());
  }

  @Override
  public ChangesetCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    long headVersion = new GetHeadVersion<>(event).withDataSourceProvider(dataSourceProvider).run();
    long minAvailableVersion = headVersion - event.getVersionsToKeep() + 1;

    long minVersion = event.getMinVersion();
    long startVersion = event.getRef().getStart().getVersion();

    if (minAvailableVersion > minVersion || minAvailableVersion > startVersion) {
      if (startVersion < minVersion) {
        Long minDbVersion = new GetMinVersion<>(event).withDataSourceProvider(dataSourceProvider).run();
        minVersion = Math.min(minDbVersion, minVersion);
      }

      startVersion = Math.max(startVersion, minVersion);
    }

    //Use the updated ref
    event.setRef(new Ref(new Ref(startVersion), event.getRef().getEnd()));
    return super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildNextVersionFragment(Ref ref, boolean historyEnabled, String versionParamName, long baseVersion) {
    //TODO: Check if this check could be pulled up, because when requesting history versions from a range, the next-version anyways should not play any role
    if (ref.isRange())
      return new SQLQuery("");
    return super.buildNextVersionFragment(ref, historyEnabled, versionParamName, baseVersion);
  }

  //Enhances select clause by adding operation & author
  @Override
  protected SQLQuery buildSelectClause(IterateChangesetsEvent event, int dataset, long baseVersion) {
    return new SQLQuery("${{selectClauseWithoutExtraFields}}, operation, author")
        .withQueryFragment("selectClauseWithoutExtraFields", super.buildSelectClause(event, dataset, baseVersion));
  }

  protected SQLQuery buildFilterWhereClause(IterateChangesetsEvent event) {

    List<String> authors = event.getAuthors();

    long startTime = event.getStartTime(),
         endTime = event.getEndTime();

    String authSql      = "TRUE",
           startTimeSql = "TRUE",
           endTimeSql   = "TRUE",
           timeSql =
            """
             version %1$s
	           ( with ttable as ( select distinct(version) version, (jsondata#>>'{properties,@ns:com:here:xyz,updatedAt}')::bigint as ts from ${schema}.${table} )
               select coalesce( %3$s(version), %4$s ) from ttable where ts %2$s %5$d
	           )
            """;

    if( authors != null && !authors.isEmpty() )
     authSql = String.format("author in (%s)", authors.stream().map(author -> "'" + author + "'").collect(Collectors.joining(",")));

    if( startTime > 0 )
     startTimeSql = String.format(timeSql,">=",">","min","max_bigint()", startTime );

    if( endTime > 0 )
     endTimeSql = String.format(timeSql,"<=","<=","max","0", endTime );

    return
      new SQLQuery("${{superFilterWhereClause}} AND ${{authorFilterClause}} AND ${{startTimeFilterClause}} AND ${{endTimeFilterClause}}")
           .withQueryFragment("superFilterWhereClause", super.buildFilterWhereClause(event))
           .withQueryFragment("authorFilterClause", new SQLQuery(authSql))
           .withQueryFragment("startTimeFilterClause", new SQLQuery(startTimeSql))
           .withQueryFragment("endTimeFilterClause", new SQLQuery(endTimeSql));

  }

  //Enhances the limit by adding one extra feature that is loaded just for the sake of finding out whether there is another page (extra feature is not part of the response)
  //TODO: Check if that mechanism for preventing the last empty page (edge case) can be prevented also for IterateFeatures by pulling this up
  @Override
  protected SQLQuery buildLimitFragment(IterateChangesetsEvent event) {
    //Query one more feature as requested, to be able to determine if we need to include a nextPageToken
    return new SQLQuery("LIMIT #{limit}").withNamedParameter("limit", event.getLimit() + 1);
  }

  //Ensures that *all* versions of a feature are returned, also the ones from base branches (even if normally these would be masked)
  @Override
  protected SQLQuery buildBranchCompositionFilter() {
    return new SQLQuery("");
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

  @Override
  protected void handleFeature(ResultSet rs, StringBuilder result) throws SQLException {
    super.handleFeature(rs, result);
    nextTokenVersion = rs.getLong("version");
    nextTokenId = rs.getString("id");
  }
}
