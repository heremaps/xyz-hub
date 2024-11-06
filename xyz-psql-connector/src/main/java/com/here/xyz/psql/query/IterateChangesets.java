/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.helpers.versioning.GetMinAvailableVersion;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IterateChangesets extends XyzQueryRunner<IterateChangesetsEvent, ChangesetCollection> {
  public static long DEFAULT_LIMIT = 10_000L;
  private String pageToken;
  private long limit;
  private long startVersion = -1;
  private IterateChangesetsEvent event; //TODO: Do not store the whole event during the request phase

  public IterateChangesets(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    super(event);
    this.event = event;
    limit = event.getLimit() <= 0 ? DEFAULT_LIMIT : event.getLimit();
    pageToken = event.getPageToken();
  }

  @Override
  public ChangesetCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    startVersion = Math.max(event.getStartVersion(), new GetMinAvailableVersion<>(event).withDataSourceProvider(dataSourceProvider).run());
    return super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildQuery(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    return buildIterateChangesets(event);
  }

  @Override
  public ChangesetCollection handle(ResultSet rs) throws SQLException {
    long numFeatures = 0;

    ChangesetCollection ccol = new ChangesetCollection();
    Map<Long, Changeset> versions = new HashMap<>();
    Long lastVersion = null;
    Long startVersion = null;
    boolean wroteStart = false;
    String author = null;
    long createdAt = 0;

    List<Feature> inserts = new ArrayList<>();
    List<Feature> updates = new ArrayList<>();
    List<Feature> deletes = new ArrayList<>();

    while (rs.next()) {
      numFeatures++;
      //skip the additional added feature
      if(!rs.isFirst() && rs.isLast() && numFeatures > limit)
        break;

      Feature feature;
      String operation = rs.getString("operation");
      Long version = rs.getLong("version");

      if(!wroteStart){
        startVersion = version;
        wroteStart = true;
      }

      if(lastVersion !=  null && version > lastVersion) {
        Changeset cs = new Changeset().withInserted(new FeatureCollection().withFeatures(inserts))
            .withVersion(lastVersion).withUpdated(new FeatureCollection().withFeatures(updates))
            .withDeleted(new FeatureCollection().withFeatures(deletes))
            .withCreatedAt(createdAt)
            .withAuthor(author);

        versions.put(lastVersion, cs);
        inserts = new ArrayList<>();
        updates = new ArrayList<>();
        deletes = new ArrayList<>();
      }

      author = rs.getString("author");

      try {
        feature =  new ObjectMapper().readValue(rs.getString("feature"), Feature.class);
        createdAt = feature.getProperties().getXyzNamespace().getUpdatedAt();
      }catch (JsonProcessingException e){
        throw new SQLException("Cant read json from database!");
      }

      switch (operation){
        case "I":
        case "H":
          inserts.add(feature);
          break;
        case "U":
        case "J":
          updates.add(feature);
          break;
        case "D":
          deletes.add(feature);
          break;
      }

      pageToken = rs.getString("vid");
      lastVersion = version;
    }

    if(wroteStart){
      Changeset cs = new Changeset().withInserted(new FeatureCollection().withFeatures(inserts))
          .withVersion(lastVersion)
          .withUpdated(new FeatureCollection().withFeatures(updates))
          .withDeleted(new FeatureCollection().withFeatures(deletes))
          .withCreatedAt(createdAt)
          .withAuthor(author);

      versions.put(lastVersion, cs);
      ccol.setStartVersion(startVersion);
      ccol.setEndVersion(lastVersion);
    }else{
      ccol.setStartVersion(-1);
      ccol.setEndVersion(-1);
    }

    ccol.setVersions(versions);

    //Only add pageToke if we have further results
    if (numFeatures > 0 && numFeatures == limit + 1 && numFeatures > limit)
      ccol.setNextPageToken(pageToken);

    return ccol;
  }

  public SQLQuery buildIterateChangesets(IterateChangesetsEvent event){
    //TODO: Re-use geo fragment from GetFeatures QR instead of duplicating it here
    String geo = "REGEXP_REPLACE(ST_AsGeojson(geo, " + GetFeatures.GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0', 'gi')::jsonb";

    SQLQuery query = new SQLQuery(
        "SELECT " +
                " version||'_'||id as vid,"+
                " id,"+
                " version,"+
                " author,"+
                " operation,"+
                " jsonb_set(jsondata,'{properties, @ns:com:here:xyz, version}',to_jsonb(version)) || jsonb_strip_nulls(jsonb_build_object('geometry',"+ geo +")) As feature "+
                "   from  ${schema}.${table} "+
                " WHERE 1=1"+
                "      ${{page}}"+
                "      ${{start_version}}"+
                "      ${{end_version}}"+
                " order by version ASC,id "+
                " ${{limit}}");

    query.setVariable(SCHEMA, getSchema());
    query.setVariable(TABLE, getDefaultTable(event));
    query.setQueryFragment("page", event.getPageToken() != null ?
            new SQLQuery("AND version||'_'||id > #{page_token} ")
                    .withNamedParameter("page_token", event.getPageToken()) : new SQLQuery(""));

    query.setQueryFragment("start_version", new SQLQuery("AND version >=  #{start} ")
                    .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
                    .withNamedParameter("start", startVersion));

    query.setQueryFragment("end_version", event.getEndVersion() != -1 ? new SQLQuery("AND version <= #{end}")
                    .withNamedParameter("end", event.getEndVersion()) : new SQLQuery(""));

    //Query one more feature as requested, to be able to determine if we need to include a nextPageToken
    query.setQueryFragment("limit", new SQLQuery("LIMIT #{limit}").withNamedParameter("limit", limit + 1));

    return query;
  }

}
