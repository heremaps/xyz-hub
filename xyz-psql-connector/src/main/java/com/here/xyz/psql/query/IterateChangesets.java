/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.*;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IterateChangesets extends SearchForFeatures<IterateChangesetsEvent, ChangesetCollection> {

  private String pageToken;
  private long limit;
  private long start;
  private long end;

  private boolean isCompact;

  private IterateFeaturesEvent tmpEvent; //TODO: Remove after refactoring

  public IterateChangesets(IterateChangesetsEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);

    limit = event.getLimit();
    pageToken = event.getPageToken();
    start = event.getStartVersion();
    end = event.getEndVersion();
    isCompact = event.isCompact();
  }

  @Override
  protected SQLQuery buildQuery(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    return build(event);
  }

  @Override
  public ChangesetCollection handle(ResultSet rs) throws SQLException {
    long numFeatures = 0;

    ChangesetCollection ccol = new ChangesetCollection();
    Map<Integer, Changeset> versions = new HashMap<>();
    Integer lastVersion = null;
    Integer startVersion = null;
    boolean wroteStart = false;

    List<Feature> inserts = new ArrayList<>();
    List<Feature> updates = new ArrayList<>();
    List<Feature> deletes = new ArrayList<>();

    while (rs.next()) {
      Feature feature;
      String operation = rs.getString("operation");
      Integer version = rs.getInt("version");

      if(!wroteStart){
        startVersion = version;
        wroteStart = true;
      }

      if(lastVersion !=  null && version > lastVersion) {
        Changeset cs = new Changeset().withInserted(new FeatureCollection().withFeatures(inserts))
                .withUpdated(new FeatureCollection().withFeatures(updates))
                .withDeleted(new FeatureCollection().withFeatures(deletes));
        versions.put(lastVersion, cs);
        inserts = new ArrayList<>();
        updates = new ArrayList<>();
        deletes = new ArrayList<>();
      }

      try {
        feature =  new ObjectMapper().readValue(rs.getString("feature"), Feature.class);
      }catch (JsonProcessingException e){
        throw new SQLException("Cant read json from database!");
      }

      switch (operation){
        case "I":
          inserts.add(feature);
          break;
        case "U":
          updates.add(feature);
          break;
        case "D":
          deletes.add(feature);
          break;
      }

      pageToken = rs.getString("vid");
      lastVersion = version;
      numFeatures++;
    }

    if(wroteStart){
      Changeset cs = new Changeset().withInserted(new FeatureCollection().withFeatures(inserts))
              .withUpdated(new FeatureCollection().withFeatures(updates))
              .withDeleted(new FeatureCollection().withFeatures(deletes));
      versions.put(lastVersion, cs);
      ccol.setStartVersion(startVersion);
      ccol.setEndVersion(lastVersion);
    }

    ccol.setVersions(versions);

    if (numFeatures > 0 && numFeatures == limit) {
      ccol.setNextPageToken(pageToken);
    }

    return ccol;
  }

  public SQLQuery build(IterateChangesetsEvent event){
    SQLQuery query =new SQLQuery(
        "SELECT " +
                " version||'_'||id as vid,"+
                " id,"+
                " version,"+
                " next_version,"+
                " operation,"+
                " jsondata || jsonb_strip_nulls(jsonb_build_object('geometry',ST_AsGeoJSON(geo)::jsonb)) As feature "+
                "   from  ${schema}.${table} "+
                " WHERE 1=1"+
                "      ${{page}}"+
                "      ${{start_version}}"+
                "      ${{end_version}}"+
                " order by version ASC,id "+
                " ${{limit}}");

    query.setVariable(SCHEMA, getSchema());
    query.setVariable(TABLE, getDefaultTable(event));
    query.setNamedParameter("start",event.getStartVersion());
    query.setNamedParameter("end",event.getEndVersion());
    query.setQueryFragment("page", event.getPageToken() != null ?
            new SQLQuery("AND version||'_'||id > #{page_token} ")
                    .withNamedParameter("page_token", event.getPageToken()) : new SQLQuery(""));
    query.setQueryFragment("start_version", event.getStartVersion() != null ?
            new SQLQuery("AND version >= #{start}")
                    .withNamedParameter("start", event.getStartVersion()) : new SQLQuery(""));
    query.setQueryFragment("end_version", event.getEndVersion() != null ?
            new SQLQuery("AND version <= #{end}")
                    .withNamedParameter("end", event.getEndVersion()) : new SQLQuery(""));
    query.setQueryFragment("limit", event.getLimit() != 0 ? buildLimitFragment(event.getLimit()) : new SQLQuery(""));

    return query;
  }
}
