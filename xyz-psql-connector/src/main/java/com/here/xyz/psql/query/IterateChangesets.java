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
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IterateChangesets extends SearchForFeatures<IterateChangesetsEvent, XyzResponse> {

  private String pageToken;
  private long limit;
  private Long start;
  private Long end;

  public IterateChangesets(IterateChangesetsEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);

    limit = event.getLimit();
    pageToken = event.getPageToken();
    start = event.getStartVersion();
    end = event.getEndVersion();
  }

  @Override
  protected SQLQuery buildQuery(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    return buildIterateChangesets(event);
  }

  @Override
  public XyzResponse handle(ResultSet rs) throws SQLException {
    if(end == null)
      return handleChangeset(rs);
    return handleChangesetCollection(rs);
  }

  public SQLQuery buildIterateChangesets(IterateChangesetsEvent event){
    SQLQuery query =new SQLQuery(
        "SELECT " +
                " version||'_'||id as vid,"+
                " id,"+
                " version,"+
                " author,"+
                " operation,"+
                " (jsondata#-'{properties,@ns:com:here:xyz,version}') || jsonb_strip_nulls(jsonb_build_object('geometry',ST_AsGeoJSON(geo)::jsonb)) As feature "+
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

  public Changeset handleChangeset(ResultSet rs) throws SQLException {
    String author = null;
    long createdAt = 0;
    long numFeatures = 0;
    Changeset cc = new Changeset();

    List<Feature> inserts = new ArrayList<>();
    List<Feature> updates = new ArrayList<>();
    List<Feature> deletes = new ArrayList<>();

    while (rs.next()) {
      Feature feature;
      String operation = rs.getString("Operation");
      if(author == null)
        author = rs.getString("author");

      try {
        feature =  new ObjectMapper().readValue(rs.getString("Feature"), Feature.class);
        if(createdAt == 0)
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
      numFeatures++;
    }

    cc.setVersion(numFeatures > 0 ? start : -1l);
    cc.setCreatedAt(createdAt);
    cc.setAuthor(author);
    cc.setInserted(new FeatureCollection().withFeatures(inserts));
    cc.setUpdated(new FeatureCollection().withFeatures(updates));
    cc.setDeleted(new FeatureCollection().withFeatures(deletes));

    if (numFeatures > 0 && numFeatures == limit) {
      cc.setNextPageToken(pageToken);
    }

    return cc;
  }

  public ChangesetCollection handleChangesetCollection(ResultSet rs) throws SQLException {
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
}
