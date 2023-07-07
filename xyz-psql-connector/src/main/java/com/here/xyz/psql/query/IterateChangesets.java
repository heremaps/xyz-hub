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

package com.here.xyz.psql.query;

import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.helpers.versioning.GetMinAvailableVersion;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IterateChangesets extends XyzQueryRunner<IterateChangesetsEvent, XyzResponse> {

  private String pageToken;
  private long limit;
  private Long start;
  private boolean useCollection;
  private IterateChangesetsEvent event;

  public IterateChangesets(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    super(event);

    this.event = event;
    this.limit = event.getLimit();
    this.pageToken = event.getPageToken();
    this.start = event.getStartVersion();
    this.useCollection = event.isUseCollection();
  }

  @Override
  public XyzResponse run() throws SQLException, ErrorResponseException {
    long min = new GetMinAvailableVersion<>(event).run();

    if (start < min)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Min Version=" + min);

    return super.run();
  }

  @Override
  protected SQLQuery buildQuery(IterateChangesetsEvent event) throws SQLException, ErrorResponseException {
    return buildIterateChangesets(event);
  }

  @Override
  public XyzResponse handle(ResultSet rs) throws SQLException {
    if(useCollection)
      return handleChangesetCollection(rs);
    return handleChangeset(rs);
  }

  public SQLQuery buildIterateChangesets(IterateChangesetsEvent event){

    String geo = "replace(ST_AsGeojson(geo, " + GetFeaturesByBBox.GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0')::jsonb";

    SQLQuery query =new SQLQuery(
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

    query.setQueryFragment("start_version", event.getStartVersion() != null ?
            new SQLQuery("AND version >=  #{start} ")
                    .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
                    .withNamedParameter("start", event.getStartVersion()) : new SQLQuery(""));

    query.setQueryFragment("end_version", event.getEndVersion() != null ?
            new SQLQuery("AND version <= #{end}")
                    .withNamedParameter("end", event.getEndVersion()) : new SQLQuery(""));
    query.setQueryFragment("limit", event.getLimit() != 0 ? new SQLQuery("LIMIT #{limit}").withNamedParameter("limit", event.getLimit()) : new SQLQuery(""));

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
            .withVersion(lastVersion).withUpdated(new FeatureCollection().withFeatures(updates))
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
    }else{
      ccol.setStartVersion(-1);
      ccol.setEndVersion(-1);
    }

    ccol.setVersions(versions);

    if (numFeatures > 0 && numFeatures == limit) {
      ccol.setNextPageToken(pageToken);
    }
    return ccol;
  }
}
