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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT_HIDE_COMPOSITE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE_HIDE_COMPOSITE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.DatabaseWriter.ModificationType;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class GetFeatures<E extends ContextAwareEvent, R extends XyzResponse> extends ExtendedSpace<E, R> {

  public static final long GEOMETRY_DECIMAL_DIGITS = 8;
  public static long MAX_BIGINT = Long.MAX_VALUE;

  private boolean withoutIdField = false;

  public GetFeatures(E event) throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);
    boolean isExtended = ( isExtendedSpace(event) && event.getContext() == DEFAULT ) && (viewMode != ViewModus.DELTA ); /** deltaOnly -> ignore underlying base, act as not extended  */
    SQLQuery query;

    if ( isExtended ) {
      String UNION_ALL = ( viewMode == ViewModus.BASE_DELTA ? "UNION ALL" : "UNION DISTINCT" ),
             NOT       = ( viewMode == ViewModus.BASE_DELTA ? "NOT" : "" );

      query = new SQLQuery(
          "SELECT * FROM ("
          + "    (SELECT ${{selection}}, ${{geo}}${{iColumnExtension}}${{id}}"
          + "        FROM ${schema}.${table}"
          + "        WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{authorCheck}} ${{iOffsetExtension}} ${{limit}})"
          + "  " + UNION_ALL
          + "        SELECT ${{selection}}, ${{geo}}${{iColumn}}${{id}} FROM"
          + "            ("
          + "                ${{baseQuery}}"
          + "            ) a WHERE "+ NOT +" exists(SELECT 1 FROM ${schema}.${table} b WHERE ${{notExistsIdComparison}})"
          + ") limitQuery ${{limit}}")
          .withQueryFragment("notExistsIdComparison", buildIdComparisonFragment(event, "a."));
    }
    else {
      query = new SQLQuery(
          "SELECT ${{selection}}, ${{geo}}${{iColumn}}${{id}}"
              + "    FROM ${schema}.${table} ${{tableSample}}"
              + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{authorCheck}} ${{orderBy}} ${{limit}} ${{offset}}");
    }

    query.setQueryFragment("deletedCheck", buildDeletionCheckFragment(versionsToKeep, isExtended));
    query.withQueryFragment("versionCheck", buildVersionCheckFragment(event));
    query.withQueryFragment("authorCheck", buildAuthorCheckFragment(event));
    query.setQueryFragment("selection", buildSelectionFragment(event));
    query.setQueryFragment("geo", buildGeoFragment(event));

    query.setQueryFragment("iColumn", ""); //NOTE: This can be overridden by implementing subclasses
    query.setQueryFragment("tableSample", ""); //NOTE: This can be overridden by implementing subclasses
    query.setQueryFragment("limit", ""); //NOTE: This can be overridden by implementing subclasses
    query.setQueryFragment("id", withoutIdField ? "" : ", id");

    query.setVariable(SCHEMA, getSchema());
    query.setVariable(TABLE, getDefaultTable(event));

    if (isExtended) {
      query.setQueryFragment("iColumnExtension", ""); //NOTE: This can be overridden by implementing subclasses
      query.setQueryFragment("iOffsetExtension", "");

      SQLQuery baseQuery = !is2LevelExtendedSpace(event)
          ? build1LevelBaseQuery(event) //1-level extension
          : build2LevelBaseQuery(event); //2-level extension

      query.setQueryFragment("baseQuery", baseQuery);

      query.setQueryFragment("iColumnBase", ""); //NOTE: This can be overridden by implementing subclasses
      query.setQueryFragment("iOffsetBase", ""); //NOTE: This can be overridden by implementing subclasses
      query.setQueryFragment("iColumnIntermediate", ""); //NOTE: This can be overridden by implementing subclasses
      query.setQueryFragment("iOffsetIntermediate", ""); //NOTE: This can be overridden by implementing subclasses
    }
    else {
      query.setQueryFragment("orderBy", buildOrderByFragment(event)); //NOTE: This can be overridden by implementing subclasses
      query.setQueryFragment("offset", ""); //NOTE: This can be overridden by implementing subclasses
    }

    return query;
  }

  private SQLQuery buildVersionCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent)) return new SQLQuery("");

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);
    boolean versionIsStar = "*".equals(selectiveEvent.getRef());
    boolean versionIsNotPresent = selectiveEvent.getRef() == null;

    final SQLQuery defaultClause = new SQLQuery(" ${{minVersion}} ${{nextVersion}} ")
        .withQueryFragment("nextVersion", versionIsStar || versionsToKeep <= 1 ? "" : " AND next_version = #{MAX_BIGINT} ")
        .withNamedParameter("MAX_BIGINT", MAX_BIGINT);
    defaultClause.withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent));

    if (versionsToKeep == 1 || versionIsNotPresent || versionIsStar) return defaultClause;

    // versionsToKeep > 1 AND contains a reference to a version or version is a valid version
    return new SQLQuery(" AND version <= #{version} AND next_version > #{version} ${{minVersion}} ")
        .withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent))
        .withNamedParameter("version", getVersionFromRef(selectiveEvent));
  }

  private SQLQuery buildBaseVersionCheckFragment() {
    //Always assume HEAD version for base spaces
    return new SQLQuery(" AND next_version = #{MAX_BIGINT}").withNamedParameter("MAX_BIGINT", MAX_BIGINT);
  }

  private SQLQuery buildMinVersionFragment(SelectiveEvent event) {
    long version = getVersionFromRef(event);
    boolean isHead = version == MAX_BIGINT;
    if (event.getVersionsToKeep() > 1)
      return new SQLQuery("AND greatest(#{minVersion}, (select max(version) - #{versionsToKeep} from ${schema}.${table})) <= #{version}")
          .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
          .withNamedParameter("minVersion", event.getMinVersion())
          .withNamedParameter("version", version);
    return isHead ? new SQLQuery("") : new SQLQuery("AND #{version} = (SELECT max(version) as HEAD FROM ${schema}.${table})")
        .withNamedParameter("version", version);
  }

  private SQLQuery buildAuthorCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent)) return new SQLQuery("");

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    long v2k = DatabaseHandler.readVersionsToKeep(event);
    boolean emptyAuthor = selectiveEvent.getAuthor() == null;

    if (v2k < 1 || emptyAuthor) return new SQLQuery("");

    return new SQLQuery(" AND author = #{author}")
        .withNamedParameter("author", selectiveEvent.getAuthor());
  }

  private long getVersionFromRef(SelectiveEvent event) {
    try {
      return Long.parseLong(event.getRef());
    }
    catch (NumberFormatException e) {
      return Long.MAX_VALUE;
    }
  }

  private SQLQuery build1LevelBaseQuery(E event) {
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);

    return new SQLQuery("SELECT id, version, operation, jsondata, geo${{iColumnBase}}"
        + "    FROM ${schema}.${extendedTable} m"
        + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{iOffsetBase}} ${{limit}}")
        .withVariable("extendedTable", getExtendedTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(versionsToKeep, false)) //NOTE: We know that the base space is not an extended one
        .withQueryFragment("versionCheck", buildBaseVersionCheckFragment());
  }

  private SQLQuery build2LevelBaseQuery(E event) {
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);

    return new SQLQuery("(SELECT id, version, operation, jsondata, geo${{iColumnIntermediate}}"
        + "    FROM ${schema}.${intermediateExtensionTable}"
        + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{iOffsetIntermediate}} ${{limit}})"
        + "UNION ALL"
        + "    SELECT id, version, operation, jsondata, geo${{iColumn}} FROM"
        + "        ("
        + "            ${{innerBaseQuery}}"
        + "        ) b WHERE NOT exists(SELECT 1 FROM ${schema}.${intermediateExtensionTable} WHERE ${{idComparison}})")
        .withVariable("intermediateExtensionTable", getIntermediateTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(versionsToKeep, true)) //NOTE: We know that the intermediate space is an extended one
        .withQueryFragment("versionCheck", buildBaseVersionCheckFragment())
        .withQueryFragment("innerBaseQuery", build1LevelBaseQuery(event))
        .withQueryFragment("idComparison", buildIdComparisonFragment(event, "b."));
  }

  private String buildIdComparisonFragment(E event, String prefix) {
    return  "id = " + prefix + "id";
  }

  private SQLQuery buildDeletionCheckFragment(int v2k, boolean isExtended, boolean isDeleted) {
    if (v2k <= 1 && !isExtended) return new SQLQuery("");

    String operationsParamName = "operationsToFilterOut" + (isExtended ? "Extended" : ""); //TODO: That's a workaround for a minor bug in SQLQuery
    return new SQLQuery(" AND operation " + (isDeleted ? "" : "NOT") + " IN (SELECT unnest(#{" + operationsParamName + "}::CHAR[]))")
        .withNamedParameter(operationsParamName, Arrays.stream(isExtended
            ? new ModificationType[]{DELETE, INSERT_HIDE_COMPOSITE, UPDATE_HIDE_COMPOSITE}
            : new ModificationType[]{DELETE}).map(ModificationType::toString).toArray(String[]::new));
  }

  private SQLQuery buildDeletionCheckFragment(int v2k, boolean isExtended) { 
    return buildDeletionCheckFragment( v2k, isExtended, false); 
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    return (R) dbHandler.legacyDefaultFeatureResultSetHandler(rs);
  }

  public static SQLQuery buildSelectionFragment(ContextAwareEvent event) {
    String jsonDataWithVersion = "jsonb_set(jsondata, '{properties, @ns:com:here:xyz, version}', to_jsonb(version))";

    if (!(event instanceof SelectiveEvent) || ((SelectiveEvent) event).getSelection() == null
        || ((SelectiveEvent) event).getSelection().size() == 0)
      return new SQLQuery(jsonDataWithVersion + " AS jsondata");

    List<String> selection = ((SelectiveEvent) event).getSelection();
    if (!selection.contains("type")) {
      selection = new ArrayList<>(selection);
      selection.add("type");
    }

    return new SQLQuery("(SELECT "
        + "CASE WHEN prj_build ?? 'properties' THEN prj_build "
        + "ELSE jsonb_set(prj_build, '{properties}', '{}'::jsonb) "
        + "END "
        + "FROM prj_build(#{selection}, " + jsonDataWithVersion + ")) AS jsondata")
        .withNamedParameter("selection", selection.toArray(new String[0]));
  }

  private static String buildOrderByFragment(ContextAwareEvent event) {
    if (!(event instanceof SelectiveEvent)) return "";

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    return "*".equals(selectiveEvent.getRef()) ? "ORDER BY version" : "";
  }

  protected SQLQuery buildGeoFragment(E event) {
    return buildGeoFragment(event, true, null);
  }

  protected SQLQuery buildGeoFragment(ContextAwareEvent event, SQLQuery geoOverride) {
    return buildGeoFragment(event, true, geoOverride);
  }

  public static SQLQuery buildGeoFragment(ContextAwareEvent event, boolean convertToGeoJson) {
    return buildGeoFragment(event, convertToGeoJson, null);
  }

  protected static SQLQuery buildGeoFragment(ContextAwareEvent event, boolean convertToGeoJson, SQLQuery geoOverride) {
    boolean isForce2D = event instanceof SelectiveEvent ? ((SelectiveEvent) event).isForce2D() : false;
    String geo = geoOverride != null ? "${{geoOverride}}" : ((isForce2D ? "ST_Force2D" : "ST_Force3D") + "(geo)");

    if (convertToGeoJson)
      geo = "replace(ST_AsGeojson(" + geo + ", " + GetFeatures.GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0')";

    SQLQuery geoFragment = new SQLQuery(geo + " as geo");
    if (geoOverride != null)
      geoFragment.setQueryFragment("geoOverride", geoOverride);

    return geoFragment;
  }


  //TODO: Remove that hack and instantiate & use the whole GetFeatures QR instead from wherever it's needed
  public enum ViewModus { BASE_DELTA, CHANGE_BASE_DELTA, DELTA; }
  private ViewModus viewMode = ViewModus.BASE_DELTA;
  
  public SQLQuery _buildQuery(E event, ViewModus viewMode ) throws SQLException, ErrorResponseException {
    withoutIdField = true;
    this.viewMode = viewMode;
    return buildQuery(event);
  }
}
