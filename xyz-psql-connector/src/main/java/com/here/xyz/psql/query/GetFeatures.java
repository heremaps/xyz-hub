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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.COMPOSITE_EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.models.hub.Ref.HEAD;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT_HIDE_COMPOSITE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE_HIDE_COMPOSITE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.DatabaseWriter.ModificationType;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class GetFeatures<E extends ContextAwareEvent, R extends XyzResponse> extends ExtendedSpace<E, R> {
  protected static final long MAX_RESULT_SIZE = 100 * 1024 * 1024;
  public static final long GEOMETRY_DECIMAL_DIGITS = 8;
  public static long MAX_BIGINT = Long.MAX_VALUE;

  public GetFeatures(E event) throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);
    boolean useExtensionQuery = isExtendedSpace(event) && (event.getContext() == DEFAULT || event.getContext() == COMPOSITE_EXTENSION);
    SQLQuery versionCheckFragment = buildVersionCheckFragment(event);
    SQLQuery query;
    if (useExtensionQuery) {
      query = new SQLQuery(
          "SELECT * FROM ("
          + "    (SELECT ${{selection}}, ${{geo}}${{iColumnExtension}}${{id}}"
          + "        FROM ${schema}.${table}"
          + "        WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{authorCheck}} ${{iOffsetExtension}})"
          + " ${{unionAll}} "
          + "        SELECT ${{selection}}, ${{geo}}${{iColumn}}${{id}} FROM"
          + "            ("
          + "                ${{baseQuery}}"
          + "            ) a WHERE ${{exists}} exists(SELECT 1 FROM ${schema}.${table} b WHERE ${{notExistsIdComparison}})"
          + ") limitQuery ${{limit}}")
          .withQueryFragment("notExistsIdComparison", buildIdComparisonFragment(event, "a.", versionCheckFragment))
          .withQueryFragment("unionAll", event.getContext() == COMPOSITE_EXTENSION ? "UNION DISTINCT" : "UNION ALL")
          .withQueryFragment("exists", event.getContext() == COMPOSITE_EXTENSION ? "" : "NOT")
          .withQueryFragment("iColumnExtension", "") //NOTE: This can be overridden by implementing subclasses
          .withQueryFragment("iOffsetExtension", "")
          .withQueryFragment("baseQuery", !is2LevelExtendedSpace(event)
              ? build1LevelBaseQuery(event) //1-level extension
              : build2LevelBaseQuery(event)) //2-level extension
          .withQueryFragment("iColumnBase", "") //NOTE: This can be overridden by implementing subclasses
          .withQueryFragment("iOffsetBase", "") //NOTE: This can be overridden by implementing subclasses
          .withQueryFragment("iColumnIntermediate", "") //NOTE: This can be overridden by implementing subclasses
          .withQueryFragment("iOffsetIntermediate", ""); //NOTE: This can be overridden by implementing subclasses
    }
    else {
      query = new SQLQuery(
          "SELECT ${{selection}}, ${{geo}}${{iColumn}}${{id}}"
              + "    FROM ${schema}.${table} ${{tableSample}}"
              + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{authorCheck}} ${{orderBy}} ${{limit}} ${{offset}}")
          .withQueryFragment("orderBy", buildOrderByFragment(event)) //NOTE: This can be overridden by implementing subclasses
          .withQueryFragment("offset", ""); //NOTE: This can be overridden by implementing subclasses
    }

    return query
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(versionsToKeep, useExtensionQuery))
        .withQueryFragment("versionCheck", versionCheckFragment)
        .withQueryFragment("authorCheck", buildAuthorCheckFragment(event))
        .withQueryFragment("selection", buildSelectionFragment(event))
        .withQueryFragment("geo", buildGeoFragment(event))
        .withQueryFragment("iColumn", "") //NOTE: This can be overridden by implementing subclasses
        .withQueryFragment("tableSample", "") //NOTE: This can be overridden by implementing subclasses
        .withQueryFragment("limit", "") //NOTE: This can be overridden by implementing subclasses
        .withQueryFragment("id", ", id");
  }

  private SQLQuery buildVersionCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent selectiveEvent))
      return new SQLQuery("");

    return new SQLQuery("${{versionComparison}} ${{nextVersion}} ${{minVersion}}")
        .withQueryFragment("versionComparison", buildVersionComparison(selectiveEvent))
        .withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent))
        .withQueryFragment("nextVersion", buildNextVersionFragment(selectiveEvent));
  }

  private SQLQuery buildVersionComparison(SelectiveEvent event) {
    Ref ref = event.getRef();
    if (event.getVersionsToKeep() == 1 || ref.isAllVersions() || ref.isHead())
      return new SQLQuery("");

    return new SQLQuery("AND version <= #{version}")
        .withNamedParameter("version", ref.getVersion());
  }

  private SQLQuery buildNextVersionFragment(SelectiveEvent event) {
    return buildNextVersionFragment(event.getRef(), event.getVersionsToKeep() > 1,
        "requestedVersion");
  }

  private SQLQuery buildNextVersionFragment(Ref ref, boolean historyEnabled, String versionParamName) {
    if (!historyEnabled || ref.isAllVersions())
      return new SQLQuery("");

    return new SQLQuery("AND next_version ${{op}} #{" + versionParamName + "}")
        .withQueryFragment("op", ref.isHead() ? "=" : ">")
        .withNamedParameter(versionParamName, ref.isHead() ? MAX_BIGINT : ref.getVersion());
  }

  private SQLQuery buildBaseVersionCheckFragment(String versionParamName) {
    /*
    Always assume HEAD version for base spaces and also assume history to be enabled,
    because from the even we don't know whether it is enabled or not.
     */
    return buildNextVersionFragment(new Ref(HEAD), true, versionParamName);
  }

  private SQLQuery buildMinVersionFragment(SelectiveEvent event) {
    Ref ref = event.getRef();
    boolean isHeadOrStar = ref.isHead() || ref.isAllVersions();
    long version = isHeadOrStar ? Long.MAX_VALUE : ref.getVersion();
    if (event.getVersionsToKeep() > 1)
      return new SQLQuery("AND greatest(#{minVersion}, (SELECT max(version) - #{versionsToKeep} FROM ${schema}.${table})) <= #{version}")
          .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
          .withNamedParameter("minVersion", event.getMinVersion())
          .withNamedParameter("version", version);
    return isHeadOrStar ? new SQLQuery("") : new SQLQuery("AND #{version} = (SELECT max(version) as HEAD FROM ${schema}.${table})")
        .withNamedParameter("version", version);
  }

  private SQLQuery buildAuthorCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent selectiveEvent) || selectiveEvent.getAuthor() == null)
      return new SQLQuery("");

    return new SQLQuery(" AND author = #{author}")
        .withNamedParameter("author", selectiveEvent.getAuthor());
  }

  private SQLQuery build1LevelBaseQuery(E event) {
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);

    return new SQLQuery("SELECT id, version, operation, jsondata, geo${{iColumnBase}}"
        + "    FROM ${schema}.${extendedTable} m"
        + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{iOffsetBase}}")
        .withVariable("extendedTable", getExtendedTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(versionsToKeep, false)) //NOTE: We know that the base space is not an extended one
        .withQueryFragment("versionCheck", buildBaseVersionCheckFragment("base1Version"));
  }

  private SQLQuery build2LevelBaseQuery(E event) {
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);
    SQLQuery versionCheckFragment = buildBaseVersionCheckFragment("base2Version");

    return new SQLQuery("(SELECT id, version, operation, jsondata, geo${{iColumnIntermediate}}"
        + "    FROM ${schema}.${intermediateExtensionTable}"
        + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{iOffsetIntermediate}}) "
        + "UNION ALL"
        + "    SELECT id, version, operation, jsondata, geo${{iColumn}} FROM"
        + "        ("
        + "            ${{innerBaseQuery}}"
        + "        ) b WHERE NOT exists(SELECT 1 FROM ${schema}.${intermediateExtensionTable} WHERE ${{idComparison}})")
        .withVariable("intermediateExtensionTable", getIntermediateTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(versionsToKeep, true)) //NOTE: We know that the intermediate space is an extended one
        .withQueryFragment("versionCheck", versionCheckFragment)
        .withQueryFragment("innerBaseQuery", build1LevelBaseQuery(event))
        .withQueryFragment("idComparison", buildIdComparisonFragment(event, "b.", versionCheckFragment));
  }

  private SQLQuery buildIdComparisonFragment(E event, String prefix, SQLQuery versionCheckFragment) {
    return new SQLQuery("id = " + prefix + "id"
        + (event instanceof SelectiveEvent ? " ${{versionCheck}} AND operation != 'D'" : ""))
        .withQueryFragment("versionCheck", versionCheckFragment);
  }

  private SQLQuery buildDeletionCheckFragment(int v2k, boolean isExtended) {
    if (v2k == 1 && !isExtended) return new SQLQuery("");

    String operationsParamName = "operationsToFilterOut" + (isExtended ? "Extended" : ""); //TODO: That's a workaround for a minor bug in SQLQuery
    return new SQLQuery(" AND operation NOT IN (SELECT unnest(#{" + operationsParamName + "}::CHAR[]))")
        .withNamedParameter(operationsParamName, Arrays.stream(isExtended
            ? new ModificationType[]{DELETE, INSERT_HIDE_COMPOSITE, UPDATE_HIDE_COMPOSITE}
            : new ModificationType[]{DELETE}).map(ModificationType::toString).toArray(String[]::new));
  }

  /**
   * The default handler for the most results.
   *
   * @param rs The result set.
   * @return The generated feature collection from the result set.
   * @throws SQLException When any unexpected error happened.
   */
  @Override
  public R handle(ResultSet rs) throws SQLException {
    StringBuilder result = new StringBuilder();
    String prefix = "[";
    result.append(prefix);

    while (rs.next() && MAX_RESULT_SIZE > result.length())
      handleFeature(rs, result);

    if (result.length() > prefix.length())
      result.setLength(result.length() - 1);

    result.append("]");

    final FeatureCollection featureCollection = new FeatureCollection();
    featureCollection._setFeatures(result.toString());

    if (result.length() > MAX_RESULT_SIZE)
      throw new SQLException("Maximum response char limit of " + MAX_RESULT_SIZE + " reached");

    return (R) featureCollection;
  }

  protected void handleFeature(ResultSet rs, StringBuilder result) throws SQLException {
    String geom = rs.getString("geo");
    result.append(rs.getString("jsondata"));
    result.setLength(result.length() - 1);
    result.append(",\"geometry\":");
    result.append(geom == null ? "null" : geom);
    result.append("}");
    result.append(",");
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
    if (!(event instanceof SelectiveEvent selectiveEvent))
      return "";
    return selectiveEvent.getRef().isAllVersions() ? "ORDER BY version" : "";
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
}
