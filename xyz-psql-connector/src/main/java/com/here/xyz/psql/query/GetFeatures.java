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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT_HIDE_COMPOSITE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE_HIDE_COMPOSITE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.QueryEvent;
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

  public GetFeatures(E event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
    setUseReadReplica(true);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    boolean isExtended = isExtendedSpace(event) && event.getContext() == DEFAULT;
    SQLQuery query;
    if (isExtended) {
      query = new SQLQuery(
          "SELECT * FROM ("
          + "    (SELECT ${{selection}}, ${{geo}}${{iColumnExtension}}"
          + "        FROM ${schema}.${table}"
          + "        WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{authorCheck}} ${{iOffsetExtension}} ${{limit}})"
          + "    UNION ALL "
          + "        SELECT ${{selection}}, ${{geo}}${{iColumn}} FROM"
          + "            ("
          + "                ${{baseQuery}}"
          + "            ) a WHERE NOT exists(SELECT 1 FROM ${schema}.${table} b WHERE ${{notExistsIdComparison}})"
          + ") limitQuery ${{limit}}")
          .withQueryFragment("notExistsIdComparison", buildIdComparisonFragment(event, "a."));
    }
    else {
      query = new SQLQuery(
          "SELECT ${{selection}}, ${{geo}}${{iColumn}}"
              + "    FROM ${schema}.${table} ${{tableSample}}"
              + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{authorCheck}} ${{orderBy}} ${{limit}} ${{offset}}");
    }

    query.setQueryFragment("deletedCheck", buildDeletionCheckFragment(isExtended));
    query.withQueryFragment("versionCheck", buildVersionCheckFragment(event));
    query.withQueryFragment("authorCheck", buildAuthorCheckFragment(event));
    query.setQueryFragment("selection", buildSelectionFragment(event));
    query.setQueryFragment("geo", buildGeoFragment(event));
    query.setQueryFragment("iColumn", ""); //NOTE: This can be overridden by implementing subclasses
    query.setQueryFragment("tableSample", ""); //NOTE: This can be overridden by implementing subclasses
    query.setQueryFragment("limit", ""); //NOTE: This can be overridden by implementing subclasses

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
        .withQueryFragment("nextVersion", versionIsStar || versionsToKeep <= 1 ? "" : " AND next_version = max_bigint() ");
    if (versionsToKeep > 1)
      defaultClause.withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent));
    else
      defaultClause.withQueryFragment("minVersion", "");

    if (versionsToKeep == 0) return new SQLQuery("");
    if (versionsToKeep == 1 || versionIsNotPresent || versionIsStar) return defaultClause;

    // versionsToKeep > 1 AND contains a reference to a version or version is a valid version
    return new SQLQuery(" AND version <= #{version} AND next_version > #{version} ${{minVersion}}")
        .withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent))
        .withNamedParameter("version", loadVersionFromRef(selectiveEvent));
  }

  private String buildBaseVersionCheckFragment() {
    return " AND next_version = max_bigint()"; //Always assume HEAD version for base spaces
  }

  private SQLQuery buildMinVersionFragment(SelectiveEvent event) {
    return new SQLQuery("AND greatest(#{minVersion}, (select max(version) - #{versionsToKeep} from ${schema}.${table})) <= #{version} ")
        .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
        .withNamedParameter("minVersion", event.getMinVersion())
        .withNamedParameter("version", loadVersionFromRef(event));
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

  private long loadVersionFromRef(SelectiveEvent event) {
    try {
      return Integer.parseInt(event.getRef());
    } catch (NumberFormatException e) {
      return Long.MAX_VALUE;
    }
  }

  private SQLQuery build1LevelBaseQuery(E event) {
    return new SQLQuery("SELECT id, version, operation, jsondata, geo${{iColumnBase}}"
        + "    FROM ${schema}.${extendedTable} m"
        + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{iOffsetBase}} ${{limit}}")
        .withVariable("extendedTable", getExtendedTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(false)) //NOTE: We know that the base space is not an extended one
        .withQueryFragment("versionCheck", buildBaseVersionCheckFragment());
  }

  private SQLQuery build2LevelBaseQuery(E event) {
    return new SQLQuery("(SELECT id, version, operation, jsondata, geo${{iColumnIntermediate}}"
        + "    FROM ${schema}.${intermediateExtensionTable}"
        + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{iOffsetIntermediate}} ${{limit}})"
        + "UNION ALL"
        + "    SELECT id, version, operation, jsondata, geo${{iColumn}} FROM"
        + "        ("
        + "            ${{innerBaseQuery}}"
        + "        ) b WHERE NOT exists(SELECT 1 FROM ${schema}.${intermediateExtensionTable} WHERE ${{idComparison}})")
        .withVariable("intermediateExtensionTable", getIntermediateTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(true)) //NOTE: We know that the intermediate space is an extended one
        .withQueryFragment("versionCheck", buildBaseVersionCheckFragment())
        .withQueryFragment("innerBaseQuery", build1LevelBaseQuery(event))
        .withQueryFragment("idComparison", buildIdComparisonFragment(event, "b."));
  }

  private String buildIdComparisonFragment(E event, String prefix) {
    return DatabaseHandler.readVersionsToKeep(event) > 0 ? "id = " + prefix + "id" : "jsondata->>'id' = " + prefix + "jsondata->>'id'";
  }

  private SQLQuery buildDeletionCheckFragment(boolean isExtended) {
    String operationsParamName = "operationsToFilterOut" + (isExtended ? "Extended" : ""); //TODO: That's a workaround for a minor bug in SQLQuery
    return new SQLQuery(" AND operation NOT IN (SELECT unnest(#{" + operationsParamName + "}::CHAR[]))")
        .withNamedParameter(operationsParamName, Arrays.stream(isExtended
            ? new ModificationType[]{DELETE, INSERT_HIDE_COMPOSITE, UPDATE_HIDE_COMPOSITE}
            : new ModificationType[]{DELETE}).map(ModificationType::toString).toArray(String[]::new));
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    return (R) dbHandler.defaultFeatureResultSetHandler(rs);
  }

  protected static SQLQuery buildSelectionFragment(ContextAwareEvent event) {
    String jsonDataWithVersion = injectVersionIntoNS(event, "jsondata");

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

  private static String injectVersionIntoNS(ContextAwareEvent event, String wrappedJsondata) {
    //NOTE: The following is a temporary implementation for backwards compatibility for spaces without versioning
    if (DatabaseHandler.readVersionsToKeep(event) > 1 || (DatabaseHandler.readVersionsToKeep(event) > 0 && event instanceof LoadFeaturesEvent))
      return "jsonb_set(" + wrappedJsondata + ", '{properties, @ns:com:here:xyz, version}', to_jsonb(version))";
    return wrappedJsondata;
  }

  private static String buildOrderByFragment(ContextAwareEvent event) {
    if (!(event instanceof SelectiveEvent)) return "";

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    return "*".equals(selectiveEvent.getRef()) ? "ORDER BY version" : "";
  }

  //TODO: Can be removed after completion of refactoring
  @Deprecated
  public static SQLQuery buildSelectionFragmentBWC(QueryEvent event) {
    SQLQuery selectionFragment = buildSelectionFragment(event);
    selectionFragment.replaceNamedParameters();
    return selectionFragment;
  }

  protected SQLQuery buildGeoFragment(E event) {
    return buildGeoFragment(event, true, null);
  }

  protected SQLQuery buildGeoFragment(ContextAwareEvent event, SQLQuery geoOverride) {
    return buildGeoFragment(event, true, geoOverride);
  }

  protected static SQLQuery buildGeoFragment(ContextAwareEvent event, boolean convertToGeoJson) {
    return buildGeoFragment(event, convertToGeoJson, null);
  }

  protected static SQLQuery buildGeoFragment(ContextAwareEvent event, boolean convertToGeoJson, SQLQuery geoOverride) {
    boolean isForce2D = event instanceof SelectiveEvent ? ((SelectiveEvent) event).isForce2D() : false;
    String geo = geoOverride != null ? "${{geoOverride}}" : ((isForce2D ? "ST_Force2D" : "ST_Force3D") + "(geo)");

    if (convertToGeoJson)
      geo = "replace(ST_AsGeojson(" + geo + ", " + GetFeaturesByBBox.GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0')";

    SQLQuery geoFragment = new SQLQuery(geo + " as geo");
    if (geoOverride != null)
      geoFragment.setQueryFragment("geoOverride", geoOverride);

    return geoFragment;
  }

  protected static String buildIdFragment(ContextAwareEvent event) {
    return DatabaseHandler.readVersionsToKeep(event) < 1 ? "jsondata->>'id'" : "id";
  }
}
