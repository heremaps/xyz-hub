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
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class GetFeatures<E extends ContextAwareEvent> extends ExtendedSpace<E, FeatureCollection> {

  public GetFeatures(E event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
    setUseReadReplica(true);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException {
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
          + "            ) a WHERE NOT exists(SELECT 1 FROM ${schema}.${table} b WHERE " + buildIdComparisonFragment(event, "a.") + ")"
          + ") limitQuery ${{limit}}");
    }
    else {
      query = new SQLQuery(
          "SELECT ${{selection}}, ${{geo}}${{iColumn}}"
              + "    FROM ${schema}.${table}"
              + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{versionCheck}} ${{authorCheck}} ${{orderBy}} ${{limit}} ${{offset}}");
    }

    query.setQueryFragment("deletedCheck", buildDeletionCheckFragment(event));
    query.withQueryFragment("versionCheck", buildVersionCheckFragment(event));
    query.withQueryFragment("authorCheck", buildAuthorCheckFragment(event));
    query.setQueryFragment("selection", buildSelectionFragment(event));
    query.setQueryFragment("geo", buildGeoFragment(event));
    query.setQueryFragment("iColumn", ""); //NOTE: This can be overridden by implementing subclasses
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
      query.setQueryFragment("orderBy", ""); //NOTE: This can be overridden by implementing subclasses
      query.setQueryFragment("offset", ""); //NOTE: This can be overridden by implementing subclasses
    }

    return query;
  }

  // TODO things to keep in mind: versionsToKeep, enableUUID, isExtended
  private SQLQuery buildVersionCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent)) return new SQLQuery("");

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    int versionsToKeep = DatabaseHandler.readVersionsToKeep(event);
    long version = loadVersionFromRef(selectiveEvent);

    final SQLQuery defaultClause = new SQLQuery(" AND next_version = max_bigint() ${{minVersion}}")
        .withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent));

    if (versionsToKeep == 0) return new SQLQuery("");
    if (versionsToKeep == 1 || selectiveEvent.getRef() == null) return defaultClause;

    // versionsToKeep > 1 AND contains a reference to a version
    return new SQLQuery(" AND version <= #{version} AND next_version > #{version} ${{minVersion}}")
        .withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent))
        .withNamedParameter("version", version);
  }

  private SQLQuery buildMinVersionFragment(SelectiveEvent event) {
    return new SQLQuery("AND version >= (select max(version) - #{versionsToKeep} from ${schema}.${table}) ")
        .withNamedParameter("versionsToKeep", event.getVersionsToKeep());
  }

  private SQLQuery buildAuthorCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent)) return new SQLQuery("");

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    long v2k = DatabaseHandler.readVersionsToKeep(event);
    boolean emptyAuthor = selectiveEvent.getAuthor() == null;

    if (v2k < 0 || emptyAuthor) return new SQLQuery("");

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
    SQLQuery query = new SQLQuery("SELECT id, version, operation, jsondata, geo${{iColumnBase}}"
        + "    FROM ${schema}.${extendedTable} m"
        + "    WHERE ${{filterWhereClause}} ${{iOffsetBase}} ${{limit}}"); //in the base table there is no need to check a deleted flag;
    query.setVariable("extendedTable", getExtendedTable(event));
    return query;
  }

  private SQLQuery build2LevelBaseQuery(E event) {
    return new SQLQuery("(SELECT id, version, operation, jsondata, geo${{iColumnIntermediate}}"
        + "    FROM ${schema}.${intermediateExtensionTable}"
        + "    WHERE ${{filterWhereClause}} ${{deletedCheck}} ${{iOffsetIntermediate}} ${{limit}})"
        + "UNION ALL"
        + "    SELECT id, version, operation, jsondata, geo${{iColumn}} FROM"
        + "        ("
        + "            ${{innerBaseQuery}}"
        + "        ) b WHERE NOT exists(SELECT 1 FROM ${schema}.${intermediateExtensionTable} WHERE ${{idComparison}})")
        .withVariable("intermediateExtensionTable", getIntermediateTable(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(event))
        .withQueryFragment("innerBaseQuery", build1LevelBaseQuery(event))
        .withQueryFragment("idComparison", buildIdComparisonFragment(event, "b."));
  }

  private String buildIdComparisonFragment(E event, String prefix) {
    return DatabaseHandler.readVersionsToKeep(event) > 0 ? "id = " + prefix + "id" : "jsondata->>'id' = " + prefix + "jsondata->>'id'";
  }

  private String buildDeletionCheckFragment(E event) {
    // queries on the EXTENSION layer to avoid the deletion check and simply return the row
    if (event.getContext() == EXTENSION) return "";

    return " AND operation != 'D'";
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    return dbHandler.defaultFeatureResultSetHandler(rs);
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
    if (DatabaseHandler.readVersionsToKeep(event) > 0)
      return "jsonb_set(" + wrappedJsondata + ", '{properties, @ns:com:here:xyz, version}', to_jsonb(version))";
    return wrappedJsondata;
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
      geo = "replace(ST_AsGeojson(" + geo + ", " + SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0')";

    SQLQuery geoFragment = new SQLQuery(geo + " as geo");
    if (geoOverride != null)
      geoFragment.setQueryFragment("geoOverride", geoOverride);

    return geoFragment;
  }

  protected static String buildIdFragment(ContextAwareEvent event) {
    return DatabaseHandler.readVersionsToKeep(event) < 1 ? "jsondata->>'id'" : "id";
  }
}
