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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.COMPOSITE_EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.models.hub.Ref.HEAD;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.DELETE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.INSERT_HIDE_COMPOSITE;
import static com.here.xyz.psql.DatabaseWriter.ModificationType.UPDATE_HIDE_COMPOSITE;
import static com.here.xyz.responses.XyzError.PAYLOAD_TO_LARGE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.DatabaseWriter.ModificationType;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public abstract class GetFeatures<E extends ContextAwareEvent, R extends XyzResponse> extends ExtendedSpace<E, R> {
  protected static final long MAX_RESULT_SIZE = 100 * 1024 * 1024;
  public static final long GEOMETRY_DECIMAL_DIGITS = 8;
  public static final String NO_GEOMETRY = "!geometry";
  public static long MAX_BIGINT = Long.MAX_VALUE;
  private boolean historyEnabled;

  public GetFeatures(E event) throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
    historyEnabled = event.getVersionsToKeep() > 1;
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    SQLQuery filterWhereClause = buildFilterWhereClause(event);

    SQLQuery query;
    if (isCompositeQuery(event)) {
      int dataset = compositeDatasetNo(event, CompositeDataset.EXTENSION);
      SQLQuery versionCheckFragment = buildVersionCheckFragment(event);

      query = new SQLQuery(
          "SELECT * FROM (SELECT * FROM ("
          + "     (SELECT ${{selectClause}} FROM ${schema}.${table} WHERE ${{filters}} ${{versionCheck}} ${{orderBy}})"
          + "   ${{unionAll}} "
          + "     SELECT * FROM (${{baseQuery}}) a"
          + "       ${{compositionFilter}}"
          + ") limitQuery ${{limit}}) orderQuery ${{outerOrderBy}}")
          .withQueryFragment("selectClause", buildSelectClause(event, dataset))
          .withQueryFragment("filters", buildFiltersFragment(event, true, filterWhereClause, dataset))
          .withQueryFragment("orderBy", buildOrderByFragment(event))
          .withQueryFragment("versionCheck", versionCheckFragment)
          .withQueryFragment("unionAll", event.getContext() == COMPOSITE_EXTENSION ? "UNION DISTINCT" : "UNION ALL")
          .withQueryFragment("compositionFilter", buildCompositionFilter(event, false, buildIdComparisonFragment(event, "a.", versionCheckFragment)))
          .withQueryFragment("baseQuery", !is2LevelExtendedSpace(event)
              ? build1LevelBaseQuery(event, filterWhereClause) //1-level extension
              : build2LevelBaseQuery(event, filterWhereClause)); //2-level extension
    }
    else {
      query = new SQLQuery(
          "SELECT ${{selectClause}} FROM ${schema}.${table} WHERE ${{filters}} ${{versionCheck}} ${{outerOrderBy}} ${{limit}}")
          .withQueryFragment("selectClause", buildSelectClause(event, 0))
          .withQueryFragment("filters", buildFiltersFragment(event, false, filterWhereClause, 0))
          .withQueryFragment("versionCheck", buildVersionCheckFragment(event));
    }

    return query
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event))
        .withQueryFragment("outerOrderBy", buildOuterOrderByFragment(event))
        .withQueryFragment("limit", buildLimitFragment(event));
  }

  private SQLQuery buildCompositionFilter(E event, boolean isL2, SQLQuery idComparisonFragment) {
    if (event instanceof SelectiveEvent selectiveEvent && selectiveEvent.getRef().isAllVersions())
      return new SQLQuery("");

    String tableVariable = isL2 ? "intermediateExtensionTable" : "table";
    return new SQLQuery("WHERE ${{exists}} exists(SELECT 1 FROM ${schema}.${" + tableVariable + "} WHERE ${{idComparison}})")
        .withQueryFragment("exists", event.getContext() == COMPOSITE_EXTENSION && !isL2 ? "" : "NOT")
        .withQueryFragment("idComparison", idComparisonFragment);
  }

  protected SQLQuery buildFiltersFragment(E event, boolean isExtension, SQLQuery filterWhereClause, int dataset) {
    return new SQLQuery("${{filterWhereClause}} ${{authorCheck}} ${{deletedCheck}}")
        .withQueryFragment("filterWhereClause", filterWhereClause)
        .withQueryFragment("authorCheck", buildAuthorCheckFragment(event))
        .withQueryFragment("deletedCheck", buildDeletionCheckFragment(event, isExtension));
  }

  protected SQLQuery buildFilterWhereClause(E event) {
    return new SQLQuery("TRUE");
  }

  protected SQLQuery buildSelectClause(E event, int dataset) {
    return new SQLQuery("id, ${{version}}, ${{jsonData}}, ${{geo}}, ${{dataset}}")
        .withQueryFragment("jsonData", buildJsonDataFragment(event, dataset))
        .withQueryFragment("geo", buildGeoFragment(event))
        .withQueryFragment("dataset", new SQLQuery("${{datasetNo}} AS dataset")
        .withQueryFragment("datasetNo", "" + dataset))
        .withQueryFragment("version", getFeatureVersion(dataset) + " AS version");
  }

  private SQLQuery buildVersionCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent selectiveEvent))
      return new SQLQuery("");

    return new SQLQuery("${{versionComparison}} ${{nextVersion}} ${{minVersion}}")
        .withQueryFragment("versionComparison", buildVersionComparison(selectiveEvent))
        .withQueryFragment("nextVersion", buildNextVersionFragment(selectiveEvent))
        .withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent));
  }

  private SQLQuery buildVersionComparison(SelectiveEvent event) {
    Ref ref = event.getRef();
    if (event.getVersionsToKeep() == 1 || ref.isAllVersions() || ref.isHead())
      return new SQLQuery("");

    if (ref.isRange())
      return new SQLQuery("AND version > #{startVersion} AND version <= #{endVersion}")
          .withNamedParameter("startVersion", ref.getStart().getVersion())
          .withNamedParameter("endVersion", ref.getEnd().getVersion());

    return new SQLQuery("AND version <= #{requestedVersion}")
        .withNamedParameter("requestedVersion", ref.getVersion());
  }

  private SQLQuery buildNextVersionFragment(SelectiveEvent event) {
    return buildNextVersionFragment(event.getRef(), event.getVersionsToKeep() > 1,
        "requestedVersion");
  }

  private SQLQuery buildNextVersionFragment(Ref ref, boolean historyEnabled, String versionParamName) {
    if (!historyEnabled || ref.isAllVersions())
      return new SQLQuery("");

    boolean maxVersionIsHead = ref.isHead() || ref.isRange() && ref.getEnd().isHead();
    long maxVersion = maxVersionIsHead ? MAX_BIGINT : ref.isRange() ? ref.getEnd().getVersion() : ref.getVersion();

    return new SQLQuery("AND next_version ${{op}} #{" + versionParamName + "}")
        .withQueryFragment("op", maxVersionIsHead ? "=" : ">")
        .withNamedParameter(versionParamName, maxVersion);
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
    boolean isHeadOrAllVersions = ref.isHead() || ref.isAllVersions() || ref.isRange() && ref.getEnd().isHead();
    long requestedVersion = isHeadOrAllVersions ? Long.MAX_VALUE : ref.isRange() ? ref.getEnd().getVersion() : ref.getVersion();

    if (event.getVersionsToKeep() > 1)
      return new SQLQuery("AND greatest(#{minVersion}, (SELECT max(version) - #{versionsToKeep} FROM ${schema}.${table})) <= #{requestedVersion}")
          .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
          .withNamedParameter("minVersion", event.getMinVersion())
          .withNamedParameter("requestedVersion", requestedVersion);

    return isHeadOrAllVersions ? new SQLQuery("") : new SQLQuery("AND #{requestedVersion} = (SELECT max(version) AS HEAD FROM ${schema}.${table})")
        .withNamedParameter("requestedVersion", requestedVersion);
  }

  private SQLQuery buildAuthorCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent selectiveEvent) || selectiveEvent.getAuthor() == null)
      return new SQLQuery("");

    return new SQLQuery(" AND author = #{author}")
        .withNamedParameter("author", selectiveEvent.getAuthor());
  }

  private SQLQuery build1LevelBaseQuery(E event, SQLQuery filterWhereClause) {
    int dataset = compositeDatasetNo(event, CompositeDataset.SUPER);
    return new SQLQuery("SELECT ${{selectClause}} FROM ${schema}.${extendedTable} WHERE ${{filters}} ${{versionCheck}} ${{orderBy}}")
        .withVariable("extendedTable", getExtendedTable(event))
        .withQueryFragment("selectClause", buildSelectClause(event, dataset))
        .withQueryFragment("filters", buildFiltersFragment(event, false, filterWhereClause, dataset)) //NOTE: We know that the base space is not an extended one
        .withQueryFragment("versionCheck", buildBaseVersionCheckFragment("base1Version"))
        .withQueryFragment("orderBy", buildOrderByFragment(event));
  }

  private SQLQuery build2LevelBaseQuery(E event, SQLQuery filterWhereClause) {
    SQLQuery versionCheckFragment = buildBaseVersionCheckFragment("base2Version");

    int dataset = compositeDatasetNo(event, CompositeDataset.INTERMEDIATE);
    return new SQLQuery("(SELECT ${{selectClause}}"
        + "  FROM ${schema}.${intermediateExtensionTable} WHERE ${{filters}} ${{versionCheck}} ${{orderBy}}) "
        + "UNION ALL"
        + "  SELECT * FROM (${{baseQuery}}) b"
        + "    ${{compositionFilter}}")
        .withVariable("intermediateExtensionTable", getIntermediateTable(event))
        .withQueryFragment("selectClause", buildSelectClause(event, dataset))
        .withQueryFragment("filters", buildFiltersFragment(event, false, filterWhereClause, dataset)) //NOTE: We know that the intermediate space is an extended one
        .withQueryFragment("versionCheck", versionCheckFragment)
        .withQueryFragment("orderBy", buildOrderByFragment(event))
        .withQueryFragment("baseQuery", build1LevelBaseQuery(event, filterWhereClause))
        .withQueryFragment("compositionFilter", buildCompositionFilter(event, true, buildIdComparisonFragment(event, "b.", versionCheckFragment)));
  }

  private SQLQuery buildIdComparisonFragment(E event, String prefix, SQLQuery versionCheckFragment) {
    return new SQLQuery("id = " + prefix + "id"
        + (event instanceof SelectiveEvent ? " ${{versionCheck}} AND operation != 'D'" : ""))
        .withQueryFragment("versionCheck", versionCheckFragment);
  }

  private SQLQuery buildDeletionCheckFragment(E event, boolean isExtension) {
    if (!historyEnabled && !isExtension
        || event instanceof SelectiveEvent selectiveEvent && selectiveEvent.getRef().isRange())
      return new SQLQuery("");

    String operationsParamName = "operationsToFilterOut" + (isExtension ? "Extension" : ""); //TODO: That's a workaround for a minor bug in SQLQuery
    return new SQLQuery(" AND operation NOT IN (SELECT unnest(#{" + operationsParamName + "}::CHAR[]))")
        .withNamedParameter(operationsParamName, Arrays.stream(isExtension
            ? new ModificationType[]{DELETE, INSERT_HIDE_COMPOSITE, UPDATE_HIDE_COMPOSITE}
            : new ModificationType[]{DELETE}).map(ModificationType::toString).toArray(String[]::new));
  }

  protected SQLQuery buildLimitFragment(E event) {
    return new SQLQuery("");
  }

  protected boolean isCompositeQuery(E event) {
    return isExtendedSpace(event) && (event.getContext() == DEFAULT || event.getContext() == COMPOSITE_EXTENSION);
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
      //TODO: Throw ErrorResponseException instead
      return (R) new ErrorResponse().withError(PAYLOAD_TO_LARGE)
          .withErrorMessage("Maximum response char limit of " + MAX_RESULT_SIZE + " reached");

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

  protected SQLQuery buildJsonDataFragment(ContextAwareEvent event, int dataset) {
    SQLQuery jsonData;

    if (!(event instanceof SelectiveEvent selectiveEvent) || selectiveEvent.getSelection() == null)
      jsonData = new SQLQuery("jsondata");
    else {
      Set<String> selection = new HashSet<>(selectiveEvent.getSelection());
      selection.remove(NO_GEOMETRY);
      selection.add("type");

      jsonData = selection.isEmpty() ? new SQLQuery("jsondata") : new SQLQuery("(SELECT "
          + "CASE WHEN prj_build->'properties' IS NOT NULL THEN prj_build "
          + "ELSE jsonb_set(prj_build, '{properties}', '{}'::jsonb) "
          + "END "
          + "FROM prj_build(#{selection}, jsondata))")
          .withNamedParameter("selection", selection.toArray(new String[0]));
    }

    return new SQLQuery("jsonb_set(${{innerJsonData}}, '{properties, @ns:com:here:xyz, version}', to_jsonb(${{featureVersion}})) AS jsondata")
        .withQueryFragment("innerJsonData", jsonData)
        .withQueryFragment("featureVersion", getFeatureVersion(dataset));
  }

  private static String getFeatureVersion(int dataset) {
    /*
    NOTE: From the perspective of a composite layer (requested with context = DEFAULT),
    the extended dataset(s) always have version = 0, because the extension itself *is empty* and this *empty version (aka version 0)*
    is simply not empty but contains the whole HEAD of the dataset being extended (e.g., INTERMEDIATE and / or SUPER dataset).
     */
    return dataset > 0 ? "0" : "version";
  }

  protected String buildOuterOrderByFragment(ContextAwareEvent event) {
    return buildOrderByFragment(event);
  }

  protected String buildOrderByFragment(ContextAwareEvent event) {
    if (!(event instanceof SelectiveEvent selectiveEvent))
      return "";
    return selectiveEvent.getRef().isAllVersions() ? "ORDER BY version" : "";
  }

  private boolean selectNoGeometry(E event) {
    return event instanceof SelectiveEvent selectiveEvent
        && selectiveEvent.getSelection() != null
        && !selectiveEvent.getSelection().isEmpty()
        && selectiveEvent.getSelection().contains(NO_GEOMETRY);
  }

  protected SQLQuery buildGeoFragment(E event) {
    if (selectNoGeometry(event))
      return new SQLQuery("NULL::GEOMETRY AS geo");

    return new SQLQuery("${{geoExpression}} AS geo")
        .withQueryFragment("geoExpression", buildGeoJsonExpression(event));
  }

  protected SQLQuery buildGeoJsonExpression(E event) {
    return new SQLQuery("REGEXP_REPLACE(ST_AsGeojson(${{rawGeoExpression}}, ${{precision}}), 'nan', '0', 'gi')")
          .withQueryFragment("rawGeoExpression", buildRawGeoExpression(event))
          .withQueryFragment("precision", "" + GetFeatures.GEOMETRY_DECIMAL_DIGITS);
  }

  protected SQLQuery buildRawGeoExpression(E event) {
    boolean isForce2D = event instanceof SelectiveEvent selectiveEvent ? selectiveEvent.isForce2D() : false;
    return new SQLQuery((isForce2D ? "ST_Force2D" : "ST_Force3D") + "(geo)");
  }

  protected enum CompositeDataset {
    EXTENSION,
    INTERMEDIATE,
    SUPER
  }

  protected int compositeDatasetNo(E event, CompositeDataset dataset) {
    return switch (dataset) {
      case EXTENSION -> 0;
      case INTERMEDIATE -> 1;
      case SUPER -> is2LevelExtendedSpace(event) ? 2 : 1;
    };
  }
}
