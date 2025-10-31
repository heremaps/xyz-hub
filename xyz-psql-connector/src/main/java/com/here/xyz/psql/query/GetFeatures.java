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
import static com.here.xyz.psql.query.branching.BranchManager.branchTableName;
import static com.here.xyz.psql.query.branching.BranchManager.getNodeId;
import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.PAYLOAD_TO_LARGE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.DatabaseWriter.ModificationType;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
          .withQueryFragment("selectClause", buildSelectClause(event, dataset, 0))
          .withQueryFragment("filters", buildFiltersFragment(event, true, filterWhereClause, dataset))
          .withQueryFragment("orderBy", buildOrderByFragment(event))
          .withQueryFragment("versionCheck", versionCheckFragment)
          .withQueryFragment("unionAll", event.getContext() == COMPOSITE_EXTENSION ? "UNION DISTINCT" : "UNION ALL")
          .withQueryFragment("compositionFilter", buildCompositionFilter(event, false, buildIdComparisonFragment(event, "a.", versionCheckFragment)))
          .withQueryFragment("baseQuery", !is2LevelExtendedSpace(event)
              ? build1LevelBaseQuery(event, filterWhereClause) //1-level extension
              : build2LevelBaseQuery(event, filterWhereClause)); //2-level extension
    }
    else if (event.getNodeId() > 0) {
      if (event.getBranchPath() == null || event.getBranchPath().isEmpty())
        throw new ErrorResponseException(EXCEPTION, "Illegal event: Branch path must be provided if the node ID > 0 (not main branch)");

      String rootTableName = getDefaultTable(event);

      SQLQuery innerDataQuery = new SQLQuery("SELECT ${{selectClause}} FROM ${schema}.${table} WHERE ${{filters}} ${{versionCheck}}")
          .withVariable(TABLE, rootTableName)
          .withQueryFragment("selectClause", buildSelectClause(event, 0, 0))
          .withQueryFragment("filters", buildFiltersFragment(event, false, filterWhereClause, 0))
          .withQueryFragment("versionCheck", buildVersionCheckFragment(event, (Ref) event.getBranchPath().get(0), 0));

      innerDataQuery = wrapAndBranch(event, innerDataQuery, filterWhereClause, rootTableName, event.getBranchPath());

      query = new SQLQuery(
                            "SELECT * FROM (\n" +
                            "  ${{innerData}}\n" +
                            ") ${{datasetId}} ${{orderBy}} ${{limit}}\n")
          .withQueryFragment("innerData", innerDataQuery)
          .withQueryFragment("datasetId", "dataset" + event.getNodeId())
          .withQueryFragment("orderBy", buildOuterOrderByFragment(event)); //TODO: Check if belows "outerOrderBy" really has to be called like that
    }
    else {
      query = new SQLQuery(
          "SELECT ${{selectClause}} FROM ${schema}.${table} WHERE ${{filters}} ${{versionCheck}} ${{outerOrderBy}} ${{limit}}")
          .withQueryFragment("selectClause", buildSelectClause(event, 0, 0))
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
    if (event instanceof SelectiveEvent) {
      SelectiveEvent selectiveEvent = (SelectiveEvent) event;
      if (selectiveEvent.getRef().isAllVersions())
        return new SQLQuery("");
    }

    String tableVariable = isL2 ? "intermediateExtensionTable" : "table";
    return new SQLQuery("WHERE ${{exists}} exists(SELECT 1 FROM ${schema}.${" + tableVariable + "} WHERE ${{idComparison}})")
        .withQueryFragment("exists", event.getContext() == COMPOSITE_EXTENSION && !isL2 ? "" : "NOT")
        .withQueryFragment("idComparison", idComparisonFragment);
  }

  protected SQLQuery buildBranchCompositionFilter() {
    return new SQLQuery("WHERE NOT EXISTS (SELECT 1 FROM ${schema}.${table} WHERE id = ${{datasetId}}.id ${{versionCheck}})");
  }

  private SQLQuery wrapAndBranch(E event, SQLQuery baseQuery, SQLQuery filterWhereClause, String rootTableName, List<Ref> remainingBranchPath) {
    boolean isTopLevelBranch = remainingBranchPath.size() == 1;
    Ref successorBaseRef = isTopLevelBranch ? null : remainingBranchPath.get(1);
    int nodeId = isTopLevelBranch ? event.getNodeId() : getNodeId(successorBaseRef);
    Ref baseRef = remainingBranchPath.get(0);
    int baseNodeId = getNodeId(baseRef);
    String branchTable = branchTableName(rootTableName, baseRef, nodeId);

    SQLQuery branchDataQuery = new SQLQuery("SELECT ${{selectClause}} FROM ${schema}.${table} WHERE ${{filters}} ${{versionCheck}}")
        .withVariable(TABLE, branchTable)
        .withQueryFragment("selectClause", buildSelectClause(event, nodeId, baseRef.getVersion()))
        .withQueryFragment("filters", buildFiltersFragment(event, false, filterWhereClause, nodeId))
        .withQueryFragment("versionCheck", buildVersionCheckFragment(event, successorBaseRef, baseRef.getVersion()));

    SQLQuery wrappedBranchAndBaseQuery = new SQLQuery(
        "(${{branchData}})\n" +
        "UNION ALL\n" +
        "(\n" +
        "  SELECT * FROM (${{base}}) ${{datasetId}} ${{branchCompositionFilter}}\n" +
        ")\n")
        .withQueryFragment("branchData", branchDataQuery)
        .withQueryFragment("base", baseQuery)
        .withQueryFragment("datasetId", "dataset" + baseNodeId)
        .withQueryFragment("branchCompositionFilter", buildBranchCompositionFilter())
        .withVariable(TABLE, branchTable)
        .withQueryFragment("versionCheck", branchDataQuery.getQueryFragment("versionCheck"));

    return isTopLevelBranch
        ? wrappedBranchAndBaseQuery
        : wrapAndBranch(event, wrappedBranchAndBaseQuery, filterWhereClause, rootTableName,
            remainingBranchPath.subList(1, remainingBranchPath.size()));
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

  protected SQLQuery buildSelectClause(E event, int dataset, long baseVersion) {
    SQLQuery jsonDataWithVersion = new SQLQuery("jsonb_set(${{innerJsonData}}, '{properties, @ns:com:here:xyz, version}', to_jsonb(${{featureVersion}} + ${{baseVersion}}::BIGINT)) AS jsondata")
        .withQueryFragment("innerJsonData", buildJsonDataFragment(event))
        .withQueryFragment("baseVersion", "" + baseVersion) //TODO: That's a workaround for a minor bug in SQLQuery
        .withQueryFragment("featureVersion", isCompositeQuery(event) ? getFeatureVersion(event, dataset) : "version");


    if(getTableLayout().equals(ConnectorParameters.TableLayout.NEW_LAYOUT)) {
      jsonDataWithVersion = new SQLQuery("author, ${{innerJsonData}} AS jsondata")
              .withQueryFragment("innerJsonData", buildJsonDataFragment(event))
              .withQueryFragment("baseVersion", "" + baseVersion) //TODO: That's a workaround for a minor bug in SQLQuery
              .withQueryFragment("featureVersion", isCompositeQuery(event) ? getFeatureVersion(event, dataset) : "version");
    }

    return new SQLQuery("id, ${{version}}, ${{jsonData}}, ${{geo}}, ${{dataset}}")
        .withQueryFragment("jsonData", jsonDataWithVersion)
        .withQueryFragment("geo", buildGeoFragment(event))
        .withQueryFragment("dataset", new SQLQuery("${{datasetNo}} AS dataset")
        .withQueryFragment("datasetNo", "" + dataset))
        .withQueryFragment("version", new SQLQuery("${{featureVersion}} + ${{baseVersion}}::BIGINT AS version")
            .withQueryFragment("featureVersion", isCompositeQuery(event) ? getFeatureVersion(event, dataset) : "version")
            .withQueryFragment("baseVersion", "" + baseVersion));
  }

  private SQLQuery buildVersionCheckFragment(E event) {
    return buildVersionCheckFragment(event, null, 0);
  }

  private SQLQuery buildVersionCheckFragment(E event, Ref successorBaseRef, long baseVersion) {
    Ref ref = successorBaseRef;
    //In case this is not a branching-query simply take the requested ref from the event
    //FIXME: Also is null if this method is called for the most top branch, in that case the event ref also has to be taken into account
    if (ref == null)
      ref = event.getRef();
    //In case the requested version is smaller than the successor base version, take the smaller one.
    else if (!event.getRef().isHead() && event.getRef().isSingleVersion() && event.getRef().getVersion() < ref.getVersion())
      ref = event.getRef();
    //In case the requested ref is a range ...
    else if (event.getRef().isRange()) {
      //... and partially overlaps with the version range of the branch ...
      if (event.getRef().getStart().getVersion() < ref.getVersion())
        //... shrink the range to the overlapping part
        ref = new Ref(event.getRef().getStart(), event.getRef().getEnd().isHead() || event.getRef().getEnd().getVersion() > ref.getVersion() ? ref : event.getRef().getEnd());
      else
        //... otherwise the range has no overlap with the branch so pass the "empty-range" ref
        ref = new Ref(successorBaseRef, successorBaseRef);
    }

    if (!(event instanceof SelectiveEvent))
      return new SQLQuery("");

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    return new SQLQuery("${{versionComparison}} ${{nextVersion}} ${{minVersion}}")
        .withQueryFragment("versionComparison", buildVersionComparison(selectiveEvent, ref, baseVersion))
        .withQueryFragment("nextVersion", buildNextVersionFragment(selectiveEvent, ref, baseVersion))
        .withQueryFragment("minVersion", buildMinVersionFragment(selectiveEvent, baseVersion));
  }

  private SQLQuery buildVersionComparison(SelectiveEvent event, Ref ref, long baseVersion) {
    if (event.getVersionsToKeep() == 1 || ref.isAllVersions() || ref.isHead())
      return new SQLQuery("");

    if (ref.isRange())
      return new SQLQuery("AND version > ${{startVersion}}::BIGINT ${{endVersionCheck}}")
          .withQueryFragment("startVersion", "" + (ref.getStart().getVersion() - baseVersion)) //TODO: That's a workaround for a minor bug in SQLQuery
          .withQueryFragment("endVersionCheck", ref.getEnd().isHead()
              ? new SQLQuery("")
              : new SQLQuery("AND version <= ${{endVersion}}::BIGINT")
                  .withQueryFragment("endVersion", "" + (ref.getEnd().getVersion() - baseVersion))); //TODO: That's a workaround for a minor bug in SQLQuery

    return new SQLQuery("AND version <= ${{requestedVersion}}::BIGINT")
        .withQueryFragment("requestedVersion", "" + (ref.getVersion() - baseVersion)); //TODO: That's a workaround for a minor bug in SQLQuery
  }

  private SQLQuery buildNextVersionFragment(SelectiveEvent event, Ref ref, long baseVersion) {
    return buildNextVersionFragment(ref, event.getVersionsToKeep() > 1, "requestedVersion", baseVersion);
  }

  protected SQLQuery buildNextVersionFragment(Ref ref, boolean historyEnabled, String versionParamName, long baseVersion) {
    if (!historyEnabled || ref.isAllVersions())
      return new SQLQuery("");

    boolean maxVersionIsHead = ref.isHead() || ref.isRange() && ref.getEnd().isHead();
    long maxVersion = maxVersionIsHead ? MAX_BIGINT : (ref.isRange() ? ref.getEnd().getVersion() : ref.getVersion()) - baseVersion;

    return new SQLQuery("AND next_version ${{op}} ${{" + versionParamName + "}}::BIGINT")
        .withQueryFragment("op", maxVersionIsHead ? "=" : ">")
        .withQueryFragment(versionParamName, "" + maxVersion); //TODO: That's a workaround for a minor bug in SQLQuery
  }

  private SQLQuery buildBaseVersionCheckFragment(String versionParamName) {
    /*
    Always assume HEAD version for base spaces and also assume history to be enabled,
    because from the even we don't know whether it is enabled or not.
     */
    return buildNextVersionFragment(new Ref(HEAD), true, versionParamName, 0);
  }

  private SQLQuery buildMinVersionFragment(SelectiveEvent event, long baseVersion) {
    Ref ref = event.getRef();
    boolean isHeadOrAllVersions = ref.isHead() || ref.isAllVersions() || ref.isRange() && ref.getEnd().isHead();
    long requestedVersion = isHeadOrAllVersions ? Long.MAX_VALUE : ref.isRange() ? ref.getEnd().getVersion() : ref.getVersion();

    if (event.getVersionsToKeep() > 1) {
      return new SQLQuery("AND greatest(#{minVersion}, (SELECT max(version) + ${{baseVersion}} - #{versionsToKeep} FROM ${schema}.${table})) <= ${{requestedVersion}}")
          .withNamedParameter("versionsToKeep", event.getVersionsToKeep())
          .withNamedParameter("minVersion", event.getMinVersion())
          .withQueryFragment("baseVersion", baseVersion + "::BIGINT") //TODO: That's a workaround for a minor bug in SQLQuery
          .withQueryFragment("requestedVersion", requestedVersion + "::BIGINT"); //TODO: That's a workaround for a minor bug in SQLQuery
    }

    return isHeadOrAllVersions || ref.isRange() ? new SQLQuery("") : new SQLQuery("AND ${{requestedVersion}} = (SELECT max(version) + ${{baseVersion}} AS HEAD FROM ${schema}.${table})")
        .withQueryFragment("requestedVersion", requestedVersion + "::BIGINT")
        .withQueryFragment("baseVersion", baseVersion + "::BIGINT"); //TODO: That's a workaround for a minor bug in SQLQuery
  }

  private SQLQuery buildAuthorCheckFragment(E event) {
    if (!(event instanceof SelectiveEvent))
      return new SQLQuery("");

    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    if (selectiveEvent.getAuthor() == null)
      return new SQLQuery("");

    return new SQLQuery(" AND author = #{author}")
        .withNamedParameter("author", selectiveEvent.getAuthor());
  }

  private SQLQuery build1LevelBaseQuery(E event, SQLQuery filterWhereClause) {
    int dataset = compositeDatasetNo(event, CompositeDataset.SUPER);
    return new SQLQuery("SELECT ${{selectClause}} FROM ${schema}.${extendedTable} WHERE ${{filters}} ${{versionCheck}} ${{orderBy}}")
        .withVariable("extendedTable", getExtendedTable(event))
        .withQueryFragment("selectClause", buildSelectClause(event, dataset, 0))
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
        .withQueryFragment("selectClause", buildSelectClause(event, dataset, 0))
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
    boolean shouldReturnEmpty = !historyEnabled && !isExtension;
    if (!shouldReturnEmpty && event instanceof SelectiveEvent) {
      SelectiveEvent selectiveEvent = (SelectiveEvent) event;
      if (selectiveEvent.getRef().isRange())
        shouldReturnEmpty = true;
    }
    if (shouldReturnEmpty)
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
    LazyParsableFeatureCollection fc = new LazyParsableFeatureCollection();

    while (rs.next()) {
      try {
        fc.addFeature(content -> handleFeature(rs, content));
      }
      catch (ErrorResponseException e) {
        //TODO: Throw ErrorResponseException instead
        return (R) e.getErrorResponse();
      }
    }

    return (R) fc.build();
  }

  protected void handleFeature(ResultSet rs, StringBuilder result) throws SQLException {
    String geom = rs.getString("geo");
    if(getTableLayout().equals(ConnectorParameters.TableLayout.OLD_LAYOUT))
      result.append(rs.getString("jsondata"));
    else if(getTableLayout().equals(ConnectorParameters.TableLayout.NEW_LAYOUT))
      result.append(injectValuesIntoNameSpace(rs.getString("jsondata"), rs.getLong("version"), rs.getString("author")));
    result.setLength(result.length() - 1);
    result.append(",\"geometry\":");
    result.append(geom == null ? "null" : geom);
    result.append("}");
  }

  private String injectValuesIntoNameSpace(String jsonData, long version, String author) {
    String namespacePattern = "(\"@ns:com:here:xyz\"\\s*:\\s*\\{)";
    //The NS always contains the updatedAt property, so we need a trailing comma.
    String versionAuthor = "$1\"version\":" + version + ",\"author\":\"" + author + "\",";

    return jsonData.replaceAll(namespacePattern, versionAuthor);
  }

  protected static class LazyParsableFeatureCollection {
    public static final String PREFIX = "[";
    public static final String SUFFIX = "]";

    private StringBuilder content = new StringBuilder().append(PREFIX);

    public void addFeature(FeatureAppender featureAppender) throws ErrorResponseException, SQLException {
      featureAppender.appendFeature(content);

      if( content.length() > PREFIX.length() && content.charAt(content.length() - 1) != ',' ) //prevent inserting additionals ',' in case featureAppender.appendFeature(content) does nop
       content = content.append(",");

      if (content.length() > MAX_RESULT_SIZE)
        throw new ErrorResponseException(PAYLOAD_TO_LARGE, "Maximum response char limit of " + MAX_RESULT_SIZE + " reached");
    }

    public FeatureCollection build() {
      final FeatureCollection featureCollection = new FeatureCollection();

      if (content.length() > PREFIX.length() && content.charAt(content.length() - 1) == ',' )
       content.setLength(content.length() - 1); //Removes the last extra comma

      featureCollection._setFeatures(content.append(SUFFIX).toString());
      return featureCollection;
    }

    @FunctionalInterface
    interface FeatureAppender {
      void appendFeature(StringBuilder content) throws ErrorResponseException, SQLException;
    }
  }

  protected SQLQuery buildJsonDataFragment(E event) {
    SQLQuery jsonData;

    if (!(event instanceof SelectiveEvent)) {
      jsonData = new SQLQuery("jsondata");
    }
    else {
      SelectiveEvent selectiveEvent = (SelectiveEvent) event;
      if (selectiveEvent.getSelection() == null) {
        jsonData = new SQLQuery("jsondata");
      }
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
    }
    return jsonData;
  }

  private String getFeatureVersion(E event, int dataset) {
    int topLevelDataset = compositeDatasetNo(event, CompositeDataset.EXTENSION);
    /*
    NOTE: From the perspective of a composite layer (requested with context = DEFAULT),
    the extended dataset(s) always have version = 0, because at the beginning the extension itself *is empty* and
    this *empty version (aka version 0)* is simply not empty but contains the whole HEAD
    of the dataset being extended (e.g., INTERMEDIATE and / or SUPER dataset).
     */
    return dataset < topLevelDataset ? "0" : "version";
  }

  protected String buildOuterOrderByFragment(ContextAwareEvent event) {
    return buildOrderByFragment(event);
  }

  protected String buildOrderByFragment(ContextAwareEvent event) {
    if (!(event instanceof SelectiveEvent))
      return "";
    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    return selectiveEvent.getRef().isAllVersions() ? "ORDER BY version" : "";
  }

  private boolean selectNoGeometry(E event) {
    if (!(event instanceof SelectiveEvent))
      return false;
    SelectiveEvent selectiveEvent = (SelectiveEvent) event;
    return selectiveEvent.getSelection() != null
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
    boolean isForce2D = false;
    if (event instanceof SelectiveEvent) {
      SelectiveEvent selectiveEvent = (SelectiveEvent) event;
      isForce2D = selectiveEvent.isForce2D();
    }
    return new SQLQuery((isForce2D ? "ST_Force2D" : "ST_Force3D") + "(geo)");
  }

  protected enum CompositeDataset {
    EXTENSION,
    INTERMEDIATE,
    SUPER
  }

  protected int compositeDatasetNo(E event, CompositeDataset dataset) {
    switch (dataset) {
      case EXTENSION:
        return !isCompositeQuery(event) ? 0 :  is2LevelExtendedSpace(event) ? 2 : isExtendedSpace(event) ? 1 : 0;
      case INTERMEDIATE:
        return 1;
      case SUPER:
        return 0;
      default:
        throw new IllegalStateException("Unexpected dataset: " + dataset);
    }
  }
}
