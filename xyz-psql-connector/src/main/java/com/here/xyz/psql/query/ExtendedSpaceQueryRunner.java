package com.here.xyz.psql.query;

import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;
import java.util.Map;

public abstract class ExtendedSpaceQueryRunner<E extends Event, R extends XyzResponse> extends QueryRunner<E, R> {

  protected static final String EXTENDS = "extends";
  protected static final String SPACE_ID = "spaceId";

  public ExtendedSpaceQueryRunner(E event, DatabaseHandler dbHandler) throws SQLException {
    super(event, dbHandler);
    setUseReadReplica(true);
  }

  protected boolean isExtendedSpace(E event) {
    return event.getParams() != null && event.getParams().containsKey(EXTENDS);
  }

  protected SQLQuery buildExtensionQuery(E event, String filterWhereClause) {
    PSQLConfig config = dbHandler.getConfig();

    SQLQuery extensionQuery = new SQLQuery(
        "SELECT jsondata, ${{geo}}"
        + "    FROM ${schema}.${extensionTable}"
        + "    WHERE ${{filterWhereClause}} AND deleted = false "
        + "UNION ALL "
        + "    SELECT jsondata, ${{geo}} FROM"
        + "        ("
        + "            ${{baseQuery}}"
        + "        ) a WHERE NOT exists(SELECT 1 FROM ${schema}.${extensionTable} b WHERE jsondata->>'id' = a.jsondata->>'id')");

    extensionQuery.setQueryFragment("geo", buildGeoFragment(event));
    extensionQuery.setQueryFragment("filterWhereClause", filterWhereClause);
    extensionQuery.setVariable(SCHEMA, getSchema());
    extensionQuery.setVariable("extensionTable", config.readTableFromEvent(event));

    Map<String, Object> extSpec = (Map<String, Object>) event.getParams().get(EXTENDS);
    SQLQuery baseQuery;
    if (!extSpec.containsKey(EXTENDS)) {
      //1-level extension
      String extendedTable = config.getTableNameForSpaceId((String) extSpec.get(SPACE_ID));
      baseQuery = build1LevelBaseQuery(extendedTable);
    }
    else {
      //2-level extension
      Map<String, Object> baseExtSpec = (Map<String, Object>) extSpec.get(EXTENDS);
      String intermediateTable = config.getTableNameForSpaceId((String) extSpec.get(SPACE_ID));
      String extendedTable = config.getTableNameForSpaceId((String) baseExtSpec.get(SPACE_ID));
      baseQuery = build2LevelBaseQuery(intermediateTable, extendedTable);
    }
    baseQuery.setQueryFragment("filterWhereClause", filterWhereClause);
    extensionQuery.setQueryFragment("baseQuery", baseQuery);

    return extensionQuery;
  }

  private SQLQuery build1LevelBaseQuery(String extendedTable) {
    SQLQuery query = new SQLQuery("SELECT jsondata, geo"
        + "    FROM ${schema}.${extendedTable} m"
        + "    WHERE ${{filterWhereClause}}"); //in the base table there is no need to check a deleted flag;
    query.setVariable("extendedTable", extendedTable);
    return query;
  }

  private SQLQuery build2LevelBaseQuery(String intermediateTable, String extendedTable) {
    SQLQuery query = new SQLQuery("SELECT jsondata, geo"
        + "    FROM ${schema}.${intermediateExtensionTable}"
        + "    WHERE ${{filterWhereClause}} AND deleted = false "
        + "UNION ALL"
        + "    SELECT jsondata, geo FROM"
        + "        ("
        + "            ${{innerBaseQuery}}"
        + "        ) b WHERE NOT exists(SELECT 1 FROM ${schema}.${intermediateExtensionTable} WHERE jsondata->>'id' = b.jsondata->>'id')");
    query.setVariable("intermediateExtensionTable", intermediateTable);
    query.setQueryFragment("innerBaseQuery", build1LevelBaseQuery(extendedTable));
    return query;
  }

  private String buildGeoFragment(E event) {
    boolean isForce2D = false;
    if (event instanceof GetFeaturesByIdEvent)
      isForce2D = ((GetFeaturesByIdEvent) event).isForce2D();
    else if (event instanceof SearchForFeaturesEvent)
      isForce2D = ((SearchForFeaturesEvent<?>) event).isForce2D();

    return "replace(ST_AsGeojson(" + (isForce2D ? "ST_Force2D" : "ST_Force3D") + "(geo), " + SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0')";
  }
}
