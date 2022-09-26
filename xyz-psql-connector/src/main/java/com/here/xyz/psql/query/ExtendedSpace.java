package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.PSQLConfig;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class ExtendedSpace<E extends Event> extends GetFeatures<E> {

  private static final String EXTENDS = "extends";
  private static final String SPACE_ID = "spaceId";
  private static final String INTERMEDIATE_TABLE = "intermediateTable";
  private static final String EXTENDED_TABLE = "extendedTable";

  public ExtendedSpace(E event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  protected boolean isExtendedSpace(E event) {
    return event.getParams() != null && event.getParams().containsKey(EXTENDS);
  }

  protected Map<String,String> getExtendedTableNames(E event) {
    Map<String, Object> extSpec = event.getParams() != null ? (Map<String, Object>) event.getParams().get(EXTENDS) : null;
    Map<String, String> extendedTables = new HashMap<>();
    PSQLConfig config = dbHandler.getConfig();

    if (extSpec == null)
      return null;

    if (!extSpec.containsKey(EXTENDS))
      //1-level extension
      extendedTables.put(EXTENDED_TABLE, config.getTableNameForSpaceId((String) extSpec.get(SPACE_ID)));
    else {
      //2-level extension
      Map<String, Object> baseExtSpec = (Map<String, Object>) extSpec.get(EXTENDS);
      extendedTables.put(INTERMEDIATE_TABLE, config.getTableNameForSpaceId((String) extSpec.get(SPACE_ID)));
      extendedTables.put(EXTENDED_TABLE, config.getTableNameForSpaceId((String) baseExtSpec.get(SPACE_ID)));
    }
    return extendedTables;
  }

  protected SQLQuery buildExtensionQuery(E event, String filterWhereClause) {
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
    extensionQuery.setVariable("extensionTable", getDefaultTable(event));

    Map<String, String> extendedTables = getExtendedTableNames(event);
    SQLQuery baseQuery;
    if (extendedTables.get(INTERMEDIATE_TABLE) == null) {
      //1-level extension
      baseQuery = build1LevelBaseQuery(extendedTables.get(EXTENDED_TABLE));
    }
    else {
      //2-level extension
      baseQuery = build2LevelBaseQuery(extendedTables.get(INTERMEDIATE_TABLE), extendedTables.get(EXTENDED_TABLE));
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
}
