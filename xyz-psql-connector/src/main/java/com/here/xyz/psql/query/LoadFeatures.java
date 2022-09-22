package com.here.xyz.psql.query;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoadFeatures extends ExtendedSpace<LoadFeaturesEvent> {

  public LoadFeatures(LoadFeaturesEvent event, DatabaseHandler dbHandler) throws SQLException {
    super(event, dbHandler,false);
  }

  @Override
  protected SQLQuery buildQuery(LoadFeaturesEvent event) throws SQLException {
    final Map<String, String> idMap = event.getIdsMap();

    final ArrayList<String> ids = new ArrayList<>(idMap.keySet());
    final ArrayList<String> uuids = new ArrayList<>(idMap.values());

    String filterWhereClause = "jsondata->>'id' = ANY(#{ids})";
    SQLQuery query;

    if (isExtendedSpace(event) && event.getContext() == DEFAULT)
      query = buildExtensionQuery(event, filterWhereClause);
    else {
      if (!event.getEnableHistory() || uuids.size() == 0)
        query = buildQuery(event, filterWhereClause);
      else {
        final boolean compactHistory = !event.getEnableGlobalVersioning() && dbHandler.getConfig().getConnectorParams().isCompactHistory();
        if (compactHistory)
          //History does not contain Inserts
          query = new SQLQuery("${{headQuery}} UNION ${{historyQuery}}");
        else
          //History does contain Inserts
          query = new SQLQuery("SELECT DISTINCT ON(jsondata->'properties'->'@ns:com:here:xyz'->'uuid') * FROM("
              + "    ${{headQuery}} UNION ${{historyQuery}}"
              + ")A");

        query.setQueryFragment("headQuery", buildQuery(event, filterWhereClause));
        query.setQueryFragment("historyQuery", buildHistoryQuery(event, uuids));
      }
    }
    query.setNamedParameter("ids", ids.toArray(new String[0]));
    return query;
  }

  private SQLQuery buildHistoryQuery(LoadFeaturesEvent event, List<String> uuids) {
    SQLQuery historyQuery = new SQLQuery("SELECT jsondata, ${{geo}} "
        + "FROM ${schema}.${hsttable} h "
        + "WHERE uuid = ANY(#{uuids}) AND EXISTS("
        + "    SELECT 1"
        + "    FROM ${schema}.${table} t"
        + "    WHERE t.jsondata->>'id' =  h.jsondata->>'id'"
        + ")");
    historyQuery.setQueryFragment("geo", buildGeoFragment(event));
    historyQuery.setNamedParameter("uuids", uuids.toArray(new String[0]));
    historyQuery.setVariable("hsttable", dbHandler.getConfig().readTableFromEvent(event) + DatabaseHandler.HISTORY_TABLE_SUFFIX);
    return historyQuery;
  }
}
