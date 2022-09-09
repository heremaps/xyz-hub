package com.here.xyz.psql.query;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class LoadFeaturesQueryRunner extends ExtendedSpaceQueryRunner<LoadFeaturesEvent, FeatureCollection> {

  public LoadFeaturesQueryRunner(LoadFeaturesEvent event, DatabaseHandler dbHandler) throws SQLException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(LoadFeaturesEvent event) throws SQLException {
    final Map<String, String> idMap = event.getIdsMap();

    final ArrayList<String> ids = new ArrayList<>(idMap.size());
    ids.addAll(idMap.keySet());

    final ArrayList<String> uuids = new ArrayList<>(idMap.size());
    uuids.addAll(idMap.values());

    final String[] idArray = ids.toArray(new String[0]);
    SQLQuery query;

    if (isExtendedSpace(event) && event.getContext() == DEFAULT)
      query = buildExtensionQuery(event, "jsondata->>'id' = ANY(#{ids})");
    else {
      if(!event.getEnableHistory() || uuids.size() == 0 )
        query = new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+ SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(#{ids})");
      else{
        final boolean compactHistory = !event.getEnableGlobalVersioning() && dbHandler.getConfig().getConnectorParams().isCompactHistory();
        final String[] uuidArray = uuids.toArray(new String[0]);

        if(compactHistory){
          //History does not contain Inserts
          query = new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table}");
          query.append("WHERE jsondata->>'id' = ANY(#{ids})");
          query.append("UNION ");
          query.append("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${hsttable} h ");
          query.append("WHERE uuid = ANY(#{uuids}) ");
          query.append("AND EXISTS(select 1 from${schema}.${table} t where t.jsondata->>'id' =  h.jsondata->>'id') ");
        }else{
          //History does contain Inserts
          query = new SQLQuery("SELECT DISTINCT ON(jsondata->'properties'->'@ns:com:here:xyz'->'uuid') * FROM(");
          query.append("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table}");
          query.append("WHERE jsondata->>'id' = ANY(#{ids})");
          query.append("UNION ");
          query.append("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${hsttable} h ");
          query.append("WHERE uuid = ANY(#{uuids}) ");
          query.append("AND EXISTS(select 1 from${schema}.${table} t where t.jsondata->>'id' =  h.jsondata->>'id') ");
          query.append(")A");
        }
        query.setNamedParameter("uuids", uuidArray);

        String table = dbHandler.getConfig().readTableFromEvent(event);
        query.setVariable(SCHEMA, getSchema());
        query.setVariable(TABLE, table);
        query.setVariable("hsttable", table + DatabaseHandler.HISTORY_TABLE_SUFFIX);
      }
    }
    query.setNamedParameter("ids", idArray);
    return query;
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    return dbHandler.defaultFeatureResultSetHandler(rs);
  }
}
