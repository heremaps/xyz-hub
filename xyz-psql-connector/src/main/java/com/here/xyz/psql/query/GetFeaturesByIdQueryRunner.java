package com.here.xyz.psql.query;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetFeaturesByIdQueryRunner extends ExtendedSpaceQueryRunner<GetFeaturesByIdEvent, FeatureCollection> {

  public GetFeaturesByIdQueryRunner(GetFeaturesByIdEvent event, DatabaseHandler dbHandler) throws SQLException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetFeaturesByIdEvent event) throws SQLException {
    SQLQuery query;
    String[] idArray = event.getIds().toArray(new String[0]);

    if (isExtendedSpace(event) && event.getContext() == DEFAULT)
      query = buildExtensionQuery(event, "jsondata->>'id' = ANY(#{ids})");
    else {
      query = new SQLQuery("SELECT");
      query.append(SQLQuery.selectJson(event.getSelection()));
      query.append(
          ", replace(ST_AsGeojson(" + SQLQueryBuilder.getForceMode(event.isForce2D()) + "(geo)," + SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS
              + "),'nan','0') FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(#{ids})");

      query.setVariable(SCHEMA, getSchema());
      query.setVariable(TABLE, dbHandler.getConfig().readTableFromEvent(event));
    }

    query.setNamedParameter("ids", idArray);
    return query;
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    return dbHandler.defaultFeatureResultSetHandler(rs);
  }
}
