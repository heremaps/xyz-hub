package com.here.xyz.psql.query;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;

public class GetFeaturesByBBox<E extends GetFeaturesByBBoxEvent> extends ExtendedSpace<E> {

  public GetFeaturesByBBox(E event, DatabaseHandler dbHandler) throws SQLException {
    super(event, dbHandler, false);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException {
    //NOTE: So far this query runner only handles queries regarding extended spaces
    if (isExtendedSpace(event) && event.getContext() == DEFAULT) {
      String filterWhereClause = "ST_Intersects(geo, ST_MakeEnvelope(#{minLon}, #{minLat}, #{maxLon}, #{maxLat}, 4326))";
      SQLQuery query = buildExtensionQuery(event, filterWhereClause);

      final BBox bbox = event.getBbox();
      query.setNamedParameter("minLon", bbox.minLon());
      query.setNamedParameter("minLat", bbox.minLat());
      query.setNamedParameter("maxLon", bbox.maxLon());
      query.setNamedParameter("maxLat", bbox.maxLat());
      return query;
    }
    return null;
  }
}
