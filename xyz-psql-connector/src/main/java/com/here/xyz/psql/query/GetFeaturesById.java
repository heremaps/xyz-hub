package com.here.xyz.psql.query;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;

public class GetFeaturesById extends ExtendedSpace<GetFeaturesByIdEvent> {

  public GetFeaturesById(GetFeaturesByIdEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetFeaturesByIdEvent event) {
    String[] idArray = event.getIds().toArray(new String[0]);
    String filterWhereClause = "jsondata->>'id' = ANY(#{ids})";

    SQLQuery query = isExtendedSpace(event) && event.getContext() == DEFAULT
        ? buildExtensionQuery(event, filterWhereClause)
        : buildQuery(event, filterWhereClause);

    //query.setQueryFragment("filterWhereClause", filterWhereClause);
    query.setNamedParameter("ids", idArray);
    return query;
  }
}
