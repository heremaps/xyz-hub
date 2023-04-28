package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.psql.PsqlEventHandler;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQueryExt;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

/**
 * This class provides the utility to run a single database query which is described by an XYZ {@link Event}
 * and which returns an {@link XyzResponse}.
 * It has the internal capability to build & run the necessary {@link SQLQueryExt} and translate
 * the resulting {@link ResultSet} into an {@link XyzResponse}.
 * @param <E> The event type
 * @param <R> The response type
 */
public abstract class XyzQueryRunner<E extends Event, R extends XyzResponse> extends QueryRunner<E, R> {

  public XyzQueryRunner(E event, final @NotNull PsqlEventHandler psqlConnector)
      throws SQLException, ErrorResponseException {
    super(event, psqlConnector);
  }

}
