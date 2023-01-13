package com.here.xyz.psql.query.helpers;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.helpers.TableExists.Table;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableExists extends QueryRunner<Table, Boolean> {

  public TableExists(Table table, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(table, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(Table table) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT FROM pg_tables WHERE schemaname = #{schema} AND tablename = #{tableName}")
        .withNamedParameter("schema", table.schema)
        .withNamedParameter("tableName", table.tableName);
  }

  @Override
  public Boolean handle(ResultSet rs) throws SQLException {
    return rs.next();
  }

  public static class Table {
    private String schema;
    private String tableName;

    public Table(String schema, String tableName) {
      this.schema = schema;
      this.tableName = tableName;
    }
  }
}
