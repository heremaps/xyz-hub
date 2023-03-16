package com.here.mapcreator.ext.naksha;

import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The common part of all clients, not specific to management, administration or storage.
 */
public class NakshaClient {

  protected static final Logger logger = LoggerFactory.getLogger(NakshaClient.class);

  public NakshaClient(@NotNull AbstractPsqlDataSource<?> dataSource) {
    this.dataSource = dataSource;
  }

  protected final @NotNull AbstractPsqlDataSource<?> dataSource;

  public <OBJ> @NotNull OBJ getRowById(@NotNull String id) {
    return null;
  }

  public void insertRow(@NotNull Object row) throws SQLException {
  }

  public void updateRow(@NotNull Object expected, @NotNull Object updated) throws SQLException {
  }

}