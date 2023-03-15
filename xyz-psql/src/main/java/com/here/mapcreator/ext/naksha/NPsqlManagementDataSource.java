package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;

/**
 * The management data-source to the Naksha management database.
 */
public class NPsqlManagementDataSource extends APsqlDataSource<NPsqlManagementDataSource> {

  /**
   * Create a new data source for the given connection pool and application.
   *
   * @param pool            the connection pool to wrap.
   * @param applicationName the application name.
   */
  protected NPsqlManagementDataSource(@NotNull NPsqlPool pool, @NotNull String applicationName) {
    super(pool, applicationName);
  }
}
