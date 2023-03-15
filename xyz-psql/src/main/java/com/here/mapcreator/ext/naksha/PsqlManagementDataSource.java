package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;

/**
 * The management data-source to the Naksha management database.
 */
public class PsqlManagementDataSource extends AbstractPsqlDataSource<PsqlManagementDataSource> {

  /**
   * Create a new data source for the given connection pool and application.
   *
   * @param pool            the connection pool to wrap.
   * @param applicationName the application name.
   */
  protected PsqlManagementDataSource(@NotNull PsqlPool pool, @NotNull String applicationName) {
    super(pool, applicationName);
  }
}
