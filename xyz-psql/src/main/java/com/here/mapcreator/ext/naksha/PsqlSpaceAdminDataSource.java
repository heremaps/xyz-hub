package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;

/**
 * The data-source to a Naksha admin schema of a connector.
 */
public class PsqlSpaceAdminDataSource extends AbstractPsqlDataSource<PsqlSpaceAdminDataSource> {

  /**
   * Create a new data source for the given connector configuration and application name.
   *
   * @param params          the PostgresQL connector parameters.
   * @param applicationName the application name.
   */
  public PsqlSpaceAdminDataSource(@NotNull PsqlStorageParams params, @NotNull String applicationName) {
    super(PsqlPool.get(params.getDbConfig()), applicationName);
    this.connectorParams = params;
    setSchema(params.getAdminSchema());
    setRole(params.getAdminRole());
  }

  /**
   * The connector parameters used to create this data source.
   */
  public final @NotNull PsqlStorageParams connectorParams;
}