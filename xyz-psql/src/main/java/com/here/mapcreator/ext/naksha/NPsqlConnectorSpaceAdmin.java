package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;

/**
 * The data-source to a Naksha admin schema of a connector.
 */
public class NPsqlConnectorSpaceAdmin extends APsqlDataSource<NPsqlConnectorSpaceAdmin> {

  /**
   * Create a new data source for the given connector configuration and application name.
   *
   * @param params          the PostgresQL connector parameters.
   * @param applicationName the application name.
   */
  public NPsqlConnectorSpaceAdmin(@NotNull NPsqlConnectorParams params, @NotNull String applicationName) {
    super(NPsqlPool.get(params.getDbConfig()), applicationName);
    this.connectorParams = params;
    setSchema(params.getAdminSchema());
    setRole(params.getAdminRole());
  }

  /**
   * The connector parameters used to create this data source.
   */
  public final @NotNull NPsqlConnectorParams connectorParams;
}
