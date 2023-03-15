package com.here.mapcreator.ext.naksha;

import com.here.xyz.models.hub.psql.PsqlProcessorParams;
import org.jetbrains.annotations.NotNull;

/**
 * The data-source to a Naksha admin schema of a connector.
 */
public class PsqlConnectorSpaceAdmin extends AbstractPsqlDataSource<PsqlConnectorSpaceAdmin> {

  /**
   * Create a new data source for the given connector configuration and application name.
   *
   * @param params          the PostgresQL connector parameters.
   * @param applicationName the application name.
   */
  public PsqlConnectorSpaceAdmin(@NotNull PsqlProcessorParams params, @NotNull String applicationName) {
    super(PsqlPool.get(params.getDbConfig()), applicationName);
    this.connectorParams = params;
    setSchema(params.getAdminSchema());
    setRole(params.getAdminRole());
  }

  /**
   * The connector parameters used to create this data source.
   */
  public final @NotNull PsqlProcessorParams connectorParams;
}
