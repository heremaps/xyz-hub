package com.here.mapcreator.ext.naksha;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to gain access to the management database.
 */
public class NakshaManagementClient {

  public NakshaManagementClient(@NotNull PsqlManagementDataSource dataSource) {
    this.dataSource = dataSource;
  }

  protected static final Logger logger = LoggerFactory.getLogger(NakshaManagementClient.class);

  protected final @NotNull PsqlManagementDataSource dataSource;

}