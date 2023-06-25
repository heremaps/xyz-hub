package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.INaksha;
import java.io.IOException;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

/**
 * The abstract Naksha-Hub is the base class for the Naksha-Hub implementation, granting access to the administration PostgresQL database.
 * This is a special Naksha client, used to manage spaces, connectors, subscriptions and other administrative content. This client should
 * not be used to query data from a foreign storage, it only holds administrative spaces. Normally this is only created and used by the
 * Naksha-Hub itself and exposed to all other parts of the Naksha-Hub via the {@link INaksha#get()} method.
 */
public abstract class AbstractNakshaHub extends PsqlStorage implements INaksha {

  /**
   * Create a new Naksha client instance and register as default Naksha client.
   *
   * @param config the configuration of the admin-database to connect to.
   * @throws SQLException if any error occurred while accessing the database.
   * @throws IOException  if reading the SQL extensions from the resources fail.
   */
  protected AbstractNakshaHub(@NotNull PsqlConfig config) throws SQLException, IOException {
    super(config, 0L);
    instance.getAndSet(this);
  }
}
