package com.here.mapcreator.ext.naksha;

import static com.here.mapcreator.ext.naksha.sql.MaintenanceSQL.XYZ_CONFIG;

import com.here.mapcreator.ext.naksha.sql.MaintenanceSQL;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.psql.PsqlStorageParams;
import com.here.xyz.models.hub.psql.PsqlPoolConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Naksha PostgresQL management class.
 */
public final class Naksha {

  Naksha() {
  }

  private static final Logger logger = LoggerFactory.getLogger(Naksha.class);

  /**
   * The schema into which to install the Naksha SQL extension on the management database.
   */
  public static final @NotNull String MGMT_SCHEMA = "naksha_mgmt";

  /**
   * The default schema into which to install the Naksha SQL extension on space databases.
   */
  public static final @NotNull String ADMIN_SCHEMA = "naksha_admin";

  /**
   * The default schema into which to create new spaces.
   */
  public static final @NotNull String SPACE_SCHEMA = "naksha_spaces";

  // Only needed in the management database.
  public static final @NotNull String MGMT_SPACE_TABLE = "xyz_space";
  public static final @NotNull String MGMT_STORAGE_TABLE = "xyz_storage";
  public static final @NotNull String MGMT_TXN_PUB_TABLE = "xyz_txn_pub";
  // Only needed in the space database.
  public static final @NotNull String SPACE_TXN_TABLE = "transactions";

  /**
   * The default search path to be used with the Naksha database.
   */
  public static final @NotNull String NAKSHA_SEARCH_PATH = ADMIN_SCHEMA + ",h3,topology,public;";

  static final int XYZ_EXT_VERSION = 148;

  static final int H3_CORE_VERSION = 107;

  @SuppressWarnings("PointlessBitwiseExpression")
  static final long NAKSHA_EXT_VERSION = 1L << 32 | 0L << 16 | 0L;

  /**
   * Set the management configuration and application name. Normally called by the Naksha host and read from the Naksha configuration file.
   *
   * @param config          The configuration of the Naksha management database.
   * @param applicationName The name of the Naksha instance.
   * @throws SQLException if any error occurred.
   */
  public static synchronized void init(@NotNull PsqlPoolConfig config, @NotNull String applicationName) throws SQLException {
    if (initialized) {
      if (Naksha.managementPsqlPool.config != config) {
        throw new SQLException("Naksha.init has already been called");
      }
      Naksha.applicationName = applicationName;
      return;
    }
    Naksha.initialized = true;
    Naksha.applicationName = applicationName;
    Naksha.threadGroup = new ThreadGroup("Naksha");
    Naksha.managementPsqlPool = PsqlPool.get(config);
    Naksha.managementDataSource = new PsqlManagementDataSource(Naksha.managementPsqlPool, applicationName).withSchema(ADMIN_SCHEMA);
    Naksha.runMaintenanceThread();
  }

  static volatile boolean initialized;
  static ThreadGroup threadGroup;
  static PsqlPool managementPsqlPool;
  static PsqlManagementDataSource managementDataSource;
  static String applicationName;

  /**
   * Returns the management pool of Naksha.
   *
   * @return the management pool of Naksha.
   * @throws SQLException if {@link #init(PsqlPoolConfig, String)} has not been called.
   */
  public static @NotNull PsqlPool managementPsqlPool() throws SQLException {
    final PsqlPool pool = Naksha.managementPsqlPool;
    if (pool == null) {
      throw new SQLException("Naksha.init not called");
    }
    return pool;
  }

  /**
   * Returns the management data-source of Naksha.
   *
   * @return the management data-source of Naksha.
   * @throws SQLException if {@link #init(PsqlPoolConfig, String)} has not been called.
   */
  public static @NotNull PsqlManagementDataSource managementDataSource() throws SQLException {
    final PsqlManagementDataSource dataSource = Naksha.managementDataSource;
    if (dataSource == null) {
      throw new SQLException("Naksha.init not called");
    }
    return dataSource;
  }

  /**
   * Returns the application name of Naksha.
   *
   * @return the application name of Naksha.
   * @throws SQLException if {@link #init(PsqlPoolConfig, String)} has not been called.
   */
  public static @NotNull String applicationName() throws SQLException {
    final String applicationName = Naksha.applicationName;
    if (applicationName == null) {
      throw new SQLException("Naksha.init not called");
    }
    return applicationName;
  }

  private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()"; // TODO: Why to we need this?
  private static final String[] extensionList = new String[]{"postgis", "postgis_topology", "tsm_system_rows", "dblink"};
  private static final String[] localScripts = new String[]{"/xyz_ext.sql", "/h3Core.sql", "/naksha_ext.sql"};
  private static final boolean[] ESCAPE = new boolean[128];

  static {
    Arrays.fill(ESCAPE, true);
    for (int c = '0'; c <= '9'; c++) {
      ESCAPE[c] = false;
    }
    for (int c = 'a'; c <= 'z'; c++) {
      ESCAPE[c] = false;
    }
    for (int c = 'A'; c <= 'Z'; c++) {
      ESCAPE[c] = false;
    }
    ESCAPE['_'] = false;
  }

  /**
   * Tests if the given identifier must be escaped.
   *
   * @param id the identifier to test.
   * @return true if the identifier must be escaped; false otherwise.
   */
  public static boolean shouldEscape(@NotNull CharSequence id) {
    for (int i = 0; i < id.length(); i++) {
      final char c = id.charAt(i);
      // We signal that every less than the space must be escaped. The escape method then will throw an SQLException!
      if (c < 32 || c > 126 || ESCAPE[c]) {
        return true;
      }
    }
    return false;
  }

  private static void escapeWrite(@NotNull CharSequence chars, @NotNull StringBuilder sb) throws SQLException {
    // See: https://www.asciitable.com/
    // We only allows characters between 32 (space) and 126 (~).
    for (int i = 0; i < chars.length(); i++) {
      final char c = chars.charAt(i);
      if (c < 32 || c > 126) {
        throw new SQLException("Illegal character in identifier: " + chars);
      }
      if (c == '"') {
        sb.append('"').append('"');
      } else {
        sb.append(c);
      }
    }
  }

  public static @NotNull StringBuilder escapeId(@NotNull CharSequence id, @NotNull StringBuilder sb) throws SQLException {
    sb.append('"');
    escapeWrite(id, sb);
    sb.append('"');
    return sb;
  }

  public static @NotNull StringBuilder escapeId(
      @NotNull CharSequence a,
      @NotNull CharSequence b,
      @NotNull StringBuilder sb) throws SQLException {
    sb.append('"');
    escapeWrite(a, sb);
    escapeWrite(b, sb);
    sb.append('"');
    return sb;
  }

  public static @NotNull StringBuilder escapeId(
      @NotNull CharSequence a,
      @NotNull CharSequence b,
      @NotNull CharSequence c,
      @NotNull StringBuilder sb) throws SQLException {
    sb.append('"');
    escapeWrite(a, sb);
    escapeWrite(b, sb);
    escapeWrite(c, sb);
    sb.append('"');
    return sb;
  }

  /**
   * Ensure that the admin tables are in a good state in the space database. This method does as well install the database identifier into
   * the target schema, so that the automatic generation of UUIDs works. Note that the same database can be shared with multiple connectors,
   * in that case all connectors must have the same database identifier.
   *
   * @param dataSource The data-source for the database of the connector that should be maintained.
   * @throws SQLException If any error occurred.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  public static void ensureSpaceDb(@NotNull PsqlSpaceAdminDataSource dataSource) throws SQLException, IOException {
    final StringBuilder sb = NakshaThreadLocal.get().sb();
    String SQL;

    final PsqlStorageParams connectorParams = dataSource.connectorParams;
    sb.setLength(0);
    final String ADMIN_SCHEMA = escapeId(connectorParams.getAdminSchema(), sb).toString();
    sb.setLength(0);
    final String SPACE_SCHEMA = escapeId(connectorParams.getSpaceSchema(), sb).toString();

    try (final Connection conn = dataSource.getConnection()) {
      SQL = "CREATE EXTENSION IF NOT EXISTS btree_gist SCHEMA public;\n" +
          "CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;\n" +
          "CREATE EXTENSION IF NOT EXISTS postgis_topology SCHEMA topology;\n" +
          "CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;\n";
      try (final Statement stmt = conn.createStatement()) {
        stmt.execute(SQL);
        conn.commit();
      }

      sb.setLength(0);
      sb.append("CREATE SCHEMA IF NOT EXISTS ").append(ADMIN_SCHEMA).append(";\n");
      sb.append("CREATE SCHEMA IF NOT EXISTS ").append(SPACE_SCHEMA).append(";\n");
      SQL = sb.toString();
      try (final Statement stmt = conn.createStatement()) {
        stmt.execute(SQL);
        conn.commit();
      }

      // Install extensions.
      try (final Statement stmt = conn.createStatement()) {
        SQL = readResource("/xyz_ext.sql");
        stmt.execute(SQL);
        conn.commit();

        SQL = readResource("/naksha_ext.sql");
        stmt.execute(SQL);
        conn.commit();
      }

      // Creates the transaction table.
      SQL = "SELECT 1 FROM naksha_tx_ensure();";
      try (final Statement stmt = conn.createStatement()) {
        stmt.execute(SQL);
        conn.commit();
      }

//      stmt.execute(MaintenanceSQL.createIDXTableSQL);
//      stmt.execute(MaintenanceSQL.createDbStatusTable);
//      stmt.execute(MaintenanceSQL.createSpaceMetaTable);
//      stmt.execute(MaintenanceSQL.createTxnPubTableSQL);
//      setupH3(dataSource, appName);
    }
  }

  /**
   * Ensures that the given table is set up correctly, so triggers and all.
   *
   * @param dataSource the data-source configured for administrative tasks.
   * @param spaceId    the space identifier.
   * @param table      the table name of the space.
   */
  public static void ensureSpace(@NotNull PsqlSpaceAdminDataSource dataSource, @NotNull String spaceId, @NotNull String table) {

  }

  /**
   * Ensures that the given table is set up correctly, so triggers and all.
   *
   * @param dataSource the data-source configured for administrative tasks.
   * @param spaceId    the space identifier.
   * @param table      the table name of the space.
   */
  public static void ensureSpaceWithHistory(@NotNull PsqlSpaceAdminDataSource dataSource, @NotNull String spaceId, @NotNull String table) {

  }

  /**
   * Ensures that the given table is set up correctly, so triggers and all.
   *
   * @param dataSource the data-source configured for administrative tasks.
   * @param spaceId    the space identifier.
   * @param table      the table name of the space.
   */
  public static void enableHistory(@NotNull PsqlSpaceAdminDataSource dataSource, @NotNull String spaceId, @NotNull String table) {

  }

  /**
   * Ensures that the given table is set up correctly, so triggers and all.
   *
   * @param dataSource the data-source configured for administrative tasks.
   * @param spaceId    the space identifier.
   * @param table      the table name of the space.
   */
  public static void disableHistory(@NotNull PsqlSpaceAdminDataSource dataSource, @NotNull String spaceId, @NotNull String table) {

  }

  /**
   * Setup H3?
   *
   * @param dataSource the data-source, but which (management-db or space-db)?
   * @throws SQLException if any error occurred.
   */
  public static void setupH3(@NotNull AbstractPsqlDataSource<?> dataSource) throws SQLException {
    final PsqlPoolConfig config = dataSource.getConfig();
    try (final Connection connection = dataSource.getConnection();
        final Statement stmt = connection.createStatement()) {
      boolean needUpdate = false;
      ResultSet rs;
      if ((rs = stmt.executeQuery(
          "select count(1)::integer from pg_catalog.pg_proc r inner join"
              + " pg_catalog.pg_namespace l  on ( r.pronamespace = l.oid ) where 1 = 1"
              + " and l.nspname = 'h3' and r.proname = 'h3_version'"))
          .next()) {
        needUpdate = (0 == rs.getInt(1));
      }

      if (!needUpdate && (rs = stmt.executeQuery("select h3.h3_version()")).next()) {
        needUpdate = (H3_CORE_VERSION > rs.getInt(1));
      }

      if (needUpdate) {
        stmt.execute(readResource("/h3Core.sql"));
        stmt.execute(MaintenanceSQL.generateSearchPathSQL(XYZ_CONFIG));
        logger.info("{} - Successfully created H3 SQL-Functions on database {}@{}", applicationName, config.user, config.url);
      }
    } catch (IOException e) {
      logger.error("{} - Failed run h3 init on database: {}@{}", applicationName, config.user, config.url, e);
      throw new SQLException("Failed to setup H3: " + e.getMessage());
    }
  }

  //
  // maintainSpaceSchema()
  // maintainSpace()
  // maintainTransactions()
  //

  public static @NotNull String readResource(@NotNull String resource) throws IOException {
    final InputStream is = ClassLoader.getSystemResourceAsStream(resource);
    assert is != null;
    try (final BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
      return buffer.lines().collect(Collectors.joining("\n"));
    }
  }

  // -----------------------------------------------------------------------------------------------------------------------------------
  // ---------- Static caching code
  // -----------------------------------------------------------------------------------------------------------------------------------

  private static final AtomicReference<Thread> maintenanceThreadRef = new AtomicReference<>();

  static void runMaintenanceThread() {
    Thread thread = maintenanceThreadRef.get();
    if (thread == null) {
      thread = new Thread(Naksha.threadGroup, Naksha::maintenance_run, Naksha.applicationName);
      if (maintenanceThreadRef.compareAndSet(null, thread)) {
        thread.setDaemon(true);
        thread.start();
      }
    }
  }

  private static void sleep(long time, @NotNull TimeUnit timeUnit) {
    try {
      Thread.sleep(timeUnit.toMillis(time));
    } catch (InterruptedException ignore) {
      //noinspection ResultOfMethodCallIgnored
      Thread.interrupted();
    }
  }

  private static void maintenance_run() {
    boolean naksha_ext_installed = false;
    while (true) {
      if (!naksha_ext_installed) {
        try {
          installNakshaExtension();
          naksha_ext_installed = true;
        } catch (Throwable t) {
          logger.error("{} - Failed to fix schema and tables", applicationName, t);
          sleep(15, TimeUnit.SECONDS);
          continue;
        }
      }

      try {
        final StringBuilder sb = NakshaThreadLocal.get().sb();
        String SQL;
        try (final Connection conn = managementDataSource.getConnection()) {
          sb.setLength(0);
          sb.append("SELECT id, owner, config, i FROM ").append(MGMT_SPACE_TABLE).append(" WHERE i > ?");
          SQL = sb.toString();
          long last = 0L;
          try (final PreparedStatement stmt = conn.prepareStatement(SQL)) {
            stmt.setLong(1, last);
            final ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
              final String id = rs.getString(1);
              final String owner = rs.getString(2);
              final String config = rs.getString(3);
              final long i = rs.getLong(4);
              try {
                final Space space = XyzSerializable.deserialize(config, Space.class);
                assert space != null;
                space.setId(id);
                space.setOwner(owner);
              } catch (Exception e) {
                logger.error("{} - Failed to deserialize space {}:{}, json: {}", applicationName, id, i, config, e);
              } finally {
                // This allows us to read spaces unsorted, we do not care in which order we received them, we just need to know .
                if (i > last) {
                  last = i;
                }
              }
            }
            conn.commit();
          } catch (SQLException e) {
            logger.error("{} - Failed to update spaces cache", applicationName, e);
          }
        }
      } catch (Throwable t) {
        logger.error("{} - Failed to update cache", applicationName, t);
      }

      // TODO: Fix this, so that we rather wait for notifications from the database.
      //       For this we need to add triggers to the
      try {
        //noinspection BusyWait
        Thread.sleep(30_000L);
      } catch (InterruptedException ignore) {
        //noinspection ResultOfMethodCallIgnored
        Thread.interrupted();
      }
    }
  }

  //
  // installExtensionIntoMgmt()
  //   SELECT naksha_mgmt_ensure(schema);
  // installExtensionIntoAdmin()
  //   SELECT naksha_admin_ensure(schema);
  //   SELECT naksha_spaces_ensure(schema, connector-id);
  // createNewSpace()
  //   SELECT naksha_table_ensure_with_history(schema, table);
  //   SELECT naksha_table_enable_history(schema, table);
  //
  // SELECT * FROM naksha_fetch_transaction(id); -- fetch all changes from this transaction (id sequential identifier)
  //

  private static void installNakshaExtension() throws SQLException {
    final StringBuilder sb = NakshaThreadLocal.get().sb();
    String SQL;

    sb.setLength(0);
    final String MGMT_SCHEMA = escapeId(managementDataSource.getSchema(), sb).toString();

    try (final Connection conn = managementDataSource.getConnection()) {
      sb.setLength(0);
      sb.append("CREATE TABLE IF NOT EXISTS ").append(MGMT_SCHEMA).append('.').append(MGMT_SPACE_TABLE).append(" (")
          .append(" id                TEXT PRIMARY KEY NOT NULL")
          .append(",owner             TEXT NOT NULL")
          .append(",config            JSONB NOT NULL")
          .append("i                  int8 NOT NULL")
          .append(");\n");
      sb.append("CREATE TABLE IF NOT EXISTS ").append(MGMT_SCHEMA).append(+'.').append(MGMT_STORAGE_TABLE).append(" (")
          .append(" id                TEXT PRIMARY KEY NOT NULL")
          .append(",owner             TEXT NOT NULL")
          .append(",cid               TEXT")
          .append(",config            JSONB NOT NULL")
          .append(",i int8            NOT NULL")
          .append(");\n");

      sb.append("CREATE TABLE IF NOT EXISTS ").append(MGMT_SCHEMA).append(+'.').append(MGMT_TXN_PUB_TABLE).append(" (")
          .append(" subscription_id   TEXT NOT NULL")
          .append(",last_txn_id       int8 NOT NULL")
          .append(",updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()")
          .append(");");
      sb.append("CREATE UNIQUE INDEX IF NOT EXISTS ");
      escapeId(MGMT_TXN_PUB_TABLE, "_id_idx", sb);
      sb.append(" ON ").append(MGMT_SCHEMA).append('.').append(MGMT_TXN_PUB_TABLE);
      sb.append(" USING btree (subscription_id);\n");

      SQL = sb.toString();
      try (final Statement stmt = conn.createStatement()) {
        stmt.execute(SQL);
        conn.commit();
      }
    }
  }
}