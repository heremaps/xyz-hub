package com.here.mapcreator.ext.naksha;

import static com.here.xyz.NakshaLogger.currentLogger;

import com.here.xyz.INaksha;
import com.here.xyz.lambdas.Pe1;
import com.here.xyz.models.hub.plugins.Storage;
import com.here.xyz.models.hub.transactions.TxSignalSet;
import com.here.xyz.storage.IStorage;
import com.here.xyz.util.IoHelp;
import com.here.xyz.util.json.JsonSerializable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PSQLException;

/**
 * The Naksha PostgresQL storage client. This client does implement low level access to manage
 * collections and the features within these collections. It as well grants access to transactions.
 */
public class PsqlStorage implements IStorage {

    /** The latest version of the naksha-extension stored in the resources. */
    static final PsqlExtVersion latest = new PsqlExtVersion(1, 0, 0);

    /**
     * The constructor to create a new PostgresQL storage client using a storage configuration.
     *
     * @param storage the storage configuration to use for this client.
     * @throws SQLException if any error occurred while accessing the database.
     * @throws IOException if reading the SQL extensions from the resources fail.
     */
    public PsqlStorage(@NotNull Storage storage) throws SQLException, IOException {
        final PsqlStorageProperties properties =
                JsonSerializable.convert(storage.getProperties(), PsqlStorageProperties.class);
        this.dataSource = new PsqlDataSource(properties.config);
        this.storageNumber = storage.number;
        init();
    }

    /**
     * Constructor to manually create a new PostgresQL storage client. You can register this as the
     * main instance by simply setting the {@link INaksha#instance} atomic reference.
     *
     * @param config the PSQL configuration to use for this client.
     * @param storageNumber the unique 40-bit unsigned integer storage number to use. Except for the
     *     main database (which always has the number 0), normally this number is given by the
     *     Naksha-Hub, when creating a storage.
     * @throws SQLException if any error occurred while accessing the database.
     * @throws IOException if reading the SQL extensions from the resources fail.
     */
    public PsqlStorage(@NotNull PsqlConfig config, long storageNumber) throws SQLException, IOException {
        this.dataSource = new PsqlDataSource(config);
        this.storageNumber = storageNumber;
        init();
    }

    /** The default schema to use for the Naksha client. */
    static final @NotNull String DEFAULT_SCHEMA = "postgres";

    /** The transaction table. */
    static final @NotNull String DEFAULT_TRANSACTIONS_TABLE = "naksha:transactions";

    /** The collections table. */
    static final @NotNull String DEFAULT_COLLECTIONS_TABLE = "naksha:collections";

    /**
     * Returns the PostgresQL connection pool.
     *
     * @return the PostgresQL connection pool.
     */
    public final @NotNull PsqlPool getPsqlPool() {
        return dataSource.pool;
    }

    /** The data source. */
    protected final @NotNull PsqlDataSource dataSource;

    /**
     * Returns the PSQL data source.
     *
     * @return the PSQL data source.
     */
    public final @NotNull PsqlDataSource getDataSource() {
        return dataSource;
    }

    /**
     * Returns the main schema to operate on.
     *
     * @return the main schema to operate on.
     */
    public final @NotNull String getSchema() {
        return dataSource.schema;
    }

    /**
     * The connector identification number to use. Except for the main database (which always has the
     * number 0), normally this number is given by the Naksha-Hub, when creating a connector.
     */
    protected final long storageNumber;

    /**
     * Returns the connector identification number.
     *
     * @return the connector identification number.
     */
    public final long getStorageNumber() {
        return storageNumber;
    }

    static final boolean[] ESCAPE = new boolean[128];

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
    static boolean shouldEscape(@NotNull CharSequence id) {
        for (int i = 0; i < id.length(); i++) {
            final char c = id.charAt(i);
            // We signal that every less than the space must be escaped. The escape method then will throw
            // an SQLException!
            if (c < 32 || c > 126 || ESCAPE[c]) {
                return true;
            }
        }
        return false;
    }

    static void escapeWrite(@NotNull CharSequence chars, @NotNull StringBuilder sb) throws SQLException {
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

    /**
     * Escape the given identifier.
     *
     * @param sb The string builder into which to write the escaped identifier.
     * @param id The identifier to escape.
     * @return The given string builder.
     * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
     */
    static @NotNull StringBuilder escapeId(@NotNull StringBuilder sb, @NotNull CharSequence id) throws SQLException {
        sb.append('"');
        escapeWrite(id, sb);
        sb.append('"');
        return sb;
    }

    /**
     * Escape all given identifiers together, not individually, for example:
     *
     * <pre>{@code
     * String prefix = "hello";
     * String postfix = "world";
     * escapeId(prefix, "_", postfix);
     * }</pre>
     *
     * will result in the code generated being: {@code "hello_world"}.
     *
     * @param sb The string builder into which to write the escaped identifiers.
     * @param ids The identifiers to escape.
     * @return The given string builder.
     * @throws SQLException If any identifier contains illegal characters, for example the ASCII-0.
     */
    static @NotNull StringBuilder escapeId(@NotNull StringBuilder sb, @NotNull CharSequence... ids)
            throws SQLException {
        sb.append('"');
        for (final CharSequence id : ids) {
            escapeWrite(id, sb);
        }
        sb.append('"');
        return sb;
    }

    static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()"; // TODO: Why to we need this?
    static final String[] extensionList = new String[] {"postgis", "postgis_topology", "tsm_system_rows", "dblink"};
    static final String[] localScripts = new String[] {"/xyz_ext.sql", "/h3Core.sql", "/naksha_ext.sql"};

    // We can store meta-information at tables using
    // COMMENT ON TABLE "xyz_config"."transactions" IS '{"id":"transactions"}';
    // SELECT pg_catalog.obj_description('xyz_config.transactions'::regclass, 'pg_class');
    // We should simply store the NakshaCollection information in serialized form.

    /**
     * Ensure that the administration tables exists, and the scripts are in the latest version in the
     * database.
     *
     * @throws SQLException If any error occurred while accessing the database.
     * @throws IOException If reading the SQL extensions from the resources fail.
     */
    void init() throws SQLException, IOException {
        String SQL;
        final StringBuilder sb = new StringBuilder();
        try (final Connection conn = dataSource.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                // Create the SCHEMA if it does not yet exist, this must succeed.
                sb.append("CREATE SCHEMA IF NOT EXISTS ");
                escapeId(sb, getSchema());
                stmt.execute(sb.toString());
                conn.commit();

                // Re-Initialize the connection.
                // This ensures that we really have the schema at the end of the search path and therefore
                // selected.
                dataSource.initConnection(conn);

                long version = 0L;
                try {
                    ResultSet rs = stmt.executeQuery("SELECT naksha_version();");
                    if (rs.next()) {
                        version = rs.getLong(1);
                    }
                } catch (PSQLException e) {
                    if (EPsqlState.UNDEFINED_FUNCTION != EPsqlState.of(e)) {
                        throw e;
                    }
                    conn.rollback();
                    currentLogger().info("Naksha extension missing");
                }
                if (latest.toLong() != version) {
                    if (version == 0L) {
                        currentLogger().info("Install and initialize Naksha extension v{}", latest);
                    } else {
                        currentLogger()
                                .info("Upgrade Naksha extension from v{} to v{}", new PsqlExtVersion(version), latest);
                    }
                    SQL = IoHelp.readResource("naksha_ext.sql");
                    stmt.execute(SQL);
                    conn.commit();

                    if (version == 0L) {
                        stmt.execute("SELECT naksha_init();");
                    }
                    conn.commit();
                }
            }
            //      setupH3();
        }
    }

    /**
     * Review all collections and ensure that the history does have the needed partitions created. The
     * method will as well garbage collect the history; if the history of a collection holds data that
     * is too old (exceeds the maximum age), it deletes it.
     *
     * @throws SQLException If any error occurred.
     */
    @Override
    public void maintain() throws SQLException {
        throw new UnsupportedOperationException("maintain");
    }

    @Override
    public @NotNull PsqlTxReader startRead() throws SQLException {
        return new PsqlTxWriter(this);
    }

    @Override
    public @NotNull PsqlTxWriter startWrite() throws SQLException {
        return new PsqlTxWriter(this);
    }

    @Override
    public void addListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
        throw new UnsupportedOperationException("addListener");
    }

    @Override
    public boolean removeListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
        throw new UnsupportedOperationException("removeListener");
    }

    @Override
    public void close() {}

    // Add listener for change events like create/update/delete collection and for the features in it.
    // Basically, listen to transaction log!
    // https://www.postgresql.org/docs/current/sql-notify.html
    // https://access.crunchydata.com/documentation/pgjdbc/42.1.0/listennotify.html

    /**
     * Setup H3?
     *
     * @throws SQLException if any error occurred.
     */
    private void setupH3() throws SQLException {
        final PsqlPoolConfig config = dataSource.getConfig();
        try (final Connection connection = dataSource.getConnection();
                final Statement stmt = connection.createStatement()) {
            boolean needUpdate = false;
            ResultSet rs;
            if ((rs = stmt.executeQuery("select count(1)::integer from pg_catalog.pg_proc r inner join"
                            + " pg_catalog.pg_namespace l  on ( r.pronamespace = l.oid ) where 1 = 1"
                            + " and l.nspname = 'h3' and r.proname = 'h3_version'"))
                    .next()) {
                needUpdate = (0 == rs.getInt(1));
            }

            //      if (!needUpdate && (rs = stmt.executeQuery("select h3.h3_version()")).next()) {
            //        needUpdate = (H3_CORE_VERSION > rs.getInt(1));
            //      }

            //      if (needUpdate) {
            //        stmt.execute(readResource("/h3Core.sql"));
            //        stmt.execute(MaintenanceSQL.generateSearchPathSQL(XYZ_CONFIG));
            //        logger.info("{} - Successfully created H3 SQL-Functions on database {}@{}",
            // clientName, config.user, config.url);
            //      }
        } catch (Exception e) { // IOException
            //      logger.error("{} - Failed run h3 init on database: {}@{}", clientName, config.user,
            // config.url, e);
            throw new SQLException("Failed to setup H3: " + e.getMessage());
        }
    }

    //
    // maintainSpaceSchema()
    // maintainSpace()
    // maintainTransactions()
    //

    // -----------------------------------------------------------------------------------------------------------------------------------
    // ---------- Caching code
    // -----------------------------------------------------------------------------------------------------------------------------------

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
    // SELECT * FROM naksha_fetch_transaction(id); -- fetch all changes from this transaction (id
    // sequential identifier)
    //
    //
    //  private void installNakshaExtension() throws SQLException {
    //    final StringBuilder sb = NakshaThreadLocal.get().sb();
    //    String SQL;
    //
    //    sb.setLength(0);
    //    final String MGMT_SCHEMA = escapeId(dataSource.getSchema(), sb).toString();
    //
    //    try (final Connection conn = dataSource.getConnection()) {
    //      sb.setLength(0);
    //      sb.append("CREATE TABLE IF NOT EXISTS
    // ").append(MGMT_SCHEMA).append('.').append(DEFAULT_SPACE_COLLECTION).append(" (")
    //          .append(" id                TEXT PRIMARY KEY NOT NULL")
    //          .append(",owner             TEXT NOT NULL")
    //          .append(",config            JSONB NOT NULL")
    //          .append("i                  int8 NOT NULL")
    //          .append(");\n");
    //      sb.append("CREATE TABLE IF NOT EXISTS
    // ").append(MGMT_SCHEMA).append(+'.').append(DEFAULT_CONNECTOR_COLLECTION).append(" (")
    //          .append(" id                TEXT PRIMARY KEY NOT NULL")
    //          .append(",owner             TEXT NOT NULL")
    //          .append(",cid               TEXT")
    //          .append(",config            JSONB NOT NULL")
    //          .append(",i int8            NOT NULL")
    //          .append(");\n");
    //
    //      sb.append("CREATE TABLE IF NOT EXISTS
    // ").append(MGMT_SCHEMA).append(+'.').append(DEFAULT_SUBSCRIPTIONS_COLLECTION).append(" (")
    //          .append(" subscription_id   TEXT NOT NULL")
    //          .append(",last_txn_id       int8 NOT NULL")
    //          .append(",updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()")
    //          .append(");");
    //      sb.append("CREATE UNIQUE INDEX IF NOT EXISTS ");
    //      escapeId(DEFAULT_SUBSCRIPTIONS_COLLECTION, "_id_idx", sb);
    //      sb.append(" ON
    // ").append(MGMT_SCHEMA).append('.').append(DEFAULT_SUBSCRIPTIONS_COLLECTION);
    //      sb.append(" USING btree (subscription_id);\n");
    //
    //      SQL = sb.toString();
    //      try (final Statement stmt = conn.createStatement()) {
    //        stmt.execute(SQL);
    //        conn.commit();
    //      }
    //    }
    //  }

    // TODO: Fill spaces and connectors
}