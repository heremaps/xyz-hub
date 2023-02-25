package com.here.xyz.pub.db;

import com.here.xyz.models.hub.Subscription;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.pub.models.JdbcConnectionParams;
import com.here.xyz.pub.models.PubConfig;
import com.here.xyz.pub.models.PubTransactionData;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PubDatabaseHandler {
    private static final Logger logger = LogManager.getLogger();

    final private static String LOCK_SQL
            = "SELECT pg_try_advisory_lock( ('x' || md5('%s') )::bit(60)::bigint ) AS success";

    final private static String UNLOCK_SQL
            = "SELECT pg_advisory_unlock( ('x' || md5('%s') )::bit(60)::bigint ) AS success";

    final private static String FETCH_ALL_SUBSCRIPTIONS =
            "SELECT s.id, s.source, s.config " +
                    "FROM "+PubConfig.XYZ_ADMIN_DB_CFG_SCHEMA+".xyz_subscription s " +
                    "WHERE s.status->>'state' = 'ACTIVE'";

    // TODO : Change to xyz_config (i.e. PubConfig.XYZ_ADMIN_DB_CFG_SCHEMA)
    final private static String FETCH_TXN_ID_FOR_SUBSCRIPTION =
            "SELECT t.last_txn_id FROM xyz_ops.xyz_txn_pub t WHERE t.subscription_id = ?";

    final private static String FETCH_CONN_DETAILS_FOR_SPACE =
            "SELECT st.config->'params'->>'ecps' AS ecps, st.config->'remoteFunctions'->'local'->'env'->>'ECPS_PHRASE' AS pswd " +
            "FROM "+PubConfig.XYZ_ADMIN_DB_CFG_SCHEMA+".xyz_storage st " +
            "WHERE st.id = (SELECT s.config->'storage'->>'id' FROM "+PubConfig.XYZ_ADMIN_DB_CFG_SCHEMA+".xyz_space s WHERE s.id = ?)";

    // TODO : Change to xyz_config (i.e. PubConfig.XYZ_ADMIN_DB_CFG_SCHEMA)
    // TODO : LIMIT can split transactional data into multiple fetches.
    //      It may require a fix in future, if it needs to be transactional batch publish.
    final private static String FETCH_TXNS_FROM_SPACEDB =
            "SELECT t.txn_id, d.operation, d.jsondata " +
            "FROM xyz_ops.xyz_txn t, xyz_ops.xyz_txn_data d " +
            "WHERE d.uuid = ANY(t.uuids) " +
            "AND t.txn_id > ? AND t.space_id = ? " +
            "ORDER BY t.txn_id asc, t.op_timestamp asc " +
            "LIMIT 50";

    // TODO : Change to xyz_config (i.e. PubConfig.XYZ_ADMIN_DB_CFG_SCHEMA)
    final private static String UPDATE_PUB_TXN_ID =
            "UPDATE xyz_ops.xyz_txn_pub " +
            "SET last_txn_id = ? , updated_at = now() " +
            "WHERE subscription_id = ? ";

    // TODO : Change to xyz_config (i.e. PubConfig.XYZ_ADMIN_DB_CFG_SCHEMA)
    final private static String INSERT_PUB_TXN_ID =
            "INSERT INTO xyz_ops.xyz_txn_pub (subscription_id, last_txn_id, updated_at) " +
            "VALUES (? , ? , now()) ";





    private static boolean _advisory(final String lockName, final Connection connection, final boolean lock) throws SQLException {
        boolean success = false;
        final boolean autoCommitFlag = connection.getAutoCommit();
        connection.setAutoCommit(true);

        try(final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery(String.format(Locale.US, lock? LOCK_SQL : UNLOCK_SQL, lockName));) {
            if (rs.next()) {
                success = rs.getBoolean("success");
            }
        }
        connection.setAutoCommit(autoCommitFlag);
        return success;
    }

    public static boolean advisoryLock(final String lockName, final Connection conn) throws SQLException {
        return _advisory(lockName, conn,true);
    }

    public static boolean advisoryUnlock(final String lockName, final Connection conn) throws SQLException {
        return _advisory(lockName, conn,false);
    }

    // Fetch all active subscriptions from AdminDB::xyz_config::xyz_subscription table
    public static List<Subscription> fetchAllSubscriptions(final JdbcConnectionParams dbConnParams) throws SQLException {
        List<Subscription> subList = null;

        try (final Connection conn = PubJdbcConnectionPool.getConnection(dbConnParams);
             final Statement stmt = conn.createStatement();
             final ResultSet rs = stmt.executeQuery(FETCH_ALL_SUBSCRIPTIONS);
            ) {
            while (rs.next()) {
                final String cfgJsonStr = rs.getString("config");
                final Subscription sub = new Subscription();
                sub.setConfig(Json.decodeValue(cfgJsonStr, Subscription.SubscriptionConfig.class));
                sub.setId(rs.getString("id"));
                sub.setSource(rs.getString("source"));
                if (subList == null) {
                    subList = new ArrayList<>();
                }
                subList.add(sub);
            }
        }
        return subList;
    }

    // Fetch last_txn_id from AdminDB::xyz_config::xyz_txn_pub table
    public static long fetchLastTxnIdForSubId(final String subId, final JdbcConnectionParams dbConnParams) throws SQLException {
        long lastTxnId = -1;

        try (final Connection conn = PubJdbcConnectionPool.getConnection(dbConnParams);
             final PreparedStatement stmt = conn.prepareStatement(FETCH_TXN_ID_FOR_SUBSCRIPTION);
        ) {
            stmt.setString(1, subId);
            final ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                lastTxnId = rs.getLong("last_txn_id");
            }
            rs.close();
        }
        return lastTxnId;
    }

    // Fetch DB Connection details for a given spaceId
    public static JdbcConnectionParams fetchDBConnParamsForSpaceId(final String spaceId,
                           final JdbcConnectionParams dbConnParams) throws SQLException, GeneralSecurityException {
        JdbcConnectionParams spaceDBConnParams = null;

        try (final Connection conn = PubJdbcConnectionPool.getConnection(dbConnParams);
             final PreparedStatement stmt = conn.prepareStatement(FETCH_CONN_DETAILS_FOR_SPACE);
        ) {
            stmt.setString(1, spaceId);
            final ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                final String ecps = rs.getString("ecps");
                final String pswd = rs.getString("pswd");
                // Decrypt ecps into JSON string
                // You get back string like this:
                // {"PSQL_HOST":"database-host.com","PSQL_PORT":"5432","PSQL_DB":"postgresdb","PSQL_USER":"pg_user","PSQL_PASSWORD":"pg_pswd","PSQL_SCHEMA":"pg_schema"}
                final String decodedJsonStr = new PSQLConfig.AESGCMHelper(pswd).decrypt(ecps);
                // Transform JSON string and extract connection details
                final Map<String, Object> jsonMap = new JsonObject(decodedJsonStr).getMap();
                spaceDBConnParams = new JdbcConnectionParams();
                spaceDBConnParams.setSpaceId(spaceId);
                final String dbUrl = "jdbc:postgresql://"+jsonMap.get("PSQL_HOST")+":"+jsonMap.get("PSQL_PORT")+"/"+jsonMap.get("PSQL_DB");
                spaceDBConnParams.setDbUrl(dbUrl);
                spaceDBConnParams.setUser(jsonMap.get("PSQL_USER").toString());
                spaceDBConnParams.setPswd(jsonMap.get("PSQL_PASSWORD").toString());
            }
            rs.close();
        }
        return spaceDBConnParams;
    }

    public static List<PubTransactionData> fetchPublishableTransactions(
            final JdbcConnectionParams spaceDBConnParams, final String spaceId, final long lastTxnId) throws SQLException {
        List<PubTransactionData> txnList = null;

        try (final Connection conn = PubJdbcConnectionPool.getConnection(spaceDBConnParams);
             final PreparedStatement stmt = conn.prepareStatement(FETCH_TXNS_FROM_SPACEDB);
        ) {
            stmt.setLong(1, lastTxnId);
            stmt.setString(2, spaceId);
            final ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                final PubTransactionData pubTxnData = new PubTransactionData();
                pubTxnData.setTxnId(rs.getLong("txn_id"));
                pubTxnData.setAction(rs.getString("operation"));
                pubTxnData.setJsonData(rs.getString("jsonData"));
                // add to the list
                if (txnList == null) {
                    txnList = new ArrayList<>();
                }
                txnList.add(pubTxnData);
            }
            rs.close();
        }
        return txnList;
    }


    public static void saveLastTxnId(
            final JdbcConnectionParams spaceDBConnParams, final String subId, final long lastTxnId) throws SQLException {
        int rowCnt = 0;

        // UPDATE or INSERT into xyz_txn_pub table
        try (final Connection conn = PubJdbcConnectionPool.getConnection(spaceDBConnParams)) {
            // First try UPDATE
            try (final PreparedStatement stmt = conn.prepareStatement(UPDATE_PUB_TXN_ID)) {
                stmt.setLong(1, lastTxnId);
                stmt.setString(2, subId);
                rowCnt = stmt.executeUpdate();
            }
            if (rowCnt > 0) {
                // UPDATE was successful, return from here
                conn.commit();
                return;
            }

            // UPDATE was unsuccessful, try INSERT
            try (final PreparedStatement stmt = conn.prepareStatement(INSERT_PUB_TXN_ID)) {
                stmt.setString(1, subId);
                stmt.setLong(2, lastTxnId);
                stmt.executeUpdate();
            }
            conn.commit();
        }
    }

}
