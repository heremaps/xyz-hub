package com.here.xyz.pub.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.here.xyz.pub.models.JdbcConnectionParams;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PubJdbcConnectionPool {
    private static final Logger logger = LogManager.getLogger();

    // Entire cache will be flushed every x hours (e.g. 8hrs) to prevent pile-up of stale data
    final private static int dsCacheExpiryInMins = 8*60;
    private static long dsCacheExpiryEpochMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(dsCacheExpiryInMins);
    private static ConcurrentHashMap<JdbcConnectionParams, HikariDataSource> dsCache = new ConcurrentHashMap<>();

    private PubJdbcConnectionPool() {
    }


    public static Connection getConnection(final JdbcConnectionParams dbConnParams) throws SQLException {
        // Flush old cache if it has expired
        if (System.currentTimeMillis() > dsCacheExpiryEpochMs) {
            flushDataSourceCache(null);
        }
        HikariDataSource ds = dsCache.get(dbConnParams);
        if (ds == null) {
            ds = createAndCacheDataSource(dbConnParams);
        }
        return ds.getConnection();
    }


    /*
    * Note - spaceId will be null, if full cache flush is required.
    * Else, existing datasources for that spaceId ONLY should be flushed
     */
    private synchronized static void flushDataSourceCache(final String spaceId) {
        // As this block is synchronized, by the time 2nd concurrent thread enters in this function,
        // dsCache might have been already flushed
        if (spaceId == null && System.currentTimeMillis() < dsCacheExpiryEpochMs) {
            return;
        }
        boolean flushThisDS = false;
        for (JdbcConnectionParams connParams : dsCache.keySet()) {
            flushThisDS = (spaceId == null || spaceId.equals(connParams.getSpaceId()));
            if (flushThisDS) {
                dsCache.get(connParams).close(); // close datasource
                dsCache.remove(connParams); // remove datasource from cache
                logger.info("Removed old cached DataSource for spaceId {}.", connParams.getSpaceId());
            }
        }
        // reset cache (if it is full cache flush)
        if (spaceId == null) {
            dsCache = new ConcurrentHashMap<>();
            dsCacheExpiryEpochMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(dsCacheExpiryInMins);
            logger.info("Recreated DataSource Cache with expiry of {}mins.", dsCacheExpiryInMins);
        }
    }


    private static synchronized HikariDataSource createAndCacheDataSource(final JdbcConnectionParams dbConnParams) {
        // "synchronized" block to prevent duplicate datasource creation for the same space + database combination
        HikariDataSource ds = dsCache.get(dbConnParams);
        if (ds != null)
            return ds;

        // Create new Datasource (connection pool) and add it to a cache
        final HikariConfig config = new HikariConfig();
        String dbUrl = dbConnParams.getDbUrl();
        dbUrl += (dbUrl.contains("?") ? "&" : "?") + "ApplicationName=XYZ-Hub-Publisher";
        config.setDriverClassName(dbConnParams.getDriveClass());
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbConnParams.getUser());
        config.setPassword(dbConnParams.getPswd());
        config.setConnectionTimeout(dbConnParams.getConnTimeout());
        config.setMaximumPoolSize(dbConnParams.getMaxPoolSize());
        config.setMinimumIdle(dbConnParams.getMinPoolSize());
        config.setIdleTimeout(dbConnParams.getIdleTimeout());
        config.setAutoCommit(false);
        ds = new HikariDataSource(config);

        // For the same SpaceId, remove previous DataSource (if exists) from the cache
        flushDataSourceCache(dbConnParams.getSpaceId());

        dsCache.put(dbConnParams, ds);
        return ds;
    }

}
