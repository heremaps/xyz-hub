package com.here.xyz.pub.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import com.here.xyz.pub.models.JdbcConnectionParams;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class PubJdbcConnectionPool {

    // TODO : Use TTL to clear out DS regularly
    private static ConcurrentHashMap<JdbcConnectionParams, HikariDataSource> dsCache = new ConcurrentHashMap<>();

    private PubJdbcConnectionPool() {
    }


    public static Connection getConnection(final JdbcConnectionParams dbConnParams) throws SQLException {
        HikariDataSource ds = dsCache.get(dbConnParams);
        if (ds == null) {
            ds = createAndCacheDataSource(dbConnParams);
        }
        return ds.getConnection();
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

        dsCache.put(dbConnParams, ds);
        return ds;
    }

}
