package com.here.xyz.pub.models;

import java.util.Objects;

// POJO for holding Database connection parameters
public class JdbcConnectionParams {
    // XYZ Space id, useful to cache/distinguish multiple Datasource's
    private String spaceId;
    // Hikari connection pool configuration
    private String driveClass = "org.postgresql.Driver";
    private String dbUrl;
    private String user;
    private String pswd;
    // Hikari connection pool configuration
    private long connTimeout = 30000;
    private int maxPoolSize = 10;
    private int minPoolSize = 1;
    private long idleTimeout = 600000;

    public String getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(String spaceId) {
        this.spaceId = spaceId;
    }

    public String getDriveClass() {
        return driveClass;
    }

    public void setDriveClass(String driveClass) {
        this.driveClass = driveClass;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPswd() {
        return pswd;
    }

    public void setPswd(String pswd) {
        this.pswd = pswd;
    }

    public long getConnTimeout() {
        return connTimeout;
    }

    public void setConnTimeout(long connTimeout) {
        this.connTimeout = connTimeout;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcConnectionParams that = (JdbcConnectionParams) o;
        return connTimeout == that.connTimeout && maxPoolSize == that.maxPoolSize && minPoolSize == that.minPoolSize && idleTimeout == that.idleTimeout && spaceId.equals(that.spaceId) && driveClass.equals(that.driveClass) && dbUrl.equals(that.dbUrl) && user.equals(that.user) && pswd.equals(that.pswd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spaceId, driveClass, dbUrl, user, pswd, connTimeout, maxPoolSize, minPoolSize, idleTimeout);
    }

    @Override
    public String toString() {
        return "JdbcConnectionParams{" +
                "spaceId='" + spaceId + '\'' +
                ", driveClass='" + driveClass + '\'' +
                ", dbUrl='" + dbUrl + '\'' +
                ", user='" + user + '\'' +
                ", connTimeout=" + connTimeout +
                ", maxPoolSize=" + maxPoolSize +
                ", minPoolSize=" + minPoolSize +
                ", idleTimeout=" + idleTimeout +
                '}';
    }

}
