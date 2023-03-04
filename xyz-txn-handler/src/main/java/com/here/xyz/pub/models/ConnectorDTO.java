package com.here.xyz.pub.models;


import java.util.List;

// POJO for holding Connector Space details fetched from AdminDB
public class ConnectorDTO {
    private String id;
    private String dbUrl;
    private String user;
    private String pswd;
    private String schema;
    private List<String> spaceIds;
    private List<String> tableNames;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<String> getSpaceIds() {
        return spaceIds;
    }

    public void setSpaceIds(List<String> spaceIds) {
        this.spaceIds = spaceIds;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public String toString() {
        return "ConnectorDTO{" +
                "id='" + id + '\'' +
                ", dbUrl='" + dbUrl + '\'' +
                ", user='" + user + '\'' +
                ", pswd='" + pswd + '\'' +
                ", schema='" + schema + '\'' +
                ", spaceIds=" + spaceIds +
                ", tableNames=" + tableNames +
                '}';
    }

}
