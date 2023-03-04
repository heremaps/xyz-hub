package com.here.xyz.pub.models;


import java.util.ArrayList;
import java.util.List;

// POJO for holding Connector Space details fetched from AdminDB
public class SeqJobRequest {
    private String dbUrl;
    private String user;
    private String pswd;
    private List<String> spaceIds;
    private List<String> tableNames;
    private List<String> schemas;

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

    public List<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<String> schemas) {
        this.schemas = schemas;
    }

    public void copyConnectorDTO(final ConnectorDTO connector) {
        setDbUrl(connector.getDbUrl());
        setUser(connector.getUser());
        setPswd(connector.getPswd());
        // assign or append the list
        if (spaceIds==null) {
            setSpaceIds(connector.getSpaceIds());
        }
        else {
            spaceIds.addAll(connector.getSpaceIds());
        }
        if (tableNames==null) {
            setTableNames(connector.getTableNames());
        }
        else {
            tableNames.addAll(connector.getTableNames());
        }
        // prepare list of schema (as many elements as the tableNames)
        final List<String> newSchemaList = new ArrayList<>();
        for (final String tableName : connector.getTableNames()) {
            newSchemaList.add(connector.getSchema());
        }
        if (schemas==null) {
            setSchemas(newSchemaList);
        }
        else {
            schemas.addAll(newSchemaList);
        }
    }

    @Override
    public String toString() {
        return "SeqJobRequest{" +
                "dbUrl='" + dbUrl + '\'' +
                ", user='" + user + '\'' +
                ", pswd='" + pswd + '\'' +
                ", spaceIds=" + spaceIds +
                ", tableNames=" + tableNames +
                ", schemas=" + schemas +
                '}';
    }

}
