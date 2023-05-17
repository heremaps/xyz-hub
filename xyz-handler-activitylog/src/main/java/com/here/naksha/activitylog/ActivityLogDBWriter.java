package com.here.naksha.activitylog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.mapcreator.ext.naksha.PsqlDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ActivityLogDBWriter {
    public static void fromActicityLogDBToFeature(PsqlDataSource dataSource) {
        List<String> featureList = new ArrayList<String>();
        try (Connection conn = dataSource.getConnection()) {
            String SQL = "SELECT * FROM activity.\"RnxiONGZ\" LIMIT 2;";
            try (final PreparedStatement stmt = conn.prepareStatement(SQL)) {
                final ResultSet result = stmt.executeQuery();
                while (result.next()) {
                    try {
                        Feature activityLogFeature = XyzSerializable.deserialize(result.getString(1), Feature.class);
                        ActivityLogHandler.fromActivityLogFormat(activityLogFeature);
                        featureList.add(activityLogFeature.serialize());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            }
            String queryPreRequisite = sqlQueryPreRequisites();
            try (final PreparedStatement stmt = conn.prepareStatement(queryPreRequisite)) {
                stmt.executeUpdate();
            }
            String sqlBulkInsertQuery = sqlQueryBuilder(featureList);
            try (final PreparedStatement stmt = conn.prepareStatement(sqlBulkInsertQuery)) {
                stmt.executeUpdate();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    public static String sqlQueryBuilder(List<String> featureList){
        //TODO Custom Table Name & Schema
        String firstPart = "INSERT INTO activity.\"Features_Original_Format\"(jsondata, i) VALUES ";
        for(int iterator=0; iterator<featureList.size();iterator++){
            firstPart+= "(" + "\'"+featureList.get(iterator)+"\'" +","+ iterator + ")";
            if(iterator+1!=featureList.size()){
                firstPart+=",";
            }
        }
        sqlQueryPreRequisites();
        return firstPart;
    }
    public static String sqlQueryPreRequisites(){
        //TODO Custom Table Name & Schema
        String sqlQuery = "CREATE SCHEMA IF NOT EXISTS activity;\n" +
                "CREATE TABLE IF NOT EXISTS activity.\"Features_Original_Format\"\n" +
                "(\n" +
                "    jsondata      jsonb,\n" +
                "    geo           varchar,\n" +
                "    i             int8 PRIMARY KEY NOT NULL\n" +
                ");\n" +
                "CREATE SEQUENCE IF NOT EXISTS activity.\"Features_Original_Format\" AS int8 OWNED BY activity.\"Features_Original_Format\".i;";
        return sqlQuery;
    }
}
