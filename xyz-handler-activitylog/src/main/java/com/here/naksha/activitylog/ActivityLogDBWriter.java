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
    public static void fromActicityLogDBToFeature(PsqlDataSource dataSource, String tableName) {
        String schema = dataSource.getSchema();
        List<String> featureList = new ArrayList<String>();
        try (Connection conn = dataSource.getConnection()) {
            String SQL = "SELECT * FROM "+ schema +".\""+tableName+"\" LIMIT 2;";
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
            String queryPreRequisite = sqlQueryPreRequisites(schema);
            try (final PreparedStatement stmt = conn.prepareStatement(queryPreRequisite)) {
                stmt.execute();
            }
            String sqlBulkInsertQuery = sqlQueryBuilder(featureList,schema);
            try (final PreparedStatement stmt = conn.prepareStatement(sqlBulkInsertQuery)) {
                stmt.execute();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    public static String sqlQueryBuilder(List<String> featureList,String schema){
        String firstPart = "INSERT INTO "+ schema +".\"Features_Original_Format\"(jsondata,i) VALUES ";
        for(int iterator=1; iterator<featureList.size()+1;iterator++){
            firstPart+= "(" + "\'"+featureList.get(iterator-1)+"\', "+"nextval('" +schema +".Features_Original_Format_i_seq')" + ")";
            if(iterator!=featureList.size()){
                firstPart+=",";
            }
        }
        return firstPart;
    }
    public static String sqlQueryPreRequisites(String schema){
        String sqlQuery = "CREATE SCHEMA IF NOT EXISTS "+ schema + ";" +
                "CREATE TABLE IF NOT EXISTS " + schema +".\"Features_Original_Format\"" +
                "(\n" +
                "    jsondata      jsonb," +
                "    geo           varchar," +
                "    i             int8 PRIMARY KEY" +
                ");" +
                "CREATE SEQUENCE IF NOT EXISTS " + schema +"."+ "Features_Original_Format_i_seq" +" AS int8 OWNED BY " + schema +".\"Features_Original_Format\".i;";
        return sqlQuery;
    }
}
