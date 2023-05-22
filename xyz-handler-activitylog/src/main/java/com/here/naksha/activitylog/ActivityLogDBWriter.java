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
    public static void fromActicityLogDBToFeature(PsqlDataSource dataSource, String tableName, Integer limit) {
        String schema = dataSource.getSchema();
        List<String> featureList = new ArrayList<String>();
        List<String> geoList = new ArrayList<String>();
        List<Integer> iList = new ArrayList<Integer>();
        int intMaxIFeaturesTable = 0;
        int intMaxIActivityTable = 0;
        try (Connection conn = dataSource.getConnection()) {
            String queryPreRequisite = sqlQueryPreRequisites(schema);
            try (final PreparedStatement stmt = conn.prepareStatement(queryPreRequisite)) {
                stmt.execute();
            }
            String SQLMaxIActivityTable = "select i from " + schema + ".\"" + tableName + "\"" + "order by i desc limit 1;";
            try (final PreparedStatement stmt = conn.prepareStatement(SQLMaxIActivityTable)) {
                final ResultSet result = stmt.executeQuery();
                if (result.next())
                    intMaxIActivityTable = result.getInt(1);
            }
            String SQLMaxIFeaturesTable = "select i from activity.\"Features_Original_Format\" order by i desc limit 1;";
            try (final PreparedStatement stmt = conn.prepareStatement(SQLMaxIFeaturesTable)) {
                final ResultSet result = stmt.executeQuery();
                if (result.next())
                    intMaxIFeaturesTable = result.getInt(1);
            }
            Integer batchNumber = intMaxIFeaturesTable;
            while(batchNumber<intMaxIActivityTable) {
                String SQLSelectActiLog = "SELECT jsondata,geo,i FROM " + schema + ".\"" + tableName + "\" Where i>" + batchNumber + " limit "+limit+";";
                try (final PreparedStatement stmt = conn.prepareStatement(SQLSelectActiLog)) {
                    final ResultSet result = stmt.executeQuery();
                    while (result.next()) {
                        try {
                            Feature activityLogFeature = XyzSerializable.deserialize(result.getString(1), Feature.class);
                            geoList.add(result.getString(2));
                            iList.add(result.getInt(3));
                            ActivityLogHandler.fromActivityLogFormat(activityLogFeature);
                            featureList.add(activityLogFeature.serialize());
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                }
                String sqlBulkInsertQuery = sqlQueryBuilder(featureList, schema, geoList, iList);
                try (final PreparedStatement stmt = conn.prepareStatement(sqlBulkInsertQuery)) {
                    stmt.execute();
                }
                conn.commit();
                batchNumber+=limit;
                geoList.clear();
                iList.clear();
                featureList.clear();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    public static String sqlQueryBuilder(List<String> featureList,String schema,List<String> geoList,List<Integer> iList){
        String firstPart = "INSERT INTO "+ schema +".\"Features_Original_Format\"(jsondata,geo,i) VALUES ";
        for(int iterator=1; iterator<featureList.size()+1;iterator++){
            firstPart+= "(" + "\'"+featureList.get(iterator-1)+"\', " + "'"+geoList.get(iterator-1) + "', "+iList.get(iterator-1)+ ")";
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
