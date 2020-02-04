package com.here.xyz.psql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabaseStreamWriter extends DatabaseWriter{
    private static final Logger logger = LogManager.getLogger();

    public static FeatureCollection insertFeatures(FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails,
                                                   List<Feature> inserts, Connection connection, String schema, String table)
            throws SQLException {
        String insertStmtSQL = "INSERT INTO ${schema}.${table} (jsondata, geo, geojson) VALUES(?::jsonb, ST_Force3D(ST_GeomFromWKB(?,4326)), ?::jsonb)";
        insertStmtSQL = SQLQuery.replaceVars(insertStmtSQL, schema, table);
        boolean batchInsert = false;

        String insertWithoutGeometryStmtSQL = "INSERT INTO ${schema}.${table} (jsondata, geo, geojson) VALUES(?::jsonb, NULL, NULL)";
        insertWithoutGeometryStmtSQL = SQLQuery.replaceVars(insertWithoutGeometryStmtSQL, schema, table);
        boolean batchInsertWithoutGeometry = false;

        final PreparedStatement insertStmt = createStatement(connection, insertStmtSQL);
        final PreparedStatement insertWithoutGeometryStmt = createStatement(connection, insertWithoutGeometryStmtSQL);

        for (int i = 0; i < inserts.size(); i++) {

            String fId = "";
            try {
                int rows = 0;
                final Feature feature = inserts.get(i);
                final Geometry geometry = feature.getGeometry();
                fId = feature.getId();
                feature.setGeometry(null); // Do not serialize the geometry in the JSON object

                final String json;
                final String geojson;
                try {
                    json = feature.serialize();
                    geojson = geometry != null ? geometry.serialize() : null;
                } finally {
                    feature.setGeometry(geometry);
                }

                final PGobject jsonbObject = new PGobject();
                jsonbObject.setType("jsonb");
                jsonbObject.setValue(json);

                final PGobject geojsonbObject = new PGobject();
                geojsonbObject.setType("jsonb");
                geojsonbObject.setValue(geojson);

                if (geometry == null) {
                    insertWithoutGeometryStmt.setObject(1, jsonbObject);
                    rows = insertWithoutGeometryStmt.executeUpdate();
                } else {
                    insertStmt.setObject(1, jsonbObject);
                    final WKBWriter wkbWriter = new WKBWriter(3);
                    insertStmt.setBytes(2, wkbWriter.write(geometry.getJTSGeometry()));
                    insertStmt.setObject(3, geojsonbObject);
                    rows = insertStmt.executeUpdate();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage("Insert Failed"));
                }else
                    collection.getFeatures().add(feature);

            } catch (Exception e) {
                insertStmt.close();
                insertWithoutGeometryStmt.close();
                connection.close();

                fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage("Insert Failed"));

                throw new SQLException(e);
            }
        }
        return collection;
    };

    public static FeatureCollection updateFeatures(FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails,
                                                   List<Feature> updates, Connection connection, String schema, String table,
                                                   boolean handleUUID)
            throws SQLException, JsonProcessingException {
        String updateStmtSQL = "UPDATE ${schema}.${table} SET jsondata = ?::jsonb, geo=ST_Force3D(ST_GeomFromWKB(?,4326)), geojson = ?::jsonb WHERE jsondata->>'id' = ?";
        String updateWithoutGeometryStmtSQL = "UPDATE ${schema}.${table} SET  jsondata = ?::jsonb, geo=NULL, geojson = NULL WHERE jsondata->>'id' = ?";

        if(handleUUID) {
            updateStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
            updateWithoutGeometryStmtSQL += " AND jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = ?";
        }
        updateStmtSQL = SQLQuery.replaceVars(updateStmtSQL, schema, table);
        updateWithoutGeometryStmtSQL = SQLQuery.replaceVars(updateWithoutGeometryStmtSQL, schema, table);

        final PreparedStatement updateStmt = createStatement(connection, updateStmtSQL);
        final PreparedStatement updateWithoutGeometryStmt = createStatement(connection, updateWithoutGeometryStmtSQL);

        for (int i = 0; i < updates.size(); i++) {
            try {
                final Feature feature = updates.get(i);
                final String puuid = feature.getProperties().getXyzNamespace().getPuuid();
                int rows = 0;

                if (feature.getId() == null) {
                    throw new NullPointerException("id");
                }

                if (puuid == null && handleUUID){
                    throw new NullPointerException("puuid");
                }

                final String id = feature.getId();
                final Geometry geometry = feature.getGeometry();
                feature.setGeometry(null); // Do not serialize the geometry in the JSON object

                final String json;
                final String geojson;
                try {
                    json = feature.serialize();
                    geojson = geometry != null ? geometry.serialize() : null;
                } finally {
                    feature.setGeometry(geometry);
                }
                final PGobject jsonbObject = new PGobject();
                jsonbObject.setType("jsonb");
                jsonbObject.setValue(json);

                final PGobject geojsonbObject = new PGobject();
                geojsonbObject.setType("jsonb");
                geojsonbObject.setValue(geojson);

                if (geometry == null) {
                    updateWithoutGeometryStmt.setObject(1, jsonbObject);
                    updateWithoutGeometryStmt.setString(2, id);

                    if(handleUUID)
                        updateWithoutGeometryStmt.setString(3, puuid);

                    rows = updateWithoutGeometryStmt.executeUpdate();
                } else {
                    updateStmt.setObject(1, jsonbObject);
                    final WKBWriter wkbWriter = new WKBWriter(3);
                    updateStmt.setBytes(2, wkbWriter.write(geometry.getJTSGeometry()));
                    updateStmt.setObject(3, geojsonbObject);
                    updateStmt.setString(4, id);

                    if(handleUUID) {
                        updateStmt.setString(5, puuid);
                    }
                    rows = updateStmt.executeUpdate();
//                        updateStmt.execute();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(id).withMessage("UUID does not match!"));
                }else
                    collection.getFeatures().add(feature);

            } catch (Exception e) {
                updateStmt.close();
                updateWithoutGeometryStmt.close();
                connection.close();

                logger.error("{} - Failed to update object #{}: {}", "streamID_TODO", i, e);
                throw new SQLException(e);
            }
        }
        return collection;
    };

    public static FeatureCollection deleteFeatures(Map<String, String> deletes, Connection connection, String schema, String table)
            throws SQLException {
        return null;
    };
}
