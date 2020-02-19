package com.here.xyz.psql;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.vividsolutions.jts.io.WKBWriter;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabaseStreamWriter extends DatabaseWriter{

    protected static FeatureCollection insertFeatures( String schema, String table, String streamId, FeatureCollection collection,
                                                    List<FeatureCollection.ModificationFailure> fails,
                                                    List<Feature> inserts, Connection connection)
            throws SQLException {

        final PreparedStatement insertStmt = createInsertStatement(connection,schema,table);
        final PreparedStatement insertWithoutGeometryStmt = createInsertWithoutGeometryStatement(connection,schema,table);

        for (int i = 0; i < inserts.size(); i++) {

            String fId = "";
            try {
                int rows = 0;
                final Feature feature = inserts.get(i);
                fId = feature.getId();

                final PGobject jsonbObject= featureToPGobject(feature, true);
                final PGobject geojsonbObject = featureToPGobject(feature, false);

                if (feature.getGeometry() == null) {
                    insertWithoutGeometryStmt.setObject(1, jsonbObject);
                    rows = insertWithoutGeometryStmt.executeUpdate();
                } else {
                    insertStmt.setObject(1, jsonbObject);
                    final WKBWriter wkbWriter = new WKBWriter(3);
                    insertStmt.setBytes(2, wkbWriter.write(feature.getGeometry().getJTSGeometry()));
                    insertStmt.setObject(3, geojsonbObject);
                    rows = insertStmt.executeUpdate();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(INSERT_ERROR_GENERAL));
                }else
                    collection.getFeatures().add(feature);

            } catch (Exception e) {
                if(e.getMessage() != null && e.getMessage().contains("relation ") && e.getMessage().contains("does not exist")){
                    insertStmt.close();
                    insertWithoutGeometryStmt.close();
                    connection.close();
                    throw new SQLException(e);
                }

                fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(INSERT_ERROR_GENERAL));
                logException(e,streamId,i, LOG_EXCEPTION_INSERT);
            }
        }

        return collection;
    };

    protected static FeatureCollection updateFeatures( String schema, String table, String streamId, FeatureCollection collection,
                                                    List<FeatureCollection.ModificationFailure> fails,
                                                    List<Feature> updates, Connection connection,
                                                    boolean handleUUID)
            throws SQLException {

        final PreparedStatement updateStmt = createUpdateStatement(connection, schema, table, handleUUID);
        final PreparedStatement updateWithoutGeometryStmt = createUpdateWithoutGeometryStatement(connection,schema,table,handleUUID);

        for (int i = 0; i < updates.size(); i++) {
            String fId = "";
            try {
                final Feature feature = updates.get(i);
                final String puuid = feature.getProperties().getXyzNamespace().getPuuid();
                int rows = 0;

                if (feature.getId() == null) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(UPDATE_ERROR_ID_MISSING));
                    continue;
                }

                fId = feature.getId();

                if (handleUUID && puuid == null){
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(UPDATE_ERROR_PUUID_MISSING));
                    continue;
                }

                final PGobject jsonbObject= featureToPGobject(feature, true);
                final PGobject geojsonbObject = featureToPGobject(feature, false);

                if (feature.getGeometry() == null) {
                    updateWithoutGeometryStmt.setObject(1, jsonbObject);
                    updateWithoutGeometryStmt.setString(2, fId);

                    if(handleUUID)
                        updateWithoutGeometryStmt.setString(3, puuid);

                    rows = updateWithoutGeometryStmt.executeUpdate();
                } else {
                    updateStmt.setObject(1, jsonbObject);
                    final WKBWriter wkbWriter = new WKBWriter(3);
                    updateStmt.setBytes(2, wkbWriter.write(feature.getGeometry().getJTSGeometry()));
                    updateStmt.setObject(3, geojsonbObject);
                    updateStmt.setString(4, fId);

                    if(handleUUID) {
                        updateStmt.setString(5, puuid);
                    }
                    rows = updateStmt.executeUpdate();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage((handleUUID ? UPDATE_ERROR_UUID : UPDATE_ERROR_NOT_EXISTS)));
                }else
                    collection.getFeatures().add(feature);

            } catch (Exception e) {
                fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(UPDATE_ERROR_GENERAL));
                logException(e,streamId,i, LOG_EXCEPTION_UPDATE);
            }
        }

        updateStmt.close();
        updateWithoutGeometryStmt.close();
        connection.close();

        return collection;
    };

    protected static void deleteFeatures(String schema, String table, String streamId,
                                         List<FeatureCollection.ModificationFailure> fails, Map<String, String> deletes,
                                         Connection connection, boolean handleUUID)
            throws SQLException {

        final PreparedStatement deleteStmt = deleteStmtSQLStatement(connection,schema,table,handleUUID);
        final PreparedStatement deleteStmtWithoutUUID = deleteStmtSQLStatement(connection,schema,table,false);

        for (String deleteId : deletes.keySet()) {
            try {
                final String puuid = deletes.get(deleteId);
                int rows = 0;

                if(handleUUID && puuid == null){
                    deleteStmtWithoutUUID.setString(1, deleteId);
                    rows += deleteStmtWithoutUUID.executeUpdate();
                }else{
                    deleteStmt.setString(1, deleteId);
                    if(handleUUID) {
                        deleteStmt.setString(2, puuid);
                    }
                    rows += deleteStmt.executeUpdate();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(deleteId).withMessage((handleUUID ? DELETE_ERROR_UUID : DELETE_ERROR_NOT_EXISTS)));
                }

            } catch (Exception e) {
                deleteStmt.close();
                connection.close();

                fails.add(new FeatureCollection.ModificationFailure().withId(deleteId).withMessage(DELETE_ERROR_GENERAL));
                logException(e,streamId,0, LOG_EXCEPTION_DELETE);
                throw new SQLException(e);
            }
        }
    }
}
