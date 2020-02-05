package com.here.xyz.psql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.here.xyz.psql.DatabaseHandler.STATEMENT_TIMEOUT_SECONDS;

public class DatabaseWriter {

    protected static PGobject featureToPGobject(final Feature feature, final boolean jsonObjectMode) throws SQLException {
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

        if(jsonObjectMode){
            final PGobject jsonbObject = new PGobject();
            jsonbObject.setType("jsonb");
            jsonbObject.setValue(json);
            return jsonbObject;
        }

        final PGobject geojsonbObject = new PGobject();
        geojsonbObject.setType("jsonb");
        geojsonbObject.setValue(geojson);

        return geojsonbObject;
    }

    protected static PreparedStatement createStatement(Connection connection, String statement) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(statement);
        preparedStatement.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
        return preparedStatement;
    }

    protected  static void setAutocommit(Connection connection, boolean isActive) throws SQLException {
        connection.setAutoCommit(isActive);
    }

    protected static FeatureCollection insertFeatures(String schema, String table, String streamId, FeatureCollection collection,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                   List<Feature> inserts, Connection connection,
                                                   boolean transactional)
            throws SQLException, JsonProcessingException {
        if(transactional) {
            setAutocommit(connection,false);
            return DatabaseTransactionalWriter.insertFeatures(schema, table, streamId, collection, inserts, connection);
        }
        setAutocommit(connection,true);
        return DatabaseStreamWriter.insertFeatures(schema, table, streamId, collection, fails, inserts, connection);
    }

    protected static FeatureCollection updateFeatures(String schema, String table, String streamId, FeatureCollection collection,
                                                   List<FeatureCollection.ModificationFailure> fails,
                                                   List<Feature> updates, Connection connection,
                                                   boolean transactional, boolean handleUUID)
            throws SQLException, JsonProcessingException {
        if(transactional) {
            setAutocommit(connection,false);
            return DatabaseTransactionalWriter.updateFeatures(schema, table, streamId, collection, updates, connection);
        }
        setAutocommit(connection,true);
        return DatabaseStreamWriter.updateFeatures(schema, table, streamId, collection, fails, updates, connection, handleUUID);
    }

    protected static void deleteFeatures( String schema, String table, String streamId,
                                          Map<String, String> deletes, Connection connection, boolean transactional)
     throws SQLException {
        final Set<String> idsToDelete = deletes.keySet();

/** eventually needed for UUID
        String deleteAtomicStmtSQL = "DELETE FROM ${schema}.${table} WHERE jsondata->>'id' = ? AND jsondata->'properties'->'@ns:com:here:xyz'->>'hash' = ?";
        deleteAtomicStmtSQL = SQLQuery.replaceVars(deleteAtomicStmtSQL, schema, table);

        try (final PreparedStatement deleteAtomicStmt = createStatement(connection, deleteAtomicStmtSQL)) {
            for (String id : deletes.keySet()) {
                final String hash = deletes.get(id);
                try {
                    if (hash == null) {
                        idsToDelete.add(id);
                    } else {
                        deleteAtomicStmt.setString(1, id);
                        deleteAtomicStmt.setString(2, hash);
                        deleteAtomicStmt.addBatch();
                    }
                } catch (Exception e) {
                    throw e;
                }
            }
            deleteAtomicStmt.executeBatch();
        } catch (Exception dex) {
            throw dex;
        }
*/

        if (idsToDelete.size() > 0) {
            String deleteStmtSQL = "DELETE FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)";
            deleteStmtSQL = SQLQuery.replaceVars(deleteStmtSQL, schema, table);
            try (final PreparedStatement deleteStmt = createStatement(connection, deleteStmtSQL)) {
                deleteStmt.setArray(1, connection.createArrayOf("text", idsToDelete.toArray(new String[idsToDelete.size()])));
                deleteStmt.execute();
            }
        }
    }
}
