package com.here.xyz.psql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.here.xyz.psql.DatabaseHandler.STATEMENT_TIMEOUT_SECONDS;

public class DatabaseWriter {

    protected static PreparedStatement createStatement(Connection connection, String statement) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(statement);
        preparedStatement.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
        return preparedStatement;
    }

    protected  static void setAutocommit(Connection connection, boolean isActive) throws SQLException {
        connection.setAutoCommit(isActive);
    }

    public static FeatureCollection insertFeatures(FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails,
                                                   List<Feature> inserts, Connection connection, String schema, String table,
                                                   boolean transactional)
            throws SQLException, JsonProcessingException {
        if(transactional) {
            setAutocommit(connection,false);
            return DatabaseTransactionalWriter.insertFeatures(collection, inserts, connection, schema, table);
        }
        setAutocommit(connection,true);
        return DatabaseStreamWriter.insertFeatures(collection, fails, inserts, connection, schema, table);
    };

    public static FeatureCollection updateFeatures(FeatureCollection collection, List<FeatureCollection.ModificationFailure> fails,
                                                   List<Feature> updates, Connection connection, String schema, String table,
                                                   boolean transactional, boolean handleUUID)
            throws SQLException, JsonProcessingException {
        if(transactional) {
            setAutocommit(connection,false);
            return DatabaseTransactionalWriter.updateFeatures(collection, updates, connection, schema, table);
        }
        setAutocommit(connection,true);
        return DatabaseStreamWriter.updateFeatures(collection, fails, updates, connection, schema, table, handleUUID);
    };

    public static void deleteFeatures(Map<String, String> deletes, Connection connection, String schema,
                                                   String table, boolean transactional)
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
