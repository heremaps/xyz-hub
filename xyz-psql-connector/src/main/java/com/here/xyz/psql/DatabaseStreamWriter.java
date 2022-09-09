/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.psql;

import com.here.xyz.connectors.AbstractConnectorHandler.TraceItem;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabaseStreamWriter extends DatabaseWriter{

    protected static FeatureCollection insertFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem, FeatureCollection collection,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                      List<Feature> inserts, Connection connection, boolean forExtendedSpace)
            throws SQLException {

        final PreparedStatement insertStmt = createInsertStatement(connection, schema, table, forExtendedSpace);
        final PreparedStatement insertWithoutGeometryStmt = createInsertWithoutGeometryStatement(connection, schema, table, forExtendedSpace);

        for (int i = 0; i < inserts.size(); i++) {

            String fId = "";
            try {
                int rows = 0;
                final Feature feature = inserts.get(i);
                fId = feature.getId();

                final PGobject jsonbObject= featureToPGobject(feature,null);

                if (feature.getGeometry() == null) {
                    insertWithoutGeometryStmt.setObject(1, jsonbObject);
                    if (forExtendedSpace)
                        insertWithoutGeometryStmt.setBoolean(2, getDeletedFlagFromFeature(feature));
                    insertWithoutGeometryStmt.setQueryTimeout(dbh.calculateTimeout());
                    rows = insertWithoutGeometryStmt.executeUpdate();
                } else {
                    insertStmt.setObject(1, jsonbObject);
                    final WKBWriter wkbWriter = new WKBWriter(3);
                    Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                    //Avoid NAN values
                    assure3d(jtsGeometry.getCoordinates());
                    insertStmt.setBytes(2, wkbWriter.write(jtsGeometry));
                    if (forExtendedSpace)
                        insertStmt.setBoolean(3, getDeletedFlagFromFeature(feature));
                    insertStmt.setQueryTimeout(dbh.calculateTimeout());
                    rows = insertStmt.executeUpdate();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(INSERT_ERROR_GENERAL));
                }else
                    collection.getFeatures().add(feature);

            } catch (Exception e) {
                if((e instanceof SQLException && ((SQLException)e).getSQLState() != null
                        && ((SQLException)e).getSQLState().equalsIgnoreCase("42P01"))){
                    insertStmt.close();
                    insertWithoutGeometryStmt.close();
                    throw new SQLException(e);
                }

                fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(INSERT_ERROR_GENERAL));
                logException(e, traceItem, LOG_EXCEPTION_INSERT, table);
            }
        }

        insertStmt.close();
        insertWithoutGeometryStmt.close();

        return collection;
    }

    protected static FeatureCollection updateFeatures(DatabaseHandler dbh, String schema, String table, TraceItem traceItem, FeatureCollection collection,
                                                      List<FeatureCollection.ModificationFailure> fails,
                                                      List<Feature> updates, Connection connection,
                                                      boolean handleUUID, boolean forExtendedSpace)
            throws SQLException {

        final PreparedStatement updateStmt = createUpdateStatement(connection, schema, table, handleUUID, forExtendedSpace);
        final PreparedStatement updateWithoutGeometryStmt = createUpdateWithoutGeometryStatement(connection,schema,table,handleUUID, forExtendedSpace);

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

                final PGobject jsonbObject= featureToPGobject(feature,null);

                int paramIdx = 0;
                if (feature.getGeometry() == null) {
                    updateWithoutGeometryStmt.setObject(++paramIdx, jsonbObject);
                    if (forExtendedSpace)
                        updateWithoutGeometryStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));
                    updateWithoutGeometryStmt.setString(++paramIdx, fId);
                    if (handleUUID)
                        updateWithoutGeometryStmt.setString(++paramIdx, puuid);

                    updateWithoutGeometryStmt.setQueryTimeout(dbh.calculateTimeout());
                    rows = updateWithoutGeometryStmt.executeUpdate();
                } else {
                    updateStmt.setObject(++paramIdx, jsonbObject);
                    final WKBWriter wkbWriter = new WKBWriter(3);
                    Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                    //Avoid NAN values
                    assure3d(jtsGeometry.getCoordinates());
                    updateStmt.setBytes(++paramIdx, wkbWriter.write(jtsGeometry));
                    if (forExtendedSpace)
                        updateStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));
                    updateStmt.setString(++paramIdx, fId);
                    if (handleUUID)
                        updateStmt.setString(++paramIdx, puuid);

                    updateStmt.setQueryTimeout(dbh.calculateTimeout());
                    rows = updateStmt.executeUpdate();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage((handleUUID ? UPDATE_ERROR_UUID : UPDATE_ERROR_NOT_EXISTS)));
                }else
                    collection.getFeatures().add(feature);

            } catch (Exception e) {
                fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(UPDATE_ERROR_GENERAL));
                logException(e, traceItem, LOG_EXCEPTION_UPDATE, table);
            }
        }

        updateStmt.close();
        updateWithoutGeometryStmt.close();

        return collection;
    }

    protected static void deleteFeatures( DatabaseHandler dbh, String schema, String table, TraceItem traceItem,
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
                    deleteStmtWithoutUUID.setQueryTimeout(dbh.calculateTimeout());
                    rows += deleteStmtWithoutUUID.executeUpdate();
                }else{
                    deleteStmt.setString(1, deleteId);
                    if(handleUUID) {
                        deleteStmt.setString(2, puuid);
                    }
                    deleteStmt.setQueryTimeout(dbh.calculateTimeout());
                    rows += deleteStmt.executeUpdate();
                }

                if(rows == 0) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(deleteId).withMessage((handleUUID ? DELETE_ERROR_UUID : DELETE_ERROR_NOT_EXISTS)));
                }

            } catch (Exception e) {
                fails.add(new FeatureCollection.ModificationFailure().withId(deleteId).withMessage(DELETE_ERROR_GENERAL));
                logException(e, traceItem, LOG_EXCEPTION_DELETE, table);
            }
        }

        deleteStmt.close();
        deleteStmtWithoutUUID.close();
    }
}
