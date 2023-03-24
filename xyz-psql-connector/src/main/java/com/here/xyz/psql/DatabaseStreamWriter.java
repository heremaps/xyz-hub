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
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
                boolean success = false;
                ResultSet rs = null;
                final Feature feature = inserts.get(i);
                fId = feature.getId();

                final PGobject jsonbObject= featureToPGobject(feature,null);
                final List<PGobject> jsonbObjectList = new ArrayList<>();
                final List<Geometry> geometryList = new ArrayList<>();

                jsonbObjectList.add(jsonbObject);
                if (feature.getGeometry() == null) {
                    insertWithoutGeometryStmt.setArray(1, connection.createArrayOf("jsonb", jsonbObjectList.toArray()));
                    /*if (forExtendedSpace)
                        insertWithoutGeometryStmt.setBoolean(2, getDeletedFlagFromFeature(feature));*/
                    insertWithoutGeometryStmt.setQueryTimeout(dbh.calculateTimeout());
                    success = insertWithoutGeometryStmt.execute();
                    if (success) {
                        rs = insertWithoutGeometryStmt.getResultSet();
                    }
                } else {
                    insertStmt.setArray(1, connection.createArrayOf("jsonb", jsonbObjectList.toArray()));
                    Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                    //Avoid NAN values
                    assure3d(jtsGeometry.getCoordinates());
                    geometryList.add(jtsGeometry);
                    insertStmt.setArray(2, connection.createArrayOf("geometry", geometryList.toArray()));
                    /*if (forExtendedSpace)
                        insertStmt.setBoolean(3, getDeletedFlagFromFeature(feature));*/
                    insertStmt.setQueryTimeout(dbh.calculateTimeout());
                    success = insertStmt.execute();
                    if (success) {
                        rs = insertStmt.getResultSet();
                    }
                }

                if(success && rs!=null && rs.next() && ((boolean[])rs.getArray("success").getArray())[0] ) {
                    saveXyzNamespaceInFeature(feature, ((String[])rs.getArray("xyz_ns").getArray())[0] );
                    rs.close();
                    collection.getFeatures().add(feature);
                }else {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage(INSERT_ERROR_GENERAL));
                }

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
                boolean success = false;
                ResultSet rs = null;

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
                final List<String> fIdList = new ArrayList<>();
                final List<String> uuidList = new ArrayList<>();
                final List<PGobject> jsonbObjectList = new ArrayList<>();
                final List<Geometry> geometryList = new ArrayList<>();

                fIdList.add(fId);
                if (handleUUID) {
                    uuidList.add(puuid);
                }
                else {
                    uuidList.add(null);
                }
                jsonbObjectList.add(jsonbObject);
                int paramIdx = 0;
                if (feature.getGeometry() == null) {
                    updateWithoutGeometryStmt.setArray(++paramIdx, connection.createArrayOf("text", fIdList.toArray()));
                    updateWithoutGeometryStmt.setArray(++paramIdx, connection.createArrayOf("text", uuidList.toArray()));
                    updateWithoutGeometryStmt.setArray(++paramIdx, connection.createArrayOf("jsonb", jsonbObjectList.toArray()));
                    /*if (forExtendedSpace)
                        updateWithoutGeometryStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));*/

                    updateWithoutGeometryStmt.setQueryTimeout(dbh.calculateTimeout());
                    success = updateWithoutGeometryStmt.execute();
                    if (success) {
                        rs = updateWithoutGeometryStmt.getResultSet();
                    }
                } else {
                    updateStmt.setArray(++paramIdx, connection.createArrayOf("text", fIdList.toArray()));
                    updateStmt.setArray(++paramIdx, connection.createArrayOf("text", uuidList.toArray()));
                    updateStmt.setArray(++paramIdx, connection.createArrayOf("jsonb", jsonbObjectList.toArray()));
                    Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                    //Avoid NAN values
                    assure3d(jtsGeometry.getCoordinates());
                    geometryList.add(jtsGeometry);
                    updateStmt.setArray(++paramIdx, connection.createArrayOf("geometry", geometryList.toArray()));
                    /*if (forExtendedSpace)
                        updateStmt.setBoolean(++paramIdx, getDeletedFlagFromFeature(feature));*/

                    updateStmt.setQueryTimeout(dbh.calculateTimeout());
                    success = updateStmt.execute();
                    if (success) {
                        rs = updateStmt.getResultSet();
                    }
                }

                if(success && rs!=null && rs.next() && ((boolean[])rs.getArray("success").getArray())[0] ) {
                    saveXyzNamespaceInFeature(feature, ((String[])rs.getArray("xyz_ns").getArray())[0] );
                    rs.close();
                    collection.getFeatures().add(feature);
                }else {
                    fails.add(new FeatureCollection.ModificationFailure().withId(fId).withMessage((handleUUID ? UPDATE_ERROR_UUID : UPDATE_ERROR_NOT_EXISTS)));
                }

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
                boolean success = false;
                ResultSet rs = null;
                final List<String> fIdList = new ArrayList<>();
                final List<String> uuidList = new ArrayList<>();

                fIdList.add(deleteId);
                if(handleUUID && puuid == null){
                    deleteStmtWithoutUUID.setArray(1, connection.createArrayOf("text", fIdList.toArray()));
                    deleteStmtWithoutUUID.setQueryTimeout(dbh.calculateTimeout());
                    success = deleteStmtWithoutUUID.execute();
                    if (success) {
                        rs = deleteStmtWithoutUUID.getResultSet();
                    }
                }else{
                    deleteStmt.setArray(1, connection.createArrayOf("text", fIdList.toArray()));
                    if(handleUUID) {
                        uuidList.add(puuid);
                        deleteStmt.setArray(2, connection.createArrayOf("text", uuidList.toArray()));
                    }
                    deleteStmt.setQueryTimeout(dbh.calculateTimeout());
                    success = deleteStmt.execute();
                    if (success) {
                        rs = deleteStmt.getResultSet();
                    }
                }

                if(!success || (rs.next() && !((boolean[])rs.getArray("success").getArray())[0] ) ) {
                    fails.add(new FeatureCollection.ModificationFailure().withId(deleteId).withMessage((handleUUID ? DELETE_ERROR_UUID : DELETE_ERROR_NOT_EXISTS)));
                }
                if (rs!=null) rs.close();

            } catch (Exception e) {
                fails.add(new FeatureCollection.ModificationFailure().withId(deleteId).withMessage(DELETE_ERROR_GENERAL));
                logException(e, traceItem, LOG_EXCEPTION_DELETE, table);
            }
        }

        deleteStmt.close();
        deleteStmtWithoutUUID.close();
    }
}
