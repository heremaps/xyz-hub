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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatabaseTransactionalWriter extends  DatabaseWriter{

    public static FeatureCollection insertFeatures(String schema, String table, String streamId, FeatureCollection collection,
                                                   List<Feature> inserts, Connection connection)
            throws SQLException, JsonProcessingException {

        boolean batchInsert = false;
        boolean batchInsertWithoutGeometry = false;

        final PreparedStatement insertStmt = createInsertStatement(connection,schema,table);
        final PreparedStatement insertWithoutGeometryStmt = createInsertWithoutGeometryStatement(connection,schema,table);

        insertStmt.setQueryTimeout(TIMEOUT);
        insertWithoutGeometryStmt.setQueryTimeout(TIMEOUT);

        for (int i = 0; i < inserts.size(); i++) {
            final Feature feature = inserts.get(i);

            final PGobject jsonbObject= featureToPGobject(feature);

            if (feature.getGeometry() == null) {
                insertWithoutGeometryStmt.setObject(1, jsonbObject);
                insertWithoutGeometryStmt.addBatch();
                batchInsertWithoutGeometry = true;
            } else {
                insertStmt.setObject(1, jsonbObject);

                final WKBWriter wkbWriter = new WKBWriter(3);
                Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                //Avoid NAN values
                assure3d(jtsGeometry.getCoordinates());
                insertStmt.setBytes(2, wkbWriter.write(jtsGeometry));

                insertStmt.addBatch();
                batchInsert = true;
            }
            collection.getFeatures().add(feature);
        }

        if (batchInsert) {
            insertStmt.executeBatch();
        }
        if (batchInsertWithoutGeometry) {
            insertWithoutGeometryStmt.executeBatch();
        }

        return collection;
    }

    public static FeatureCollection updateFeatures(String schema, String table, String streamId, FeatureCollection collection,
                                                   List<FeatureCollection.ModificationFailure> fails, List<Feature> updates,
                                                   Connection connection, boolean handleUUID)
            throws SQLException, JsonProcessingException {

        final PreparedStatement updateStmt = createUpdateStatement(connection, schema, table, handleUUID);
        final PreparedStatement updateWithoutGeometryStmt = createUpdateWithoutGeometryStatement(connection,schema,table,handleUUID);

        updateStmt.setQueryTimeout(TIMEOUT);
        updateWithoutGeometryStmt.setQueryTimeout(TIMEOUT);

        List<String> updateIdList = new ArrayList<>();
        List<String> updateWithoutGeometryIdList = new ArrayList<>();

        int[] batchUpdateResult = null;
        int[] batchUpdateWithoutGeometryResult = null;

        for (int i = 0; i < updates.size(); i++) {
            final Feature feature = updates.get(i);
            final String puuid = feature.getProperties().getXyzNamespace().getPuuid();

            if (feature.getId() == null) {
                throw new NullPointerException("id");
            }

            final PGobject jsonbObject= featureToPGobject(feature);

            if (feature.getGeometry() == null) {
                updateWithoutGeometryStmt.setObject(1, jsonbObject);
                updateWithoutGeometryStmt.setString(2, feature.getId());
                if(handleUUID)
                    updateWithoutGeometryStmt.setString(3, puuid);
                updateWithoutGeometryStmt.addBatch();

                updateWithoutGeometryIdList.add(feature.getId());
            } else {
                updateStmt.setObject(1, jsonbObject);

                final WKBWriter wkbWriter = new WKBWriter(3);
                Geometry jtsGeometry = feature.getGeometry().getJTSGeometry();
                //Avoid NAN values
                assure3d(jtsGeometry.getCoordinates());
                updateStmt.setBytes(2, wkbWriter.write(jtsGeometry));
                updateStmt.setString(3, feature.getId());

                if(handleUUID) {
                    updateStmt.setString(4, puuid);
                }
                updateStmt.addBatch();

                updateIdList.add(feature.getId());
            }
            collection.getFeatures().add(feature);
        }

        if (updateIdList.size() > 0) {
            batchUpdateResult = updateStmt.executeBatch();
            fillFailList(batchUpdateResult, fails, updateIdList, handleUUID);
        }
        if (updateWithoutGeometryIdList.size() > 0) {
            batchUpdateWithoutGeometryResult = updateWithoutGeometryStmt.executeBatch();
            fillFailList(batchUpdateWithoutGeometryResult, fails, updateWithoutGeometryIdList, handleUUID);
        }

        if(fails.size() > 0)
            throw new SQLException(UPDATE_ERROR_GENERAL);

        return collection;
    }

    protected static void deleteFeatures(String schema, String table, String streamId,
                                         List<FeatureCollection.ModificationFailure> fails, Map<String, String> deletes,
                                         Connection connection, boolean handleUUID)
            throws SQLException {

        final PreparedStatement batchDeleteStmt = deleteStmtSQLStatement(connection,schema,table,handleUUID);
        final PreparedStatement batchDeleteStmtWithoutUUID = deleteStmtSQLStatement(connection,schema,table,false);

        batchDeleteStmt.setQueryTimeout(TIMEOUT);
        batchDeleteStmtWithoutUUID.setQueryTimeout(TIMEOUT);

        Set<String> idsToDelete = deletes.keySet();

        List<String> deleteIdList = new ArrayList<>();
        List<String> deleteIdListWithoutUUID = new ArrayList<>();

        for (String deleteId : idsToDelete) {
            final String puuid = deletes.get(deleteId);

            if(handleUUID && puuid == null){
                batchDeleteStmtWithoutUUID.setString(1, deleteId);
                batchDeleteStmtWithoutUUID.addBatch();
                deleteIdListWithoutUUID.add(deleteId);
            }
            else {
                batchDeleteStmt.setString(1, deleteId);
                if(handleUUID) {
                    batchDeleteStmt.setString(2, puuid);
                }
                deleteIdList.add(deleteId);
                batchDeleteStmt.addBatch();
            }
        }

        int[] batchDeleteStmtResult = batchDeleteStmt.executeBatch();
        int[] batchDeleteStmtWithoutUUIDResult = batchDeleteStmtWithoutUUID.executeBatch();

        fillFailList(batchDeleteStmtResult, fails, deleteIdList, handleUUID);
        fillFailList(batchDeleteStmtWithoutUUIDResult, fails, deleteIdListWithoutUUID, handleUUID);

        if(fails.size() > 0)
            throw new SQLException(DELETE_ERROR_GENERAL);
    }

    private static void fillFailList(int[] batchResult, List<FeatureCollection.ModificationFailure> fails,  List<String> idList, boolean handleUUID){
        for (int i= 0; i < batchResult.length; i++) {
            if(batchResult[i] == 0 ) {
                fails.add(new FeatureCollection.ModificationFailure().withId(idList.get(i)).withMessage(
                    (handleUUID ? UPDATE_ERROR_UUID : UPDATE_ERROR_NOT_EXISTS)));
            }
        }
    }
}
