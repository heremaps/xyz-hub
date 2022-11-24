/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabaseStreamWriter extends DatabaseWriter{

    protected static void deleteFeatures( DatabaseHandler dbh, ModifyFeaturesEvent event, TraceItem traceItem,
                                         List<FeatureCollection.ModificationFailure> fails, Map<String, String> deletes,
                                         Connection connection)
            throws SQLException {
        boolean handleUUID = event.getEnableUUID();
        String schema = dbh.config.getDatabaseSettings().getSchema();
        String table = dbh.config.readTableFromEvent(event);

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
                //TODO: Handle SQL state "42P01"? (see: #insertFeatures())
                fails.add(new FeatureCollection.ModificationFailure().withId(deleteId).withMessage(DELETE_ERROR_GENERAL));
                logException(e, traceItem, LOG_EXCEPTION_DELETE, dbh, event);
            }
        }

        deleteStmt.close();
        deleteStmtWithoutUUID.close();
    }
}
