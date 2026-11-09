/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.psql.query;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.DELETE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.UPDATE;
import static com.here.xyz.psql.query.helpers.versioning.GetNextVersion.VERSION_SEQUENCE_SUFFIX;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueries;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCleanUpQuery;
import static com.here.xyz.util.db.pg.IndexHelper.getActivatedSearchableProperties;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.IndexHelper;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ModifySpace extends ExtendedSpace<ModifySpaceEvent, SuccessResponse> {
    private static final Logger logger = LogManager.getLogger(ModifySpace.class);

    public static final String I_SEQUENCE_SUFFIX = "_i_seq";
    private Operation operation;
    private String spaceId;
    private boolean dryRun;
    private String rootTable;

    public ModifySpace(ModifySpaceEvent event) throws SQLException, ErrorResponseException {
        super(event);
        dryRun = event.isDryRun();
        validateModifySpaceEvent(event);
        setUseReadReplica(false);
        operation = event.getOperation();
        spaceId = event.getSpace();
        rootTable = getDefaultTable(event);
    }

    private void validateModifySpaceEvent(ModifySpaceEvent event) throws ErrorResponseException {
        final ConnectorParameters connectorParameters = ConnectorParameters.fromEvent(event);
        final boolean connectorSupportsAI = connectorParameters.isAutoIndexing();

        if ((ModifySpaceEvent.Operation.UPDATE == event.getOperation() || ModifySpaceEvent.Operation.CREATE == event.getOperation())
            && connectorParameters.isPropertySearch()) {
            int onDemandLimit = connectorParameters.getOnDemandIdxLimit();
            int onDemandCounter = 0;
            if (event.getSpaceDefinition().getSearchableProperties() != null) {

                for (String property : event.getSpaceDefinition().getSearchableProperties().keySet()) {
                    if (event.getSpaceDefinition().getSearchableProperties().get(property) != null
                        && event.getSpaceDefinition().getSearchableProperties().get(property) == Boolean.TRUE)
                        onDemandCounter++;

                    if ( onDemandCounter > onDemandLimit)
                        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "On-Demand-Indexing - Maximum permissible: " + onDemandLimit
                            + " searchable properties per space!");

                    if (property.contains("'"))
                        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "On-Demand-Indexing [" + property
                            + "] - Character ['] not allowed!");

                    if (property.contains("\\"))
                        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "On-Demand-Indexing [" + property
                            + "] - Character [\\] not allowed!");

                    if (event.getSpaceDefinition().isEnableAutoSearchableProperties() != null
                        && event.getSpaceDefinition().isEnableAutoSearchableProperties()
                        && !connectorSupportsAI)
                        throw new ErrorResponseException(ILLEGAL_ARGUMENT,
                            "Connector does not support Auto-indexing!");
                }
            }

            if(event.getSpaceDefinition().getSortableProperties() != null )
            { //TODO: eval #index limits, parameter validation
                if( event.getSpaceDefinition().getSortableProperties().size() + onDemandCounter > onDemandLimit )
                    throw new ErrorResponseException(ILLEGAL_ARGUMENT,
                        "On-Demand-Indexing - Maximum permissible: " + onDemandLimit + " sortable + searchable properties per space!");

                for( List<Object> l : event.getSpaceDefinition().getSortableProperties() )
                    for( Object p : l )
                    { String property = p.toString();
                        if( property.contains("\\") || property.contains("'") )
                            throw new ErrorResponseException(ILLEGAL_ARGUMENT,
                                "On-Demand-Indexing [" + property + "] - Characters ['\\] not allowed!");
                    }
            }
        }
    }

    @Override
    protected SQLQuery buildQuery(ModifySpaceEvent event) throws SQLException {
        if(getTableLayout().equals(ConnectorParameters.TableLayout.OLD_LAYOUT)){
            //NOTE: An UPDATE has nothing to execute, see #write()
            if (event.getOperation() == CREATE) {
                List<SQLQuery> queries = new ArrayList<>();
                final String table = getDefaultTable(event);

                if (event.getSpaceDefinition() != null) {
                    //Add space table creation queries
                    List<OnDemandIndex> activatedSearchableProperties
                            = getActivatedSearchableProperties(event.getSpaceDefinition().getSearchableProperties());

                    queries.addAll(buildCreateSpaceTableQueries(getSchema(), table, activatedSearchableProperties,
                            event.getSpace(), OLD_LAYOUT));
                }

                return SQLQuery.batchOf(queries).withLock(table);
            }
            else if (event.getOperation() == DELETE) {
                //The lock serializes the deletion with the creation and a materialization of the same table
                final String table = getDefaultTable(event);
                return SQLQuery.batchOf(buildCleanUpQuery(getSchema(), table, VERSION_SEQUENCE_SUFFIX, OLD_LAYOUT)).withLock(table);
            }
        }else if(getTableLayout().equals(ConnectorParameters.TableLayout.NEW_LAYOUT)){
            if (event.getOperation() == CREATE) {
                final String table = getDefaultTable(event);
                List<SQLQuery> queries = new ArrayList<>(buildCreateSpaceTableQueries(getSchema(), table,
                        //No OnDemandIndices are supported in V2
                        null, event.getSpace(), NEW_LAYOUT));
                return SQLQuery.batchOf(queries).withLock(table);
            }
            else if (event.getOperation() == DELETE) {
                final String table = getDefaultTable(event);
                return SQLQuery.batchOf(buildCleanUpQuery(getSchema(), table, VERSION_SEQUENCE_SUFFIX, NEW_LAYOUT)).withLock(table);
            }
        }
        throw new IllegalArgumentException("Unsupported Table Layout: " + getTableLayout());
    }

    @Override
    public SuccessResponse write(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
        //The reserved names belong to the empty-space template, which no space may create, overwrite or drop
        if (LazySpaceTables.isReserved(rootTable)) {
            if (operation == CREATE)
                throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Reserved table name");
            if (operation == DELETE) {
                logger.error("Space {} has the reserved table name {}, its deletion drops no table.", spaceId, rootTable);
                return new SuccessResponse().withStatus("OK");
            }
        }

        if (dryRun)
            return new SuccessResponse().withStatus("OK");

        //An OLD_LAYOUT UPDATE has no SQL to execute, the space table only gets created on CREATE
        if (operation == UPDATE && getTableLayout() == OLD_LAYOUT)
            return new SuccessResponse().withStatus("OK");

        SuccessResponse response = super.write(dataSourceProvider);

        return response;
    }

    @Override
    protected SuccessResponse handleWrite(int[] rowCounts) {
        return new SuccessResponse().withStatus("OK");
    }

    @Override
    public SuccessResponse handle(ResultSet rs) throws SQLException {
        return null;
    }
}
