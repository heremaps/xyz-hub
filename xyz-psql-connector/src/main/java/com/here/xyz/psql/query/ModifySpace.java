/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.XyzSerializable.Mappers.DEFAULT_MAPPER;
import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.DELETE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.UPDATE;
import static com.here.xyz.psql.query.helpers.versioning.GetNextVersion.VERSION_SEQUENCE_SUFFIX;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueries;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import static com.here.xyz.util.db.ConnectorParameters.TableLayout;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;

public class ModifySpace extends ExtendedSpace<ModifySpaceEvent, SuccessResponse> {

    /**
     * Main schema for xyz-relevant configurations.
     */
    public static final String XYZ_CONFIG_SCHEMA = "xyz_config";
    public static final String SPACE_META_TABLE = "space_meta";
    public static final String SPACE_META_TABLE_FQN = XYZ_CONFIG_SCHEMA + "." + SPACE_META_TABLE;
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
        TableLayout tableLayout = getTableLayout();

        if(tableLayout.isOld()){
            if (event.getOperation() == CREATE || event.getOperation() == UPDATE) {
                List<SQLQuery> queries = new ArrayList<>();
                final String table = getDefaultTable(event);

                if (event.getSpaceDefinition() != null && event.getOperation() == CREATE) {
                    //Add space table creation queries
                    List<OnDemandIndex> activatedSearchableProperties
                            = getActivatedSearchableProperties(event.getSpaceDefinition().getSearchableProperties());

                    queries.addAll(buildCreateSpaceTableQueries(getSchema(), table, activatedSearchableProperties,
                            event.getSpace(), tableLayout));
                }

                //Write metadata
                queries.add(buildSpaceMetaUpsertQuery(event));

                return SQLQuery.batchOf(queries).withLock(table);
            }
            else if (event.getOperation() == DELETE) {
                return SQLQuery.batchOf(buildCleanUpQuery(event, getSchema(), getDefaultTable(event), VERSION_SEQUENCE_SUFFIX, tableLayout));
            }
        }else if(tableLayout.equals(NEW_LAYOUT)){
            if (event.getOperation() == CREATE) {
                final String table = getDefaultTable(event);
                List<SQLQuery> queries = new ArrayList<>(buildCreateSpaceTableQueries(getSchema(), table,
                        //No OnDemandIndices are supported in V2
                        null, event.getSpace(), tableLayout));
                return SQLQuery.batchOf(queries).withLock(table);
            }
            else if (event.getOperation() == DELETE)
                return SQLQuery.batchOf(buildCleanUpQuery(event, getSchema(), getDefaultTable(event), VERSION_SEQUENCE_SUFFIX, tableLayout));
        }
        throw new IllegalArgumentException("Unsupported Table Layout: " + tableLayout);
    }

    @Override
    public SuccessResponse write(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
        if (dryRun)
            return new SuccessResponse().withStatus("OK");

        //TODO: Delete branch tables as part of "prune"-operation
        //if (operation == DELETE)
        //  new BranchManager(getDataSourceProvider(), FunctionRuntime.getInstance().getStreamId(), spaceId, getSchema(), rootTable)
        //      .deleteAllBranchTables();

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

    private static final String INTERMEDIATE_TABLE = "intermediateTable";
    private static final String EXTENDED_TABLE = "extendedTable";


    @Deprecated
    private ObjectNode buildExtendedTablesJSON(ModifySpaceEvent event) {
        if (!isExtendedSpace(event)) {
            return DEFAULT_MAPPER.get().createObjectNode();
        }

        ObjectNode extendedTables = DEFAULT_MAPPER.get().createObjectNode();
        extendedTables.put(EXTENDED_TABLE, getExtendedTable(event));
        if (is2LevelExtendedSpace(event))
            extendedTables.put(INTERMEDIATE_TABLE, getIntermediateTable(event));

        ObjectNode jsonObject = DEFAULT_MAPPER.get().createObjectNode();
        jsonObject.put("extends", extendedTables);
        return jsonObject;
    }

    public SQLQuery buildSpaceMetaUpsertQuery(ModifySpaceEvent event) {
          SQLQuery q = new SQLQuery("INSERT INTO "+ SPACE_META_TABLE_FQN +" as s_m VALUES (#{spaceid},#{schema},#{table},(#{extend})::json)" +
                  "  ON CONFLICT (id,schem)" +
                  "  DO " +
                  "  UPDATE" +
                  "     SET meta = COALESCE(s_m.meta - 'extends','{}'::jsonb ,'{}'::jsonb) || (#{extend})::jsonb" +
                  "  WHERE 1=1" +
                  "     AND s_m.id = #{spaceid}" +
                  "     AND s_m.schem = #{schema};");

          q.setNamedParameter("spaceid", event.getSpaceDefinition() == null ? "" : event.getSpaceDefinition().getId());
          q.setNamedParameter(SCHEMA, getSchema());
          q.setNamedParameter("extend", buildExtendedTablesJSON(event).toString()); //TODO: Only use one field here for the most down base space instead
          q.setNamedParameter(TABLE, getDefaultTable(event));

          return q;
    }
}
