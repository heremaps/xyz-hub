/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.util.runtime.FunctionRuntime;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModifySpace extends ExtendedSpace<ModifySpaceEvent, SuccessResponse> {

    /**
     * Main schema for xyz-relevant configurations.
     */
    public static final String XYZ_CONFIG_SCHEMA = "xyz_config";
    public static final String IDX_STATUS_TABLE = "xyz_idxs_status";
    public static final String IDX_STATUS_TABLE_FQN = XYZ_CONFIG_SCHEMA + "." + IDX_STATUS_TABLE;
    public static final String SPACE_META_TABLE = "space_meta";
    public static final String SPACE_META_TABLE_FQN = XYZ_CONFIG_SCHEMA + "." + SPACE_META_TABLE;
    public static final String I_SEQUENCE_SUFFIX = "_i_seq";
    private Operation operation;
    private String spaceId;
    private DatabaseMaintainer dbMaintainer;
    private boolean dryRun;

    public ModifySpace(ModifySpaceEvent event) throws SQLException, ErrorResponseException {
        super(event);
        dryRun = event.isDryRun();
        validateModifySpaceEvent(event);
        setUseReadReplica(false);
        operation = event.getOperation();
        spaceId = event.getSpace();
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
        if (event.getOperation() == CREATE || event.getOperation() == UPDATE) {
            List<SQLQuery> queries = new ArrayList<>();
            final String table = getDefaultTable(event);

            if (event.getSpaceDefinition() != null && event.getOperation() == CREATE)
                //Add space table creation queries
                queries.addAll(buildCreateSpaceTableQueries(getSchema(), table));

            //Write idx related data
            queries.addAll(buildSearchablePropertiesUpsertQueries(event));
            //Write metadata
            queries.add(buildSpaceMetaUpsertQuery(event));

            return SQLQuery.batchOf(queries).withLock(table);
        }
        else if (event.getOperation() == DELETE)
            return buildCleanUpQuery(event);

        return null;
    }

    @Override
    public SuccessResponse write(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
        if (dryRun)
            return new SuccessResponse().withStatus("OK");

        SuccessResponse response = super.write(dataSourceProvider);
        if (operation != Operation.DELETE)
            getDbMaintainer().maintainSpace(FunctionRuntime.getInstance().getStreamId(), getSchema(), spaceId);
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

    public SQLQuery buildSpaceMetaUpsertQuery(ModifySpaceEvent event) throws SQLException {
          SQLQuery q = new SQLQuery("INSERT INTO "+ SPACE_META_TABLE_FQN +" as s_m VALUES (#{spaceid},#{schema},#{table},(#{extend})::json)" +
                  "  ON CONFLICT (id,schem)" +
                  "  DO " +
                  "  UPDATE" +
                  "     SET meta = COALESCE(s_m.meta,'{}'::jsonb) || (#{extend})::jsonb" +
                  "  WHERE 1=1" +
                  "     AND s_m.id = #{spaceid}" +
                  "     AND s_m.schem = #{schema};");

          q.setNamedParameter("spaceid", event.getSpaceDefinition() == null ? "" : event.getSpaceDefinition().getId());
          q.setNamedParameter(SCHEMA, getSchema());
          q.setNamedParameter("extend", buildExtendedTablesJSON(event).toString()); //TODO: Only use one field here for the most down base space instead
          q.setNamedParameter(TABLE, getDefaultTable(event));

          return q;
      }



    public List<SQLQuery> buildSearchablePropertiesUpsertQueries(ModifySpaceEvent event) throws SQLException {
        String sourceTable = isExtendedSpace(event) ? getExtendedTable(event) : getDefaultTable(event);

        SQLQuery idx_ext_q = new SQLQuery(
                "SELECT jsonb_set(idx_manual,'{searchableProperties}',"+
                        "         idx_manual->'searchableProperties' ||"+
                        // Copy possible existing auto-indices into searchableProperties Config of extended space
                        "        (SELECT COALESCE(jsonb_object_agg(key,value), '{}')"+
                        "            FROM("+
                        "                SELECT idx_property as key, true as value"+
                        "                    FROM xyz_index_list_all_available('public',#{extended_table})"+
                        "                WHERE src='a'"+
                        "            )A"+
                        "        ),"+
                        "        true) "+
                        "FROM "+ IDX_STATUS_TABLE_FQN +
                        "    WHERE spaceid=#{extended_table} ");

        idx_ext_q.setNamedParameter("extended_table", sourceTable);


        List<SQLQuery> upsertQueries = new ArrayList<>();
        upsertQueries.add(buildUpsertIndexQuery(event, idx_ext_q));

        if (event.getOperation() == UPDATE)
            upsertQueries.add(buildUpdateReferencedTablesQuery(event, idx_ext_q));

        return upsertQueries;
    }

    private static SQLQuery buildGetManualIndicesQuery(ModifySpaceEvent event) throws SQLException {
        Space spaceDefinition = event.getSpaceDefinition();
        Map<String, Boolean> searchableProperties = spaceDefinition.getSearchableProperties();
        List<List<Object>> sortableProperties = spaceDefinition.getSortableProperties();

        String idx_manual_json;
        SQLQuery idx_q;
        try {
            idx_manual_json = (new ObjectMapper()).writeValueAsString(new IdxManual(searchableProperties, sortableProperties));
            idx_q = new SQLQuery("select (#{idx_manual})::jsonb");
            idx_q.setNamedParameter("idx_manual", idx_manual_json);
        } catch (JsonProcessingException e) {
            throw new SQLException("buildSearchablePropertiesUpsertQuery", e);
        }
        return idx_q;
    }

    private SQLQuery buildUpdateReferencedTablesQuery(ModifySpaceEvent event, SQLQuery idx_ext_q) {
        //Update possible existing delta tables
        return new SQLQuery("UPDATE " + IDX_STATUS_TABLE_FQN + " "
                + "             SET schem = #{schema}, "
                + "    			idx_manual = (${{idx_manual_sub2}}), "
                + "				idx_creation_finished = false"
                + "		WHERE " +
                "           array_position(" +
                "               (SELECT ARRAY_AGG(h_id) " +
                "                   FROM " + SPACE_META_TABLE_FQN + " as b " +
                "               WHERE 1=1" +
                "                   AND b.meta->'extends'->>'extendedTable' = #{table}" +
                "                   AND b.schem = #{schema}" +
                "               ), spaceid" +
                "           ) > 0" +
                "           AND schem = #{schema};" )
            .withQueryFragment("idx_manual_sub2", idx_ext_q)
            .withNamedParameter(TABLE, getDefaultTable(event))
            .withNamedParameter(SCHEMA, getSchema());
    }

    private SQLQuery buildUpsertIndexQuery(ModifySpaceEvent event, SQLQuery idx_ext_q) throws SQLException {
        Space spaceDefinition = event.getSpaceDefinition();

        Boolean enableAutoIndexing = spaceDefinition.isEnableAutoSearchableProperties();
        //Disable Autoindexing for spaces which are extending another space.
        enableAutoIndexing = (isExtendedSpace(event) ? Boolean.FALSE : enableAutoIndexing);

        //Update xyz_idx_status table with searchableProperties information
        SQLQuery upsertIDX = new SQLQuery("INSERT INTO  "+ IDX_STATUS_TABLE_FQN +" as x_s (spaceid, schem, idx_creation_finished, idx_manual, auto_indexing) "
                + "		VALUES (#{table}, #{schema} , false, (${{idx_manual_sub}}), #{auto_indexing} ) "
                + "ON CONFLICT (spaceid) DO "
                + "		UPDATE SET schem = #{schema}, "
                + "    			idx_manual = (${{idx_manual_sub}}), "
                + "				idx_creation_finished = false,"
                + "             auto_indexing = #{auto_indexing}"
                + "		WHERE x_s.spaceid = #{table} AND x_s.schem = #{schema};");

        SQLQuery getManualIndicesQuery = buildGetManualIndicesQuery(event);
        upsertIDX.setQueryFragment("idx_manual_sub", !isExtendedSpace(event) ? getManualIndicesQuery : idx_ext_q);
        upsertIDX.setNamedParameter(TABLE, getDefaultTable(event));
        upsertIDX.setNamedParameter(SCHEMA, getSchema());
        upsertIDX.setNamedParameter("auto_indexing", enableAutoIndexing);
        return upsertIDX;
    }

    public SQLQuery buildCleanUpQuery(ModifySpaceEvent event) {
        String table = getDefaultTable(event);
        SQLQuery q = new SQLQuery("${{deleteMetadata}} ${{deleteIndexStatus}} ${{dropTable}} ${{dropISequence}} ${{dropVersionSequence}}")
            .withQueryFragment(
                "deleteMetadata",
                "DELETE FROM ${configSchema}.${spaceMetaTable} WHERE h_id = #{table} AND schem = #{schema};"
            )
            .withQueryFragment(
                "deleteIndexStatus",
                "DELETE FROM ${configSchema}.${idxStatusTable} WHERE spaceid = #{table} AND schem = #{schema};"
            )
            .withQueryFragment("dropTable", "DROP TABLE IF EXISTS ${schema}.${table};")
            .withQueryFragment("dropISequence", "DROP SEQUENCE IF EXISTS ${schema}.${iSequence};")
            .withQueryFragment("dropVersionSequence", "DROP SEQUENCE IF EXISTS ${schema}.${versionSequence};");

        return q
            .withVariable(SCHEMA, getSchema())
            .withVariable(TABLE, table)
            .withNamedParameter(SCHEMA, getSchema())
            .withNamedParameter(TABLE, table)
            .withVariable("configSchema", XYZ_CONFIG_SCHEMA)
            .withVariable("idxStatusTable", IDX_STATUS_TABLE)
            .withVariable("spaceMetaTable", SPACE_META_TABLE)
            .withVariable("iSequence", table + I_SEQUENCE_SUFFIX)
            .withVariable("versionSequence", getDefaultTable(event) + VERSION_SEQUENCE_SUFFIX);
    }



    private static class IdxManual {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Map<String, Boolean> searchableProperties;
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<List<Object>> sortableProperties;

        IdxManual( Map<String, Boolean> searchableProperties, List<List<Object>> sortableProperties ){
            this.searchableProperties = searchableProperties;
            this.sortableProperties = sortableProperties;
            if(this.searchableProperties != null) {
                //Remove entries with null values
                Map<String, Boolean> clearedSearchableProperties = new HashMap<>(searchableProperties);
                for (String property : searchableProperties.keySet()) {
                    if (searchableProperties.get(property) == null)
                        clearedSearchableProperties.remove(property);
                }
                this.searchableProperties = clearedSearchableProperties;
            }
            if(this.sortableProperties != null && this.searchableProperties == null) {
                this.searchableProperties = new HashMap<>();
            }
        }
    }

    public DatabaseMaintainer getDbMaintainer() {
        return dbMaintainer;
    }

    public void setDbMaintainer(DatabaseMaintainer dbMaintainer) {
        this.dbMaintainer = dbMaintainer;
    }

    public ModifySpace withDbMaintainer(DatabaseMaintainer dbMaintainer) {
        setDbMaintainer(dbMaintainer);
        return this;
    }
}
