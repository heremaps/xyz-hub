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

package com.here.xyz.psql.query;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.DELETE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.UPDATE;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.XyzResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

public class ModifySpace extends ExtendedSpace<ModifySpaceEvent, XyzResponse> {

    public static final String IDX_STATUS_TABLE = "xyz_config.xyz_idxs_status";
    private static final String SPACE_META_TABLE = "xyz_config.space_meta";
    public ModifySpace(ModifySpaceEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
        super(event, dbHandler);
        setUseReadReplica(false);
    }

    @Override
    protected SQLQuery buildQuery(ModifySpaceEvent event) throws SQLException {
        if (event.getOperation() == CREATE || event.getOperation() == UPDATE) {
            SQLQuery q = new SQLQuery("${{searchableUpsert}} ${{spaceMetaUpsert}}");

            //Write idx related data
            SQLQuery searchableUpsertQuery = buildSearchablePropertiesUpsertQuery(event);
            q.setQueryFragment("searchableUpsert", searchableUpsertQuery);

            //Write metadata
            SQLQuery spaceMetaUpsertQuery = buildSpaceMetaUpsertQuery(event);
            q.setQueryFragment("spaceMetaUpsert", spaceMetaUpsertQuery);

            return q;
        }
        else if (event.getOperation() == DELETE)
            return buildCleanUpQuery(event);

        return null;
    }

    @Override
    public XyzResponse handle(ResultSet rs) throws SQLException {
        return null;
    }

    private static final String INTERMEDIATE_TABLE = "intermediateTable";
    private static final String EXTENDED_TABLE = "extendedTable";

    @Deprecated
    private JSONObject buildExtendedTablesJSON(ModifySpaceEvent event) {
        if (!isExtendedSpace(event))
            return new JSONObject().put("extends", (Object) null);

        Map<String, String> extendedTables = new HashMap<String, String>() {{
           put(EXTENDED_TABLE, getExtendedTable(event));
           if (is2LevelExtendedSpace(event))
               put(INTERMEDIATE_TABLE, getIntermediateTable(event));
        }};
        return new JSONObject().put("extends", extendedTables);
    }

    public SQLQuery buildSpaceMetaUpsertQuery(ModifySpaceEvent event) throws SQLException {
          SQLQuery q = new SQLQuery("INSERT INTO "+ SPACE_META_TABLE +" as s_m VALUES (#{spaceid},#{schema},#{table},(#{extend})::json)" +
                  "  ON CONFLICT (id,schem)" +
                  "  DO " +
                  "  UPDATE" +
                  "     SET meta = COALESCE(s_m.meta,'{}'::jsonb) || (#{extend})::jsonb" +
                  "  WHERE 1=1" +
                  "     AND s_m.id = #{spaceid}" +
                  "     AND s_m.schem = #{schema};");

          q.setNamedParameter("spaceid", event.getSpaceDefinition() == null ? "" : event.getSpaceDefinition().getId());
          q.setNamedParameter("schema", getSchema());
          q.setNamedParameter("extend", buildExtendedTablesJSON(event).toString()); //TODO: Only use one field here for the most down base space instead
          q.setNamedParameter("table", getDefaultTable(event));

          return q;
      }



    public SQLQuery buildSearchablePropertiesUpsertQuery(ModifySpaceEvent event) throws SQLException
    {
        Space spaceDefinition = event.getSpaceDefinition();
        Map<String, Boolean> searchableProperties = spaceDefinition.getSearchableProperties();
        List<List<Object>> sortableProperties = spaceDefinition.getSortableProperties();
        Boolean enableAutoIndexing = spaceDefinition.isEnableAutoSearchableProperties() == Boolean.TRUE && !isExtendedSpace(event);

        String idx_manual_json;
        SQLQuery q = new SQLQuery("${{upsertIDX}} ${{updateReferencedTables}}");
        SQLQuery idx_q;
        SQLQuery idx_ext_q;

        try {
            /** sortable and searchableProperties taken from space-definition */
            idx_manual_json = (new ObjectMapper()).writeValueAsString(new IdxManual(searchableProperties, sortableProperties));
            idx_q = new SQLQuery("select (#{idx_manual})::jsonb");
            idx_q.setNamedParameter("idx_manual", idx_manual_json);
        } catch (JsonProcessingException e) {
            throw new SQLException("buildSearchablePropertiesUpsertQuery", e);
        }

        String sourceTable = isExtendedSpace(event) ? getExtendedTable(event) : getDefaultTable(event);

        idx_ext_q = new SQLQuery(
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
                        "FROM "+IDX_STATUS_TABLE+
                        "    WHERE spaceid=#{extended_table} ");

        idx_ext_q.setNamedParameter("extended_table", sourceTable);


        /* update xyz_idx_status table with searchableProperties information */
        SQLQuery upsertIDX = new SQLQuery("INSERT INTO  "+IDX_STATUS_TABLE+" as x_s (spaceid, schem, idx_creation_finished, idx_manual, auto_indexing) "
                + "		VALUES (#{table}, #{schema} , false, (${{idx_manual_sub}}), #{auto_indexing} ) "
                + "ON CONFLICT (spaceid) DO "
                + "		UPDATE SET schem=#{schema}, "
                + "    			idx_manual = (${{idx_manual_sub}}), "
                + "				idx_creation_finished = false,"
                + "             auto_indexing = #{auto_indexing}"
                + "		WHERE x_s.spaceid = #{table} AND x_s.schem=#{schema};");

        upsertIDX.setQueryFragment("idx_manual_sub", !isExtendedSpace(event) ? idx_q : idx_ext_q);
        upsertIDX.setNamedParameter("table", getDefaultTable(event));
        upsertIDX.setNamedParameter("schema", getSchema());
        upsertIDX.setNamedParameter("auto_indexing", enableAutoIndexing);

        SQLQuery updateReferencedTables = new SQLQuery();
        if (event.getOperation() == UPDATE) {
            /* update possible existing delta tables */
            updateReferencedTables = new SQLQuery("UPDATE " + IDX_STATUS_TABLE + " "
                    + "             SET schem=#{schema}, "
                    + "    			idx_manual = (${{idx_manual_sub2}}), "
                    + "				idx_creation_finished = false"
                    + "		WHERE " +
                    "           array_position(" +
                    "               (SELECT ARRAY_AGG(h_id) " +
                    "                   FROM " + SPACE_META_TABLE + " as b " +
                    "               WHERE 1=1" +
                    "                   AND b.meta->'extends'->>'extendedTable' = #{table}" +
                    "                   AND b.schem = #{schema}" +
                    "               ), spaceid" +
                    "           ) > 0" +
                    "           AND schem = #{schema};" );

            updateReferencedTables.setQueryFragment("idx_manual_sub2", idx_ext_q);
        }

        q.setQueryFragment("upsertIDX",upsertIDX);
        q.setQueryFragment("updateReferencedTables",updateReferencedTables);

        return q;
    }

    public SQLQuery buildCleanUpQuery(ModifySpaceEvent event)
    {
        SQLQuery q = new SQLQuery("DELETE FROM "+SPACE_META_TABLE+" WHERE h_id=#{table} AND schem=#{schema};");
        q.append("DELETE FROM "+IDX_STATUS_TABLE+" WHERE spaceid=#{table} AND schem=#{schema};");
        q.append("DROP TABLE IF EXISTS ${schema}.${table};");
        q.append("DROP TABLE IF EXISTS ${schema}.${hsttable};");
        q.append("DROP SEQUENCE IF EXISTS ${schema}.${hsttable_seq};");
        q.append("DROP SEQUENCE IF EXISTS ${schema}.${table_seq};");

        q.setNamedParameter("table", getDefaultTable(event));
        q.setNamedParameter("schema", getSchema());
        return q;
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
}
