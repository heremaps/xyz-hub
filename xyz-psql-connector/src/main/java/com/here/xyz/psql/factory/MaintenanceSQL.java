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

package com.here.xyz.psql.factory;

public class MaintenanceSQL {
    /**
     * Main schema for xyz-relevant configurations.
     */
    public static final String XYZ_CONFIG_SCHEMA = "xyz_config";

    /**
     * Tables for database wide configurations, which belong to XYZ_CONFIG_SCHEMA
     */
    private static String XYZ_CONFIG_DB_STATUS = "db_status";
    private static String XYZ_CONFIG_SPACE_META_TABLE = "space_meta";
    public static String XYZ_CONFIG_IDX_TABLE = "xyz_idxs_status";
    private static String XYZ_CONFIG_STORAGE_TABLE = "xyz_storage";
    private static String XYZ_CONFIG_SPACE_TABLE = "xyz_space";

    /**
     * Check if all required database extensions are installed
     */
    public static String generateCheckExtensionsSQL(boolean hasPropertySearch, String user, String db){
        return "SELECT COALESCE(array_agg(extname) @> '{postgis,postgis_topology,tsm_system_rows,aws_s3"
                + (hasPropertySearch ? ",dblink" : "") + "}', false) as all_ext_av,"
                + "COALESCE(array_agg(extname)) as ext_av, "
                + "(select has_database_privilege('"+user+"', '"+db+"', 'CREATE')) as has_create_permissions "
                + "FROM ("
                + "	SELECT extname FROM pg_extension"
                + "		WHERE extname in('postgis','postgis_topology','tsm_system_rows','dblink','aws_s3')"
                + "	order by extname"
                + ") A";
    }

    /**
     * Check if all required schemas and tables are created
     */
    public static String generateCheckSchemasAndIdxTableSQL(String schema){
        return "SELECT array_agg(nspname) @> ARRAY['" + schema + "'] as main_schema, "
                + " array_agg(nspname) @> ARRAY['xyz_config'] as config_schema, "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_IDX_TABLE+"') IS NOT NULL) as idx_table), "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_DB_STATUS+"') IS NOT NULL) as db_status_table), "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_SPACE_META_TABLE+"') IS NOT NULL) as space_meta_table) "
                + "FROM( "
                + "	SELECT nspname::text FROM pg_catalog.pg_namespace "
                + "		WHERE nspowner <> 1 "
                + ")a ";
    }

    /**
     * Check if the xyz_ext_version() function is installed, which is used for
     * versioning the sql-functions.
     */
    public static String generateEnsureExtVersionSQL(String schema){
        return "SELECT routine_name FROM information_schema.routines "
                + "WHERE routine_type='FUNCTION' AND specific_schema='" + schema
                + "' AND routine_name='xyz_ext_version';";
    }

    /**
     * Retrieve version of SQL-functions
     */
    public static String generateEnsureCorrectVersionSQL(String schema){
        return "select " + schema + ".xyz_ext_version()";
    }

    /**
     * Install all required database extensions
     */
    public static String generateMandatoryExtensionSQL(){
        return "DO $$ " +
                "BEGIN " +
                "CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public; " +
                "CREATE EXTENSION IF NOT EXISTS postgis_topology; " +
                "CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public; " +
                "CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public; " +
                "CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE; " +
                "EXCEPTION WHEN OTHERS THEN " +
                "RAISE NOTICE 'Not able to install all extensions'; " +
                "END; " +
                "$$;";
    }

    /**
     * Create main XYZ-schema
     */
    public static String generateMainSchemaSQL(String schema){
        return "CREATE SCHEMA IF NOT EXISTS \"" + schema + "\";";
    }

    /**
     * Set search_path
     */
    public static String generateSearchPathSQL(String schema){
        return "SET search_path=" + schema + ",h3,public,topology;";
    }

    /**
     * Set statement_timeout
     */
    public static String generateTimeoutSQL(int seconds){
        return  "SET statement_timeout = "+ seconds * 1000 + ";";
    }

    /**
     * Trigger indexing (auto+on-demand) by using dblink
     */
    public static String generateIDXSQL(String schema, String user, String password, String database, String host, int port, int mode){
        return "SELECT * from xyz_create_idxs_over_dblink('" + schema + "',50, 0,"+mode+",ARRAY['wikvaya','" + user + "'],'" + user
                + "','" + password + "','" + database + "','" + host + "'," + port + ",'" + schema + ",h3,public,topology')";
    }

    /** Create XYZ_CONFIG_SCHEMA and required system tables */
    public static String generateInitializationEntry =
            "INSERT INTO xyz_config.db_status as a (dh_schema, connector_id, initialized, extensions, script_versions, maintenance_status) " +
                    "VALUES (?,?,?,?,?::jsonb,?::jsonb)"
                    + "ON CONFLICT (dh_schema,connector_id) DO "
                    + "UPDATE SET extensions=?,"
                    + "    		  script_versions=?::jsonb,"
                    + "    		  initialized=?"
                    + "		WHERE a.dh_schema=? AND a.connector_id=?";

    public static String updateConnectorStatusBeginMaintenance =
            "UPDATE xyz_config.db_status SET maintenance_status=" +
                    " CASE" +
                    "  WHEN maintenance_status IS NULL THEN" +
                    "       jsonb_set('{}'::jsonb || '{\"AUTO_INDEXING\":{\"maintenanceRunning\" : []}}', '{AUTO_INDEXING,maintenanceRunning}',  ?::jsonb || '[]'::jsonb)" +
                    "  WHEN maintenance_status->'AUTO_INDEXING'->'maintenanceRunning' IS NULL THEN" +
                    "       jsonb_set(maintenance_status || '{\"AUTO_INDEXING\":{\"maintenanceRunning\" : []}}', '{AUTO_INDEXING,maintenanceRunning}', ?::jsonb || '[]'::jsonb)" +
                    "   ELSE" +
                    "       jsonb_set(maintenance_status,'{AUTO_INDEXING,maintenanceRunning,999}',?::jsonb)" +
                    " END"
                    + "		WHERE dh_schema=? AND connector_id=?";

    public static String updateConnectorStatusBeginMaintenanceForce =
            "UPDATE xyz_config.db_status SET maintenance_status=" +
                    " jsonb_set(maintenance_status,'{AUTO_INDEXING,maintenanceRunning}', ?::jsonb ||  '[]'::jsonb) " +
                    "		WHERE dh_schema=? AND connector_id=?";

    public static String updateConnectorStatusMaintenanceComplete =
            "UPDATE xyz_config.db_status SET maintenance_status =  "+
                "jsonb_set( jsonb_set(maintenance_status,'{AUTO_INDEXING,maintainedAt}'::text[], ?::jsonb),'{AUTO_INDEXING,maintenanceRunning}'," +
                "   COALESCE((select jsonb_agg(jsonb_array_elements) from jsonb_array_elements(maintenance_status->'AUTO_INDEXING'->'maintenanceRunning')" +
                "           where jsonb_array_elements != ?::jsonb), '[]'::jsonb)" +
                "    )" +
                "    WHERE dh_schema=? AND connector_id=?";

    /** Get status of running index queries (statistic,analyzing,creation,deletion) */
    public static String checkIDXStatus = "SELECT * FROM xyz_index_status();";

    /** Create XYZ_CONFIG_SCHEMA and required system tables */
    public static String configSchemaAndSystemTablesSQL =
            "CREATE SCHEMA IF NOT EXISTS \"" + XYZ_CONFIG_SCHEMA + "\";"+
                    "CREATE TABLE IF NOT EXISTS  "+ XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_STORAGE_TABLE+" (id VARCHAR(50) primary key, owner VARCHAR (50), config JSONB);"+
                    "CREATE TABLE IF NOT EXISTS  " + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_SPACE_TABLE+" (id VARCHAR(50) primary key, owner VARCHAR (50), cid VARCHAR (50), config JSONB);";

    /** Create XYZ_CONFIG_IDX_TABLE in XYZ_CONFIG_SCHEMA. */
    public static String createIDXTableSQL =
        "CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_IDX_TABLE+
            "( " +
            "  runts timestamp with time zone, " +
            "  spaceid text NOT NULL, " +
            "  schem text, " +
            "  idx_available jsonb," +
            "  idx_proposals jsonb, " +
            "  idx_creation_finished boolean, " +
            "  count bigint, " +
            "  prop_stat jsonb, " +
            "  idx_manual jsonb, " +
            "  auto_indexing boolean, "+
            "  CONSTRAINT "+XYZ_CONFIG_IDX_TABLE+"_pkey PRIMARY KEY (spaceid) " +
            "); " +
            "INSERT INTO xyz_config."+XYZ_CONFIG_IDX_TABLE+" (spaceid,count) " +
            "   VALUES ('idx_in_progress','0') " +
            "ON CONFLICT DO NOTHING; ";

    /** Create XYZ_CONFIG_IDX_TABLE in XYZ_CONFIG_SCHEMA. */
    public static String createIDXTable =
            "CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_IDX_TABLE+
                    "( " +
                    "  runts timestamp with time zone, " +
                    "  spaceid text NOT NULL, " +
                    "  schem text, " +
                    "  idx_available jsonb," +
                    "  idx_proposals jsonb, " +
                    "  idx_creation_finished boolean, " +
                    "  count bigint, " +
                    "  prop_stat jsonb, " +
                    "  idx_manual jsonb, " +
                    "  CONSTRAINT "+XYZ_CONFIG_IDX_TABLE+"_pkey PRIMARY KEY (spaceid) " +
                    "); ";

    public static String createIDXInitEntry =  "INSERT INTO xyz_config."+XYZ_CONFIG_IDX_TABLE+" (spaceid,count) " +
            "   VALUES ('idx_in_progress','0') " +
            "ON CONFLICT DO NOTHING; ";


    public static String createDbStatusTable =
            "CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_DB_STATUS+
                    "( " +
                    "  dh_schema text NOT NULL," +
                    "  connector_id text NOT NULL," +
                    "  initialized boolean," +
                    "  extensions text[]," +
                    "  script_versions jsonb," +
                    "  maintenance_status jsonb," +
                    "  CONSTRAINT xyz_db_status_pkey PRIMARY KEY (dh_schema,connector_id)"+
                    "); ";

    public static String createSpaceMetaTable =
            "CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_SPACE_META_TABLE+
                    "( " +
                    "  id text NOT NULL," +
                    "  schem text NOT NULL," +
                    "  h_id text," +
                    "  meta jsonb," +
                    "  CONSTRAINT xyz_space_meta_pkey PRIMARY KEY (id,schem)"+
                    "); ";

    public static String getIDXStatus =
            "select (" +
                    "select row_to_json( status ) " +
                    "   from (" +
                    "          select 'SpaceStatus' as type, " +
                    "           (extract(epoch from runts AT TIME ZONE 'UTC')*1000)::BIGINT as runts, " +
                    "           spaceid, " +
                    "           idx_creation_finished as \"idxCreationFinished\"," +
                    "           count, " +
                    "           auto_indexing as \"autoIndexing\", " +
                    "           idx_available as \"idxAvailable\", " +
                    "           idx_manual as \"idxManual\", " +
                    "            idx_proposals as \"idxProposals\"," +
                    "            prop_stat as \"propStats\"" +
                    "   ) status " +
                    ") from "+(MaintenanceSQL.XYZ_CONFIG_SCHEMA + "."+MaintenanceSQL.XYZ_CONFIG_IDX_TABLE)
                    +" where schem=? and spaceid=?;";

    public static String maintainIDXOfSpace =
            "select xyz_maintain_idxs_for_space(?,?)";

    public static String getConnectorStatus =
            "select (select row_to_json( prop ) from ( select 'ConnectorStatus' as type, connector_id, initialized, " +
                "extensions, script_versions as \"scriptVersions\", maintenance_status as \"maintenanceStatus\") prop ) " +
                    "from xyz_config.db_status where dh_schema=? AND connector_id=?";

    public static String createIDX=
            "SELECT xyz_create_idxs(?, ?, ?, ?, ?)";

    public static String updateIDXEntry =
            "UPDATE "+MaintenanceSQL.XYZ_CONFIG_SCHEMA + "."+MaintenanceSQL.XYZ_CONFIG_IDX_TABLE
            +"  SET idx_creation_finished = null "
            +"		WHERE schem=? AND spaceid=?";
}
