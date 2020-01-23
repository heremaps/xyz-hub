/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
    private static String XYZ_CONFIG_IDX_TABLE = "xyz_idxs_status";
    private static String XYZ_CONFIG_STORAGE_TABLE = "xyz_storage";
    private static String XYZ_CONFIG_SPACE_TABLE = "xyz_space";

    /**
     * Check if all required database extensions are installed
     */
    public static String generateCheckExtensionsSQL(boolean hasPropertySearch){
        return "SELECT COALESCE(array_agg(extname) @> '{postgis,postgis_topology,tsm_system_rows"
                + (hasPropertySearch ? ",dblink" : "") + "}', false) as all_ext_av,"
                + "COALESCE(array_agg(extname)) as ext_av, "
                + "(select current_setting('is_superuser')) as is_su "
                + "FROM ("
                + "	SELECT extname FROM pg_extension"
                + "		WHERE extname in('postgis','postgis_topology','tsm_system_rows','dblink')"
                + "	order by extname"
                + ") A";
    }

    /**
     * Check if all required schemas and tables are created
     */
    public static String generateCheckSchemasAndIdxTableSQL(String schema){
        return "SELECT array_agg(nspname) @> ARRAY['" + schema + "'] as main_schema, "
                + " array_agg(nspname) @> ARRAY['xyz_config'] as config_schema, "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_IDX_TABLE+"') IS NOT NULL) as idx_table) "
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
    public static String generateMandatoryExtensionSQL(boolean isPropertySearchSupported){
        return "CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;"+
                "CREATE EXTENSION IF NOT EXISTS postgis_topology;"+
                "CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;"+
                (isPropertySearchSupported == true ? "CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;" : "");
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
    public static String generateIDXSQL(String schema, String user, String password, String database, int port, int mode){
        return  "SELECT * from xyz_create_idxs_over_dblink('" + schema + "',50, 0,"+mode+",ARRAY['wikvaya','" + user + "'],'" + user
                + "','" + password + "','" + database + "'," + port + ",'" + schema + ",h3,public,topology)')";
    }

    /** Get status of running index queries (statistic,analyzing,creation,deletion) */
    public static String checkIDXStatus = "SELECT * FROM xyz_index_status();";

    /** Create XYZ_CONFIG_SCHEMA and required system tables */
    public static String configSchemaAndSystemTablesSQL =
            "CREATE SCHEMA IF NOT EXISTS \"" + XYZ_CONFIG_SCHEMA + "\";"+
                    "CREATE TABLE IF NOT EXISTS  "+ XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_STORAGE_TABLE+" (id VARCHAR(50) primary key, config JSONB);"+
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
            "  CONSTRAINT "+XYZ_CONFIG_IDX_TABLE+" PRIMARY KEY (spaceid) " +
            "); " +
            "INSERT INTO xyz_config."+XYZ_CONFIG_IDX_TABLE+" (spaceid,count) " +
            "   VALUES ('idx_in_progress','0') " +
            "ON CONFLICT DO NOTHING; ";
}
