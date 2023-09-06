/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import static com.here.xyz.psql.query.ModifySpace.IDX_STATUS_TABLE;
import static com.here.xyz.psql.query.ModifySpace.IDX_STATUS_TABLE_FQN;
import static com.here.xyz.psql.query.ModifySpace.XYZ_CONFIG_SCHEMA;

public class MaintenanceSQL {

    /**
     * Tables for database wide configurations, which belong to XYZ_CONFIG_SCHEMA
     */
    private static String XYZ_CONFIG_DB_STATUS_TABLE = "db_status";
    private static String XYZ_CONFIG_SPACE_META_TABLE = "space_meta";
    private static String XYZ_CONFIG_STORAGE_TABLE = "xyz_storage";
    private static String XYZ_CONFIG_SPACE_TABLE = "xyz_space";
    private static String XYZ_CONFIG_SUBSCRIPTION_TABLE = "xyz_subscription";
    private static String XYZ_CONFIG_TAG_TABLE = "xyz_tags";

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
                + "(SELECT (to_regclass('" + IDX_STATUS_TABLE_FQN + "') IS NOT NULL) as idx_table), "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+ XYZ_CONFIG_DB_STATUS_TABLE +"') IS NOT NULL) as db_status_table), "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_SPACE_META_TABLE+"') IS NOT NULL) as space_meta_table), "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_SUBSCRIPTION_TABLE+"') IS NOT NULL) as subscription_table), "
                + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + "."+ XYZ_CONFIG_TAG_TABLE +"') IS NOT NULL) as tag_table) "
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
                "BEGIN" +
                "   CREATE EXTENSION IF NOT EXISTS plpython3u CASCADE; " +
                "   EXCEPTION WHEN OTHERS THEN " +
                "       RAISE NOTICE 'Not able to install plpython3u extension'; " +
                "   END;" +
                "CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE; " +
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

    /** Get status of running index queries (statistic,analyzing,creation,deletion) */
    public static String checkIDXStatus = "SELECT * FROM xyz_index_status();";

    /** Create XYZ_CONFIG_SCHEMA and required system tables */
    public static String configSchemaAndSystemTablesSQL =
            "CREATE SCHEMA IF NOT EXISTS \"" + XYZ_CONFIG_SCHEMA + "\";"+
                    "CREATE TABLE IF NOT EXISTS  "+ XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_STORAGE_TABLE+" (id VARCHAR(50) primary key, owner VARCHAR (50), config JSONB);"+
                    "CREATE TABLE IF NOT EXISTS  " + XYZ_CONFIG_SCHEMA + "."+XYZ_CONFIG_SPACE_TABLE+" (id VARCHAR(255) primary key, owner VARCHAR (50), cid VARCHAR (255), config JSONB, region VARCHAR (50));";

    public static String createIDXInitEntry =  "INSERT INTO " + IDX_STATUS_TABLE_FQN + " (spaceid,count) " +
        "   VALUES ('idx_in_progress','0') " +
        "ON CONFLICT DO NOTHING; ";

    /** Create XYZ_CONFIG_IDX_TABLE in XYZ_CONFIG_SCHEMA. */
    public static String createIDXTableSQL =
        "CREATE TABLE IF NOT EXISTS " + IDX_STATUS_TABLE_FQN +
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
            "  CONSTRAINT " + IDX_STATUS_TABLE + "_pkey PRIMARY KEY (spaceid) " +
            "); " +
            createIDXInitEntry;

    /** Create XYZ_CONFIG_IDX_TABLE in XYZ_CONFIG_SCHEMA. */
    public static String createIDXTable =
            "CREATE TABLE IF NOT EXISTS " + IDX_STATUS_TABLE_FQN +
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
                    "  CONSTRAINT " + IDX_STATUS_TABLE + "_pkey PRIMARY KEY (spaceid) " +
                    "); ";


    public static String createDbStatusTable =
            "CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + "."+ XYZ_CONFIG_DB_STATUS_TABLE +
                    "( " +
                    "  dh_schema text NOT NULL," +
                    "  connector_id text NOT NULL," +
                    "  initialized boolean," +
                    "  extensions text[]," +
                    "  script_versions jsonb," +
                    "  maintenance_status jsonb," +
                    "  CONSTRAINT xyz_db_status_pkey PRIMARY KEY (dh_schema,connector_id)"+
                    "); ";

    public static String createTagTable =
            "CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + "."+ XYZ_CONFIG_TAG_TABLE +
                    "( " +
                    "  id VARCHAR(255)," +
                    "  space VARCHAR (255)," +
                    "  version BIGINT," +
                    "  PRIMARY KEY(id, space)"+
                    "); ";

    public static String createSubscriptionTable =
            "CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + "."+ XYZ_CONFIG_SUBSCRIPTION_TABLE +
                    "( " +
                    "  id VARCHAR(255) primary key," +
                    "  source VARCHAR (255)," +
                    "  config JSONB" +
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
}
