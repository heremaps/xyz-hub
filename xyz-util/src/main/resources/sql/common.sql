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

/**
 * Install required extensions
 */
DO $installExtensions$
    BEGIN
    
     CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;
     CREATE EXTENSION IF NOT EXISTS postgis_topology;
     CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;
     CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;
     CREATE EXTENSION IF NOT EXISTS plv8 SCHEMA pg_catalog;

     IF EXISTS ( SELECT 1 FROM pg_settings WHERE name IN ( 'rds.extensions', 'rds.allowed_extensions', 'rds.superuser_variables' ) ) THEN
      CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;
      CREATE EXTENSION IF NOT EXISTS aws_lambda CASCADE;
     ELSE
      CREATE EXTENSION IF NOT EXISTS plpython3u CASCADE;
      CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;
     END IF;
     
    END;
$installExtensions$;

/**
 * Returns a context field's value.
 */
CREATE OR REPLACE FUNCTION context(key TEXT) RETURNS JSONB AS $BODY$
BEGIN
    RETURN (current_setting('xyz.queryContext')::JSONB)[key];
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * Sets the context (a static map) for the current query / transaction.
 */
CREATE OR REPLACE FUNCTION context(context JSONB) RETURNS VOID AS $BODY$
BEGIN
    PERFORM set_config('xyz.queryContext', context::TEXT, false);
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * Returns the whole context object.
 */
CREATE OR REPLACE FUNCTION context() RETURNS JSONB AS $BODY$
BEGIN
    RETURN current_setting('xyz.queryContext')::JSONB;
    EXCEPTION WHEN OTHERS THEN RETURN '{}'::JSONB;
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * Sets a context field to the specified value for the current query / transaction.
 */
CREATE OR REPLACE FUNCTION context(key TEXT, value ANYELEMENT) RETURNS VOID AS $BODY$
BEGIN
    --Inject / overwrite the field
    PERFORM context(jsonb_set(context(), ARRAY[key], to_jsonb(value), true));
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * This function can be used to write logs, if we run in Async mode.
 */
CREATE OR REPLACE FUNCTION write_log(log_msg text, log_source text, log_level text DEFAULT 'INFO') RETURNS VOID AS
$BODY$
DECLARE
BEGIN
--     CREATE TABLE IF NOT EXISTS common."logs"
--     (
--         pid integer,
--         ts TIMESTAMP,
--         log text ,
--         level text ,
--         source text
--     );

    INSERT INTO common.logs(pid, ts, log , level , source)
				VALUES (pg_backend_pid(), NOW(), log_msg, log_source, log_level);
END
$BODY$
LANGUAGE plpgsql VOLATILE;

/**
 * This function can be used to get the count of a table.
 * It will use the pg_class.reltuples value if the table has more than estimate_from_threshold rows.
 * Otherwise it will run a count query.
 */
CREATE OR REPLACE FUNCTION calculate_space_statistics(
    IN space_table regclass,
    IN space_ext_table regclass,
    IN context text
)
RETURNS TABLE(table_size bigint, table_count bigint, is_estimated boolean, min_version bigint, max_version bigint) AS
$BODY$
DECLARE 	
    count_table bigint :=0;
	count_ext_table bigint :=0;
BEGIN
	IF context NOT IN ('DEFAULT','SUPER','EXTENSION') THEN 
		RAISE EXCEPTION 'Unknown context: %!', context;
	END IF;
	
	IF space_ext_table IS NULL THEN		
		EXECUTE format(
		    'SELECT (SELECT COALESCE((meta->>''minAvailableVersion'')::BIGINT,0) FROM xyz_config.space_meta WHERE h_id=%1$L), '
	            || 'MAX(version), pg_total_relation_size(%2$L) FROM %2$s',
		    regexp_replace(replace(space_table::text, '"', ''), '_head$', '') , space_table
		) INTO min_version, max_version, table_size;
		
		RETURN QUERY SELECT table_size, A.table_count, A.is_estimated, min_version, max_version
		FROM fetch_table_count(space_table) A;
	ELSE 
		EXECUTE format('SELECT (SELECT COALESCE((meta->>''minAvailableVersion'')::BIGINT,0) FROM xyz_config.space_meta WHERE h_id=%1$L), '
				|| 'MAX(version), pg_total_relation_size(%2$L) FROM %2$s', 
			(CASE context
				WHEN 'SUPER' THEN regexp_replace(replace(space_ext_table::text, '"', ''), '_head$', '')
				ELSE regexp_replace(replace(space_table::text, '"', ''), '_head$', '')
			END),
			(CASE context
				WHEN 'SUPER' THEN space_ext_table
				ELSE space_table
			END)
		) INTO min_version, max_version, table_size;	

		CASE context
		    WHEN 'SUPER' THEN RETURN QUERY SELECT table_size, A.table_count, A.is_estimated, min_version, max_version
				FROM fetch_table_count(space_ext_table) A;
		    WHEN 'EXTENSION' THEN RETURN QUERY SELECT table_size, A.table_count, A.is_estimated, min_version, max_version 
				FROM fetch_table_count(space_table) A;
		    WHEN 'DEFAULT' THEN 
				table_size = table_size + pg_total_relation_size(space_ext_table);
				RETURN QUERY 				
					SELECT table_size, SUM(C.table_count)::BIGINT, BOOL_OR(C.is_estimated), min_version, max_version
					FROM(
						SELECT A.table_count, A.is_estimated FROM fetch_table_count(space_table) A
						UNION ALL
						SELECT B.table_count,B.is_estimated FROM fetch_table_count(space_ext_table) B
					) C;
		END CASE;
	END IF;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;

/**
 * This function can be used to get the count of a table.
 * It will use the pg_class.reltuples value if the table has more than estimate_from_threshold rows.
 * Otherwise it will run a count query.
 */
CREATE OR REPLACE FUNCTION fetch_table_count(
    IN space_table regclass,
	IN estimate_from_threshold INTEGER DEFAULT 300000
)
RETURNS TABLE(table_count bigint, is_estimated boolean) AS
$BODY$
BEGIN
	SELECT reltuples INTO table_count FROM pg_class WHERE oid = space_table;

	--TODO: table_count = -1 => limit query runtime to max 2sec

	IF table_count <= estimate_from_threshold THEN
		is_estimated = false;
		EXECUTE format('SELECT COUNT(1) FROM %s WHERE operation NOT IN(''H'',''J'',''D'') ', space_table) INTO table_count;
	ELSE
		is_estimated = true;
	END IF;
	
    RETURN NEXT;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
