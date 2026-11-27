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

/**
 * Install required extensions
 */
DO $initializeDatabase$
    DECLARE
        config_schema_name text := 'xyz_config';
        original_search_path text := current_setting('search_path');
    BEGIN
        IF EXISTS(SELECT extname FROM pg_extension WHERE extname = 'postgis') THEN
            --Skip init of db
            RETURN;
        END IF;

        -- Install required extensions
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

        -- Deprecated - can be removed once xyz_config.xyz_idxs_status is not used anymore
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', config_schema_name);

        --Ensure to restore the original search path because some of the installations above might have changed it
        EXECUTE format('SET search_path = %s', original_search_path);
END;
$initializeDatabase$;

/**
 * Installs the extensions h3 and h3_postgis (and postgis_raster, which h3_postgis requires).
 * Unlike the block above, which returns early on every database that has postgis already, this block also runs on existing
 * databases. It can be folded into the block above once the extensions are installed on all databases.
 * h3 and h3_postgis go into the dedicated schema h3_ext, never into "public" and never into a schema named "h3":
 * Older versions resolve the unqualified type h3index of their own H3 functions through a search path with "public" first, so a
 * base type h3index in "public" would replace their domain h3index.
 * If the extensions are not available, the privileges are missing or the installation fails otherwise, the block only raises a
 * warning, so that the other scripts still get installed. H3 based features (hexbin clustering, h3Index) do not work on such a
 * database then. Only transient errors (e.g., a lock timeout) fail the script, so that the installer retries it.
 * This script gets installed into several schemas concurrently (e.g., the hub and the jobs schemas), each under a lock of its own
 * schema, so the block takes an additional lock of its own.
 */
DO $installH3Extensions$
    DECLARE
        original_search_path text := current_setting('search_path');
    BEGIN
        IF (SELECT count(*) FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
            WHERE e.extname IN ('h3', 'h3_postgis') AND n.nspname = 'h3_ext') = 2 THEN
            RETURN;
        END IF;

        IF (SELECT count(*) FROM pg_available_extensions WHERE name IN ('h3', 'h3_postgis', 'postgis_raster')) < 3 THEN
            RAISE WARNING 'The extensions h3, h3_postgis and postgis_raster are not all available. H3 features will not work.';
            RETURN;
        END IF;

        IF EXISTS(SELECT 1 FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
            WHERE e.extname IN ('h3', 'h3_postgis') AND n.nspname != 'h3_ext') THEN
            RAISE WARNING 'The extension h3 or h3_postgis is installed in another schema than h3_ext. H3 features will not work.';
            RETURN;
        END IF;

        PERFORM pg_advisory_xact_lock(('x' || left(md5('xyz_h3_extensions'), 15))::bit(60)::bigint);

        BEGIN
            --No VERSION clause: the default version of the database is used. No CASCADE: it would put postgis_raster into h3_ext.
            CREATE EXTENSION IF NOT EXISTS postgis_raster SCHEMA public;
            CREATE SCHEMA IF NOT EXISTS h3_ext;
            CREATE EXTENSION IF NOT EXISTS h3 SCHEMA h3_ext;
            CREATE EXTENSION IF NOT EXISTS h3_postgis SCHEMA h3_ext;
            GRANT USAGE ON SCHEMA h3_ext TO PUBLIC;
        EXCEPTION
            WHEN unique_violation OR duplicate_object THEN
                --The lock above rules out concurrent installs by the Hub and the job steps, so this is mostly a conflicting object
                RAISE WARNING 'The H3 extensions could not be installed, an object exists already. H3 features might not work: %', SQLERRM;
            WHEN insufficient_privilege THEN
                RAISE WARNING 'Missing privileges to install the H3 extensions. H3 features will not work: %', SQLERRM;
            --Transient errors fail the script, so that the installer retries it instead of skipping the extensions
            WHEN query_canceled OR lock_not_available OR deadlock_detected OR serialization_failure THEN
                RAISE;
            --Any other error must not fail the installation of this script
            WHEN OTHERS THEN
                RAISE WARNING 'The H3 extensions could not be installed. H3 features will not work: % (SQLSTATE %)', SQLERRM, SQLSTATE;
        END;

        --Ensure to restore the original search path because some of the installations above might have changed it
        EXECUTE format('SET search_path = %s', original_search_path);
END;
$installH3Extensions$;

/**
 * Drops the schemas of the PL/pgSQL H3 port that versions before the h3 extension used: hub.h3, hub.h3:<version>, jobs.h3 and
 * jobs.h3:<version>. The installer never uninstalls the schemas of a script it no longer loads, so this is done explicitly.
 * No other schema is touched, e.g. hub.hexbin_h3*, h3_ext, or copies of the port in schemas of other names.
 * A schema with an object outside of these schemas depending on it is kept with a warning, because DROP SCHEMA ... CASCADE would
 * drop that object, too. Errors only raise a warning, so that the other scripts still get installed; transient errors fail the
 * script, so that the installer retries it. This script gets installed into several schemas concurrently (the hub and the jobs
 * schemas), each under a lock of its own schema, so the block takes an additional lock of its own.
 * This block can be removed once it ran on all databases.
 */
DO $dropH3PortSchemas$
    DECLARE
        port_schema_pattern CONSTANT text := '^(hub|jobs)[.]h3(:.+)?$';
        port_schema_oid oid;
        port_schema text;
        outside_dependents text;
    BEGIN
        IF NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname ~ port_schema_pattern) THEN
            RETURN;
        END IF;

        PERFORM pg_advisory_xact_lock(('x' || left(md5('xyz_h3_extensions'), 15))::bit(60)::bigint);

        --Read again under the lock: a concurrent installation might have dropped them already
        FOR port_schema_oid, port_schema IN SELECT oid, nspname FROM pg_namespace WHERE nspname ~ port_schema_pattern
            ORDER BY nspname LOOP
            BEGIN
                WITH members(classid, objid) AS (
                    SELECT d.classid, d.objid FROM pg_depend d
                    WHERE d.refclassid = 'pg_namespace'::regclass::oid AND d.refobjid = port_schema_oid
                    UNION
                    --Array and row types have no dependency on the namespace of their own
                    SELECT 'pg_type'::regclass::oid, t.oid FROM pg_type t WHERE t.typnamespace = port_schema_oid
                )
                SELECT string_agg(DISTINCT o.identity, ', ') INTO outside_dependents
                FROM members m
                JOIN pg_depend d ON d.refclassid = m.classid AND d.refobjid = m.objid
                CROSS JOIN LATERAL pg_identify_object(d.classid, d.objid, d.objsubid) o
                --The schema of pg_identify_object() is quoted if needed, e.g. "hub.h3:4.0.1"
                WHERE d.deptype IN ('n', 'a') AND (o.schema IS NULL OR (parse_ident(o.schema))[1] !~ port_schema_pattern);

                IF outside_dependents IS NULL THEN
                    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', port_schema);
                ELSE
                    RAISE WARNING 'The schema % of the old H3 port is kept, objects outside of it depend on it: %', port_schema,
                        left(outside_dependents, 1000);
                END IF;
            EXCEPTION
                --Transient errors fail the script, so that the installer retries it
                WHEN query_canceled OR lock_not_available OR deadlock_detected OR serialization_failure THEN
                    RAISE;
                --Any other error must not fail the installation of this script
                WHEN OTHERS THEN
                    RAISE WARNING 'The schema % of the old H3 port could not be dropped: % (SQLSTATE %)', port_schema, SQLERRM, SQLSTATE;
            END;
        END LOOP;
END;
$dropH3PortSchemas$;

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
 * This function is like coalesce() except that it returns the *index* of the first
 * non-null element within the specified array.
 */
CREATE OR REPLACE FUNCTION coalesce_subscript(anyarray) RETURNS integer AS $BODY$
    SELECT i
    FROM generate_subscripts($1, 1) AS i
    WHERE $1[i] IS NOT NULL
    ORDER BY i
    LIMIT 1;
$BODY$ LANGUAGE SQL IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $BODY$
    SELECT array_agg(val ORDER BY ordinality DESC)
    FROM unnest($1) WITH ORDINALITY AS t(val, ordinality);
$BODY$;

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

	--min_version is always 0 here, the Hub computes the real lower bound (see FeatureTaskHandler)
	IF space_ext_table IS NULL THEN
		EXECUTE format(
		    'SELECT 0::BIGINT, MAX(version), pg_total_relation_size(%1$L) FROM %1$s',
		    space_table
		) INTO min_version, max_version, table_size;

		RETURN QUERY SELECT table_size, A.table_count, A.is_estimated, min_version, max_version
		FROM fetch_table_count(space_table) A;
	ELSE
		EXECUTE format('SELECT 0::BIGINT, MAX(version), pg_total_relation_size(%1$L) FROM %1$s',
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
LANGUAGE plpgsql STABLE
PARALLEL SAFE;

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
LANGUAGE plpgsql STABLE
PARALLEL SAFE;

/**
 *  register code of requirerd js libs in gobalThis
 *  select require( 'libmod1', 'libmod2', 'libmod3' )
 *  select require(variadic array['libmod1','libmod2','libmod3'] )
 */
CREATE OR REPLACE FUNCTION require(VARIADIC modules text[])
RETURNS void AS $body$
  for (let i = 0; i < modules.length; i++) {
    const name = modules[i];

    if (!name || typeof name !== "string")
      continue;

    if (!globalThis[name]) {
      plv8.elog(NOTICE, `Loading module: ${name}`);
      try {
        const libSqlName = 'libjs_' + name;
        const res = plv8.execute(`SELECT ${libSqlName}()`);
        globalThis[name] = res.length > 0 ? new Function("return " + res[0][libSqlName])()
            : `Unable to load code for module: ${name}`;
        plv8.elog(NOTICE, `Loaded module source: ${res[0][libSqlName].substring(0, 25)}`);
      }
      catch (err) {
        plv8.elog(NOTICE, `Loading module ${name} failed: ` + err.message);
      }
    }
  }
$body$
LANGUAGE plv8 IMMUTABLE
PARALLEL UNSAFE;
