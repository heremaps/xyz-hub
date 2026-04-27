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

-- ####################################################################################################################
-- Common helper functions ---

/**
 * s3_plugin_config(format TEXT)
 *
 * Returns plugin column names and options for S3 import/export based on the given format.
 *
 * Supported formats:
 *   - csv_json_wkb: Returns columns 'jsondata,geo' with default CSV options.
 *   - csv_geojson: Returns column 'jsondata' with default CSV options.
 *   - geojson: Returns column 'jsondata' with custom CSV options (special delimiter and quote).
 *
 * Raises an exception if the format is not supported.
 *
 * @param format TEXT: The format type (e.g., 'csv_json_wkb', 'csv_geojson', 'geojson').
 * @return TABLE(plugin_columns TEXT, plugin_options TEXT)
 */
CREATE OR REPLACE FUNCTION s3_plugin_config(format TEXT)
    RETURNS TABLE(plugin_columns TEXT, plugin_options TEXT)
    LANGUAGE 'plpgsql'
    IMMUTABLE
    PARALLEL SAFE
AS $BODY$
BEGIN
    plugin_options := '(FORMAT CSV, ENCODING ''UTF8'', DELIMITER '','', QUOTE  ''"'',  ESCAPE '''''''')';
    format := lower(format);

    IF format = 'csv_json_wkb' OR format = 'csv_jsonwkb' THEN
        plugin_columns := 'jsondata,geo';
    ELSEIF format = 'csv_geojson' THEN
        plugin_columns := 'jsondata';
    ELSEIF format = 'geojson' THEN
        plugin_columns := 'jsondata';
        plugin_options := format('(FORMAT CSV, ENCODING ''UTF8'', DELIMITER %1$L , QUOTE  %2$L)', CHR(2), CHR(1));
    ELSEIF format = 'fast_import_into_empty' THEN
        plugin_columns := 'id,author,version,operation,jsondata,geo,searchable';
        plugin_options := 'DELIMITER '','' CSV ENCODING  ''UTF8'' QUOTE ''"'' ESCAPE '''''''' ';
    ELSE
        RAISE EXCEPTION 'Format ''%'' not supported! ',format
            USING HINT = 'geojson | csv_geojson | csv_json_wkb are available',
                ERRCODE = 'XYZ40';
    END IF;

    RETURN NEXT;
END;
$BODY$;

/**
 * get_table_reference(schema TEXT, tbl TEXT, type TEXT = 'DEFAULT')
 *
 * Returns the REGCLASS reference for a table, optionally with a prefix and/or suffix based on the type.
 *
 * - If type is 'JOB_TABLE' or 'TRIGGER_TABLE', adds 'job_data_' as prefix.
 * - If type is 'TRIGGER_TABLE', adds '_trigger_tbl' as suffix.
 * - Otherwise, uses the table name as-is.
 *
 * @param schema TEXT: The schema name.
 * @param tbl TEXT: The table name.
 * @param type TEXT: The table type ('DEFAULT', 'JOB_TABLE', 'TRIGGER_TABLE').
 * @return REGCLASS: The table reference.
 */
CREATE OR REPLACE FUNCTION get_table_reference(schema TEXT, tbl TEXT, type TEXT = 'DEFAULT')
    RETURNS REGCLASS
    LANGUAGE 'plpgsql'
    STABLE
    PARALLEL SAFE
AS $BODY$
DECLARE
    prefix TEXT := '';
    suffix TEXT := '';
BEGIN
    IF type = 'JOB_TABLE' OR type = 'TRIGGER_TABLE' THEN
       prefix = 'job_data_';
    END IF;

    IF type = 'TRIGGER_TABLE' THEN
       suffix = '_trigger_tbl';
    END IF;

    RETURN (schema ||'.'|| '"' || prefix || tbl || suffix ||'"')::REGCLASS;
END;
$BODY$;

/**
 * get_stepid_from_work_table(trigger_tbl REGCLASS)
 *
 * Extracts the step ID from a trigger table name.
 *
 * Assumes the table name format is 'job_data_<stepId>_trigger_tbl'.
 * Removes the 'job_data_' prefix and '_trigger_tbl' suffix to return the step ID.
 *
 * @param trigger_tbl REGCLASS: The trigger table reference.
 * @return TEXT: The extracted step ID.
 */
CREATE OR REPLACE FUNCTION get_stepid_from_work_table(trigger_tbl REGCLASS)
RETURNS TEXT
    LANGUAGE 'plpgsql'
    STABLE
    PARALLEL SAFE
AS $BODY$
BEGIN
    RETURN regexp_replace(substring(trigger_tbl::TEXT from length('job_data_') + 1), '_trigger_tbl', '');
END;
$BODY$;

/**
 * get_work_table_name(schema TEXT, step_id TEXT)
 *
 * Returns the REGCLASS reference for a work table based on the schema and step ID.
 * The table name format is 'job_data_<step_id>'.
 *
 * @param schema TEXT: The schema name.
 * @param step_id TEXT: The step identifier.
 * @return REGCLASS: The work table reference.
 */
CREATE OR REPLACE FUNCTION get_work_table_name(schema TEXT, step_id TEXT)
    RETURNS REGCLASS
    LANGUAGE 'plpgsql'
    STABLE
    PARALLEL SAFE
AS $BODY$
BEGIN
    RETURN (schema ||'.'|| '"job_data_' || step_id || '"')::REGCLASS;
END;
$BODY$;

-- ####################################################################################################################
-- Retry helpers (shared between execute_import_from_s3 / execute_export_to_s3) ---

/**
 * is_retryable_s3_sqlstate(state TEXT)
 *
 * Returns TRUE if the given SQLSTATE is considered a transient / retryable error
 * for aws_s3 import & export operations.
 *
 * Covered classes:
 *   - 40001 serialization_failure
 *   - 40P01 deadlock_detected
 *   - 55P03 lock_not_available
 *   - 23505 unique_violation           (import only in practice)
 *   - 23P01 exclusion_violation        (import only in practice)
 *   - 53300 too_many_connections
 *   - 08xxx connection_exception family
 *   - 57P01 admin_shutdown
 *   - 22P02 invalid_text_representation (import only in practice)
 *   - 22P04 bad_copy_file_format        (import only in practice)
 */
CREATE OR REPLACE FUNCTION is_retryable_s3_sqlstate(state TEXT)
    RETURNS BOOLEAN
    LANGUAGE 'sql'
    IMMUTABLE
    PARALLEL SAFE
AS $BODY$
    SELECT state IN (
        '40001', '40P01', '55P03',
        '23505', '23P01',
        '53300',
        '08000', '08001', '08003', '08006', '08004',
       -- '57P01', TBD: clarify
        '22P02', '22P04'
    );
$BODY$;

/**
 * s3_retry_backoff_ms(attempts INT, base_ms INT, cap_ms INT)
 *
 * Computes exponential backoff delay in milliseconds:
 *   delay = min(base_ms * 2^attempts, cap_ms)
 */
CREATE OR REPLACE FUNCTION s3_retry_backoff_ms(
        attempts INT,
        base_delay_ms INT DEFAULT 10000,
        max_ms INT DEFAULT 60000
    )
    RETURNS INT
    LANGUAGE 'sql'
    IMMUTABLE
    PARALLEL SAFE
AS $BODY$
    SELECT LEAST(base_delay_ms * (2 ^ attempts), max_ms)::INT;
$BODY$;

-- ####################################################################################################################
-- Import related helper functions ---

/**
 * Function: tasked_import_from_s3_trigger_for_empty_layer
 * (for tasked import into empty layers)
 *
 * Purpose:
 *   Trigger function to enrich a feature during import from S3 into an empty layer.
 *   It updates the NEW row with metadata and processed geometry.
 *
 * Arguments:
 *   - TG_ARGV[0]: author (TEXT) - The author of the import.
 *   - TG_ARGV[1]: curVersion (BIGINT) - The version to assign to the feature.
 *   - TG_ARGV[2]: retain_meta (BOOLEAN) - Whether to retain existing metadata.
 *
 * Behavior:
 *   - Calls import_from_s3_enrich_feature to process the incoming feature.
 *   - Sets NEW.version and NEW.author.
 *   - Updates NEW.id, NEW.operation, NEW.geo, and NEW.jsondata with enriched values.
 *   - Returns the modified NEW row for further processing.
 *
 * Returns:
 *   - The enriched NEW row (trigger return).
 */
CREATE OR REPLACE FUNCTION tasked_import_from_s3_trigger_for_empty_layer()
    RETURNS trigger
AS $BODY$
DECLARE
    author TEXT := TG_ARGV[0];
    curVersion BIGINT := TG_ARGV[1];
    retain_meta BOOLEAN := TG_ARGV[2]::BOOLEAN;
    feature RECORD;
BEGIN
    -- Skip features marked as deleted
    IF (NEW.jsondata::JSONB#>>'{properties,@ns:com:here:xyz,deleted}')::BOOLEAN IS TRUE THEN
        RETURN NULL;
    END IF;

    SELECT new_jsondata, new_geo, new_operation, new_id
        from import_from_s3_enrich_feature(NEW.jsondata, NEW.geo, retain_meta)
    INTO feature;

    NEW.version = curVersion;
    NEW.author = author;

    NEW.id = feature.new_id;
    NEW.operation = feature.new_operation;
    NEW.geo = xyz_reduce_precision(feature.new_geo, false);
    NEW.jsondata = feature.new_jsondata;

    RETURN NEW;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE;
    
/**
 * Function: import_from_s3_trigger_for_non_empty_layer
 * (for tasked import into non-empty layers)
 *
 * Purpose:
 *   Trigger function for importing features into non-empty layers (entity features).
 *   Handles feature enrichment, conflict resolution, and batch context setup.
 *   Uses write_features to process and import features.
 *
 * Arguments:
 *   - TG_ARGV[0]: author (TEXT) - The author of the import.
 *   - TG_ARGV[1]: currentVersion (BIGINT) - The version to assign to the feature.
 *   - TG_ARGV[2]: isPartial (BOOLEAN) - Whether the import is partial.
 *   - TG_ARGV[3]: onExists (TEXT) - Action to take if the feature exists.
 *   - TG_ARGV[4]: onNotExists (TEXT) - Action to take if the feature does not exist.
 *   - TG_ARGV[5]: onVersionConflict (TEXT) - Action on version conflict.
 *   - TG_ARGV[6]: onMergeConflict (TEXT) - Action on merge conflict.
 *   - TG_ARGV[7]: historyEnabled (BOOLEAN) - Whether history tracking is enabled.
 *   - TG_ARGV[8]: spaceContext (TEXT) - Context for the import.
 *   - TG_ARGV[9]: tables (TEXT) - Comma-separated list of tables.
 *   - TG_ARGV[10]: format (TEXT) - Import format (e\.g\., CSV_JSON_WKB, GEOJSON, CSV_GEOJSON).
 *   - TG_ARGV[11]: entityPerLine (TEXT) - Entity type per line (Feature or Features).
 *
 * Behavior:
 *   - Prepares input data based on format and entity type.
 *   - Sets up context for batch or single feature import.
 *   - Calls write_features to process and import the feature(s).
 *   - Updates NEW row with import results.
 *
 * Returns:
 *   - The enriched NEW row (trigger return).
 */
CREATE OR REPLACE FUNCTION import_from_s3_trigger_for_non_empty_layer() RETURNS trigger AS
$BODY$
DECLARE
    author text := TG_ARGV[0];
    currentVersion bigint := TG_ARGV[1];
    isPartial boolean := TG_ARGV[2];
    onExists text := TG_ARGV[3];
    onNotExists text := TG_ARGV[4];
    onVersionConflict text := TG_ARGV[5];
    onMergeConflict text := TG_ARGV[6];
    historyEnabled boolean := TG_ARGV[7]::boolean;
    spaceContext text := TG_ARGV[8];
    tables text := TG_ARGV[9];
    format text := TG_ARGV[10];
    entityPerLine text := TG_ARGV[11];

    feature_collection text;
    featureCount int := 0;
BEGIN
    --TODO: Remove the following workaround once the caller-side was fixed
    onExists = CASE WHEN onExists = 'null' THEN NULL ELSE onExists END;
    onNotExists = CASE WHEN onNotExists = 'null' THEN NULL ELSE onNotExists END;
    onVersionConflict = CASE WHEN onVersionConflict = 'null' THEN NULL ELSE onVersionConflict END;
    onMergeConflict = CASE WHEN onMergeConflict = 'null' THEN NULL ELSE onMergeConflict END;

    PERFORM context(
        jsonb_build_object(
            'stepId', get_stepid_from_work_table(TG_TABLE_NAME::regclass),
            'schema', TG_TABLE_SCHEMA,
            'tables', string_to_array(tables, ','),
            'historyEnabled', historyEnabled,
            'context', CASE WHEN spaceContext = 'null' THEN null ELSE spaceContext END,
            'batchMode', true
        )
    );

    IF format = 'CSV_JSON_WKB' THEN
        SELECT jsonb_build_object(
	       'type', 'FeatureCollection',
	       'features',
	       coalesce(
	           jsonb_agg(
	               jsonb_set(
	                   n.jsondata::jsonb,
	                   '{geometry}',
	                   xyz_reduce_precision(ST_AsGeoJSON(ST_Force3D(n.geo)), false)::JSONB
	               )
	           ),
	           '[]'::jsonb
	       )
	   )::text
        INTO feature_collection
            FROM new_rows n
        WHERE n.geo IS NOT NULL;
    END IF;

    IF format IN ('GEOJSON', 'CSV_GEOJSON') THEN
        IF entityPerLine = 'Feature' THEN
            SELECT jsonb_build_object(
                       'type', 'FeatureCollection',
                       'features',
                       COALESCE(jsonb_agg(n.jsondata::jsonb), '[]'::jsonb)
                   )::text
              INTO feature_collection
              FROM new_rows n;
        ELSE
            SELECT jsonb_build_object(
                       'type', 'FeatureCollection',
                       'features',
                       COALESCE(jsonb_agg(f.feature), '[]'::jsonb)
                   )::text
              INTO feature_collection
                  FROM new_rows n
              CROSS JOIN LATERAL jsonb_array_elements(n.jsondata::jsonb -> 'features') AS f(feature);
        END IF;
    ELSE
        RAISE EXCEPTION 'Unsupported format: %', format;
    END IF;

    IF feature_collection IS NOT NULL THEN
        SELECT (write_features(
                    feature_collection,
                    'FeatureCollection',
                    author,
                    false,
                    currentVersion,
                    onExists,
                    onNotExists,
                    onVersionConflict,
                    onMergeConflict,
                    isPartial
                )::jsonb ->> 'count')::int
          INTO featureCount;
    END IF;

    RETURN null;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;

/**
 * Function: import_from_s3_enrich_feature
 * (used in plain trigger function - without calling write_features)
 *
 * Purpose:
 *   Enriches a GeoJSON feature for import from S3, ensuring required metadata and geometry are set.
 *   - Assigns a random ID if missing.
 *   - Removes the 'bbox' property.
 *   - Sets the 'type' to 'Feature'.
 *   - Injects metadata (`createdAt`, `updatedAt`) into the properties.
 *   - Optionally retains existing metadata if `retain_meta` is true.
 *   - Extracts and processes geometry from the feature or uses the provided geometry.
 *   - Reduces geometry precision.
 *
 * Arguments:
 *   - jsondata (JSONB): The input feature data.
 *   - geo (geometry): The geometry to use if not present in `jsondata`.
 *   - retain_meta (BOOLEAN, default FALSE): Whether to retain existing metadata.
 *
 * Returns:
 *   - new_jsondata (JSONB): The enriched feature data.
 *   - new_geo (geometry): The processed geometry.
 *   - new_operation (character): The operation type ('I' for insert).
 *   - new_id (TEXT): The feature ID.
 */
CREATE OR REPLACE FUNCTION import_from_s3_enrich_feature(IN jsondata JSONB, geo geometry(GeometryZ,4326), retain_meta BOOLEAN DEFAULT FALSE)
    RETURNS TABLE(new_jsondata JSONB, new_geo geometry(GeometryZ,4326), new_operation character, new_id TEXT)
AS $BODY$
DECLARE
    fid TEXT := jsondata->>'id';
    createdAt BIGINT := FLOOR(EXTRACT(epoch FROM NOW()) * 1000);
    --TODO: Align with featureWriter. Currently we are also writing version and author there into the metadata.
    meta JSONB := format(
            '{
                 "createdAt": %s,
                 "updatedAt": %s
            }', createdAt, createdAt
                  );
BEGIN
    IF fid IS NULL THEN
        fid = xyz_random_string(10);
        jsondata := (jsondata || format('{"id": "%s"}', fid)::JSONB);
    END IF;

    -- Remove bbox on root
    jsondata := jsondata - 'bbox';

    -- Inject type
    jsondata := jsonb_set(jsondata, '{type}', '"Feature"');

    -- Inject meta
    IF retain_meta THEN
        meta := coalesce(jsonb_set(meta, '{createdAt}', (jsondata->'properties'->'@ns:com:here:xyz'->>'createdAt')::JSONB), meta);
        meta := coalesce(jsonb_set(meta, '{updatedAt}', (jsondata->'properties'->'@ns:com:here:xyz'->>'updatedAt')::JSONB), meta);
    END IF;
    jsondata := jsonb_set(jsondata, '{properties,@ns:com:here:xyz}', meta);

    IF jsondata->'geometry' IS NOT NULL THEN
        -- GeoJson Feature Import
        new_geo := ST_Force3D(ST_GeomFromGeoJSON(
            CASE WHEN (jsondata->'geometry') = 'null'::jsonb THEN NULL ELSE (jsondata->'geometry') END
        ));
        jsondata := jsondata - 'geometry';
    ELSE
        new_geo := ST_Force3D(geo);
    END IF;

    new_geo := xyz_reduce_precision(new_geo, false);
    new_jsondata := jsondata;
    new_operation := 'I';
    new_id := fid;

    RETURN NEXT;
END
$BODY$
LANGUAGE plpgsql VOLATILE;

/**
 * Function: execute_import_from_s3
 * (used for tasked import with retries)
 *
 * Purpose:
 *   Imports data from an S3 bucket into a target table using the AWS S3 extension.
 *   Supports automatic retries with exponential backoff for transient errors
 *   (see is_retryable_s3_sqlstate()).
 *
 * Arguments:
 *   - schema (TEXT): The schema name (not directly used in the function).
 *   - target_tbl (REGCLASS): The target table to import data into.
 *   - format (TEXT): The import format (used to fetch plugin config).
 *   - s3_bucket (TEXT): The S3 bucket name.
 *   - s3_key (TEXT): The S3 object key.
 *   - s3_region (TEXT): The AWS region of the S3 bucket.
 *   - max_attempts (INT, default 3): Maximum number of retry attempts.
 *   - attempts (INT, default 0): Current attempt count (internal, for recursion).
 *
 * Returns:
 *   - TEXT: Import statistics (e.g., '20 rows imported').
 */
CREATE OR REPLACE FUNCTION execute_import_from_s3(
        schema TEXT,
        target_tbl REGCLASS,
        format TEXT,
        s3_bucket TEXT, s3_key TEXT, s3_region TEXT,
        max_attempts INT DEFAULT 3,
        attempts INT DEFAULT 0
	)
RETURNS TEXT
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
    config RECORD;
    import_statistics TEXT;
BEGIN
    SELECT * FROM s3_plugin_config(format) into config;

    EXECUTE format(
            'SELECT aws_s3.table_import_from_s3( '
                ||' ''%1$s'', '
                ||'	%2$L, '
                ||'	%3$L, '
                ||' aws_commons.create_s3_uri(%4$L,%5$L,%6$L)) ',
            target_tbl,
            config.plugin_columns,
            config.plugin_options,
            s3_bucket,
            s3_key,
            s3_region) INTO import_statistics;

    RETURN import_statistics;

    EXCEPTION WHEN OTHERS THEN
        IF NOT is_retryable_s3_sqlstate(SQLSTATE) THEN
            RAISE;
        END IF;

        IF attempts >= max_attempts THEN
            RAISE EXCEPTION 'Import of ''%'' failed after ''%'' attempts. SQLSTATE: %, Message: %',
                s3_key, max_attempts, SQLSTATE, SQLERRM
                    USING ERRCODE = SQLSTATE;
        END IF;

        -- Exponential backoff: 10 s, 20 s, 40 s .. capped at 60 s
        PERFORM pg_sleep(s3_retry_backoff_ms(attempts) / 1000.0);

        RETURN execute_import_from_s3(
            schema,
            target_tbl,
            format,
            s3_bucket,
            s3_key,
            s3_region,
            max_attempts,
            attempts + 1
        );
END;
$BODY$;

/**
 * Function: execute_export_to_s3
 * (used for tasked export with retries)
 *
 * Purpose:
 *   Exports data from RDS to an S3 bucket using the AWS S3 extension.
 *   Supports automatic retries with exponential backoff for transient errors
 *   (see is_retryable_s3_sqlstate()).
 *
 * Arguments:
 *   - s3_bucket (TEXT): The target S3 bucket.
 *   - s3_path (TEXT): The target S3 object key/path.
 *   - s3_region (TEXT): The AWS region of the S3 bucket.
 *   - content_query (TEXT): SQL query producing the content to export.
 *   - max_attempts (INT, default 3): Maximum number of retry attempts.
 *   - attempts (INT, default 0): Current attempt count (internal, for recursion).
 *
 * Returns:
 *   - TABLE(rows_uploaded BIGINT, files_uploaded BIGINT, bytes_uploaded BIGINT):
 *     Export statistics returned by aws_s3.query_export_to_s3.
 */
CREATE OR REPLACE FUNCTION execute_export_to_s3(
        s3_bucket TEXT, s3_path TEXT, s3_region TEXT,
        content_query TEXT,
        max_attempts INT DEFAULT 3,
        attempts INT DEFAULT 0
    )
RETURNS TABLE(rows_uploaded BIGINT, files_uploaded BIGINT, bytes_uploaded BIGINT)
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
    config RECORD;
    export_statistics RECORD;
BEGIN
    SELECT * FROM s3_plugin_config('GEOJSON') INTO config;

    EXECUTE format(
           'SELECT * from aws_s3.query_export_to_s3( '
                ||' ''%1$s'', '
                ||' aws_commons.create_s3_uri(%2$L,%3$L,%4$L),'
                ||' %5$L )',
            format('select jsondata || jsonb_build_object(''''geometry'''', ST_AsGeoJSON(geo, 8)::jsonb) from (%1$s) X',
                    REPLACE(content_query, '''', '''''')),
            s3_bucket,
            s3_path,
            s3_region,
            REGEXP_REPLACE(config.plugin_options, '[\(\)]', '', 'g')
            ) INTO export_statistics;

    rows_uploaded := export_statistics.rows_uploaded;
    files_uploaded := export_statistics.files_uploaded;
    bytes_uploaded := export_statistics.bytes_uploaded;
    RETURN NEXT;

    EXCEPTION WHEN OTHERS THEN
        IF NOT is_retryable_s3_sqlstate(SQLSTATE) THEN
            RAISE;
        END IF;

        IF attempts >= max_attempts THEN
            RAISE EXCEPTION 'Export to ''%'' failed after ''%'' attempts. SQLSTATE: %, Message: %',
                s3_path, max_attempts, SQLSTATE, SQLERRM
                    USING ERRCODE = SQLSTATE;
        END IF;

        -- Exponential backoff: 10 s, 20 s, 40 s .. capped at 60 s
        PERFORM pg_sleep(s3_retry_backoff_ms(attempts) / 1000.0);

        RETURN QUERY SELECT * FROM execute_export_to_s3(
            s3_bucket,
            s3_path,
            s3_region,
            content_query,
            max_attempts,
            attempts + 1
        );
END;
$BODY$;

-- ####################################################################################################################
-- Task related functions --

/**
 * Function: get_task_item_and_statistics
 *
 * Purpose:
 *   Retrieves statistics about tasks and fetches the next available task item for processing.
 *   - Returns total, started, finalized task counts.
 *   - Returns the next unlocked task item, or indicates if no work is left.
 *
 * Behavior:
 *   1. Gets the current context.
 *   2. Queries the job table for total, started, and finalized tasks.
 *   3. If all tasks are finalized, returns an empty result.
 *   4. Otherwise, attempts to lock and retrieve the next unstarted task.
 *   5. If a task is found, marks it as started and returns it.
 *   6. If all unstarted tasks are locked, waits and retries.
 *   7. If no unstarted tasks exist, returns an empty result.
 *
 * Returns:
 *   TABLE (
 *     total INT,         -- Total number of tasks
 *     started INT,       -- Number of started tasks
 *     finalized INT,     -- Number of finalized tasks
 *     task_id INT,       -- Task identifier (-1 if no work)
 *     task_input JSONB   -- Task input data (empty if no work)
 *   )
 */
CREATE OR REPLACE FUNCTION get_task_item_and_statistics()
    RETURNS TABLE (total INT, started INT, finalized INT, task_id INT, task_input JSONB) AS $$
DECLARE
    v_total INT := 0;
    v_started INT := 0;
    v_finalized INT := 0;
    task_item RECORD;
    ctx JSONB;
BEGIN
    SELECT context() INTO ctx;

    -- Get statistics
    EXECUTE format(
            'SELECT
                 COUNT(1)::int,
                 COALESCE(SUM((A.started = true)::int), 0)::int,
                 COALESCE(SUM((A.finalized = true)::int), 0)::int
             FROM %1$s A;',
           get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE')
    ) INTO v_total, v_started, v_finalized;

    -- No work left
    IF v_total = v_finalized THEN
        RETURN QUERY SELECT v_total, v_started, v_finalized, -1, '{"type" : "Empty"}'::JSONB;
        RETURN;
    END IF;

    -- Retrieve next task_item (will be NULL if all are locked)
    EXECUTE format('SELECT B.task_id, B.task_input
            FROM %1$s B
            WHERE B.started = false
            ORDER BY random()
            LIMIT 1
            FOR UPDATE SKIP LOCKED;',
           get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE')
    ) INTO task_item;

    -- If a task is found, mark it as started
    IF task_item.task_id IS NOT NULL THEN
        EXECUTE format(
            'UPDATE %1$s C
                SET started = true
            WHERE C.task_id = %2$L;',
            get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE'),
            task_item.task_id
       );

       RETURN QUERY SELECT v_total, v_started, v_finalized, task_item.task_id, task_item.task_input;
       RETURN;
    END IF;

    IF v_total > v_finalized + v_started THEN
        -- There are unstarted tasks, but all are locked -> Wait & retry
        PERFORM pg_sleep(2);
        RETURN QUERY SELECT * FROM get_task_item_and_statistics();
    ELSE
        -- No unstarted tasks exist -> return no work
        RETURN QUERY SELECT v_total, v_started, v_finalized, -1, '{"type" : "Empty"}'::JSONB;
    END IF;
END;
$$ LANGUAGE plpgsql VOLATILE;

/**
 * Function: update_task_item_and_get_task_item_and_statistics
 *
 * Purpose:
 *   Atomically finalizes a task item and fetches updated task statistics plus the next task item.
 *
 * Behavior:
 *   1. Updates task_output/finalized for the provided task_id.
 *   2. Computes total/started/finalized statistics.
 *   3. If work remains, claims the next unstarted task using FOR UPDATE SKIP LOCKED.
 *   4. Returns statistics + claimed task (or task_id=-1 if nothing claimable).
 *
 * Returns:
 *   TABLE (
 *     total INT,
 *     started INT,
 *     finalized INT,
 *     task_id INT,
 *     task_input JSONB
 *   )
 */
CREATE OR REPLACE FUNCTION update_task_item_and_get_task_item_and_statistics(
    p_task_id INT,
    p_task_output JSONB,
    p_finalized BOOLEAN DEFAULT true
)
    RETURNS TABLE (total INT, started INT, finalized INT, task_id INT, task_input JSONB) AS $$
DECLARE
    v_total INT := 0;
    v_started INT := 0;
    v_finalized INT := 0;
    v_task_item RECORD;
    ctx JSONB;
BEGIN
    SELECT context() INTO ctx;
    -- Set provided task as finalized and store output
    EXECUTE format(
        'UPDATE %1$s t
            SET task_output = %2$L::JSONB,
                finalized = %3$L
          WHERE task_id = %4$L;',
        get_table_reference(ctx->>'schema', ctx->>'stepId', 'JOB_TABLE'),
        p_task_output::TEXT,
        p_finalized,
        p_task_id
    );

    RETURN QUERY SELECT * FROM get_task_item_and_statistics();
END;
$$ LANGUAGE plpgsql VOLATILE;

/**
 * Function: report_task_progress
 *
 * Purpose:
 *   Reports the progress of a specific task by invoking the report_progress function,
 *   which triggers an AWS Lambda function with a payload containing task update information.
 *
 * Arguments:
 *   - lambda_function_arn (TEXT): ARN of the Lambda function to invoke.
 *   - lambda_region (TEXT): AWS region of the Lambda function.
 *   - step_payload (JSON): Payload describing the current step context.
 *   - task_id (INT): Identifier of the task being reported.
 *   - task_output (JSONB): Output data of the task.
 *
 * Behavior:
 *   - Calls report_progress with a JSON object containing type 'SpaceBasedTaskUpdate',
 *     the task ID, and the task output.
 *
 * Returns:
 *   - VOID
 */
CREATE OR REPLACE FUNCTION report_task_progress(
    lambda_function_arn TEXT,
	lambda_region TEXT,
	step_payload JSON,
	task_id INT,
	task_output JSONB
)
RETURNS VOID
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
BEGIN
	PERFORM report_progress(
		lambda_function_arn,
		lambda_region,
		step_payload,
		json_build_object(
		       'type','SpaceBasedTaskUpdate',
			   'taskId', task_id,
			   'taskOutput', task_output
			)
	);
END
$BODY$;

/**
 * Function: export_to_s3_perform
 * (tasked export to S3)
 *
 * Purpose:
 *   Exports data from RDS to S3 using the AWS S3 extension.
 *   Wraps the export in a DO block for dynamic execution and error handling.
 *   Reports progress to an AWS Lambda function.
 *
 * Arguments:
 *   - task_id (INT): Identifier for the export task.
 *   - s3_bucket (TEXT): Target S3 bucket.
 *   - s3_path (TEXT): Target S3 path.
 *   - s3_region (TEXT): AWS region for S3.
 *   - step_payload (JSON): Step context payload.
 *   - lambda_function_arn (TEXT): ARN of the Lambda function for progress reporting.
 *   - lambda_region (TEXT): AWS region of the Lambda function.
 *   - content_query (TEXT): SQL query for content to export.
 *   - failure_callback (TEXT): Callback to execute on failure.
 *
 * Behavior:
 *   - Fetches plugin config for GEOJSON format.
 *   - Executes export using aws_s3.query_export_to_s3.
 *   - Reports export statistics (bytes, rows, files) to Lambda.
 *   - On error, executes the failure callback.
 *
 * Returns:
 *   - VOID
 */
CREATE OR REPLACE FUNCTION export_to_s3_perform(
        task_id INT,
        s3_bucket TEXT, s3_path TEXT, s3_region TEXT,
        step_payload JSON,
        lambda_function_arn TEXT,
        lambda_region TEXT,
        content_query TEXT,
        failure_callback TEXT
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
	sql_text TEXT;
BEGIN
	sql_text = $wrappedouter$ DO
	$wrappedinner$
	DECLARE
		export_statistics RECORD;
        task_id INT := $wrappedouter$||task_id||$wrappedouter$::INT;
		content_query TEXT := $x$$wrappedouter$||coalesce(content_query,'')||$wrappedouter$$x$::TEXT;
        s3_bucket TEXT := '$wrappedouter$||s3_bucket||$wrappedouter$'::TEXT;
		s3_path TEXT := '$wrappedouter$||s3_path||$wrappedouter$'::TEXT;
		s3_region TEXT := '$wrappedouter$||s3_region||$wrappedouter$'::TEXT;
		step_payload JSON := '$wrappedouter$||(step_payload::TEXT)||$wrappedouter$'::JSON;
		lambda_function_arn TEXT := '$wrappedouter$||lambda_function_arn||$wrappedouter$'::TEXT;
		lambda_region TEXT := '$wrappedouter$||lambda_region||$wrappedouter$'::TEXT;
	BEGIN
	    SELECT * FROM execute_export_to_s3(
	        s3_bucket,
	        s3_path,
	        s3_region,
	        content_query
	    ) INTO export_statistics;

		PERFORM report_task_progress(
			 lambda_function_arn,
			 lambda_region,
			 step_payload,
			 task_id,
		     jsonb_build_object(
                'bytes', export_statistics.bytes_uploaded,
                'rows', export_statistics.rows_uploaded,
                'files', export_statistics.files_uploaded::int,
			    'type', 'ExportOutput'
            )
		);

		EXCEPTION
		 	WHEN OTHERS THEN
		 		-- Export has failed
		 		BEGIN
		 			$wrappedouter$ || failure_callback || $wrappedouter$
		 		END;
	END;
	$wrappedinner$ $wrappedouter$;
	EXECUTE sql_text;
END;
$BODY$;

/**
 * Function: perform_import_from_s3_task
 * (tasked import from S3 into RDS)
 *
 * Purpose:
 *   Performs a tasked import from S3 into RDS, handling progress reporting and error callbacks.
 *   Wraps the import logic in a DO block for dynamic execution and error handling.
 *
 * Arguments:
 *   - task_id (INT): Identifier for the import task.
 *   - schema (TEXT): Target schema name.
 *   - target_tbl (REGCLASS): Target table for import.
 *   - format (TEXT): Import format (e\.g\., CSV_JSON_WKB, GEOJSON).
 *   - s3_bucket (TEXT): Source S3 bucket.
 *   - s3_key (TEXT): Source S3 object key.
 *   - s3_region (TEXT): AWS region for S3.
 *   - file_bytes (BIGINT): Size of the file to import.
 *   - step_payload (JSON): Step context payload.
 *   - lambda_function_arn (TEXT): ARN of the Lambda function for progress reporting.
 *   - lambda_region (TEXT): AWS region of the Lambda function.
 *   - failure_callback (TEXT): Callback to execute on failure.
 *
 * Behavior:
 *   - Executes the import using execute_import_from_s3.
 *   - Reports import statistics and file size to Lambda via report_task_progress.
 *   - On error, executes the failure callback.
 *
 * Returns:
 *   - VOID
 */
CREATE OR REPLACE FUNCTION perform_import_from_s3_task(
        task_id INT,
        schema TEXT,
        target_tbl REGCLASS,
        format TEXT,
        s3_bucket TEXT, s3_key TEXT, s3_region TEXT,
        file_bytes BIGINT,
        step_payload JSON,
        lambda_function_arn TEXT,
        lambda_region TEXT,
        failure_callback TEXT
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
	sql_text TEXT;
BEGIN
	sql_text = $wrappedouter$ DO
	$wrappedinner$
	DECLARE
        import_statistics TEXT;

        task_id INT := $wrappedouter$||task_id||$wrappedouter$::INT;
        schema TEXT := '$wrappedouter$||schema||$wrappedouter$'::TEXT;
        target_tbl REGCLASS := '$wrappedouter$||target_tbl||$wrappedouter$'::REGCLASS;
        format TEXT := '$wrappedouter$||format||$wrappedouter$'::TEXT;
        s3_bucket TEXT := '$wrappedouter$||s3_bucket||$wrappedouter$'::TEXT;
		s3_key TEXT := '$wrappedouter$||s3_key||$wrappedouter$'::TEXT;
		s3_region TEXT := '$wrappedouter$||s3_region||$wrappedouter$'::TEXT;
        file_bytes BIGINT := '$wrappedouter$||file_bytes||$wrappedouter$'::BIGINT;
		step_payload JSON := '$wrappedouter$||(step_payload::TEXT)||$wrappedouter$'::JSON;
		lambda_function_arn TEXT := '$wrappedouter$||lambda_function_arn||$wrappedouter$'::TEXT;
		lambda_region TEXT := '$wrappedouter$||lambda_region||$wrappedouter$'::TEXT;
	BEGIN
        SELECT execute_import_from_s3(
            schema,
            target_tbl,
            format,
            s3_bucket,
            s3_key,
            s3_region
        ) INTO import_statistics;

		PERFORM report_task_progress(
			 lambda_function_arn,
			 lambda_region,
			 step_payload,
			 task_id,
		     jsonb_build_object(
                'importStatistics', import_statistics,
                'fileBytes', file_bytes,
                'type', 'ImportOutput'
            )
		);

		EXCEPTION
		 	WHEN OTHERS THEN
		 		BEGIN
		 			$wrappedouter$ || failure_callback || $wrappedouter$
		 		END;
	END;
	$wrappedinner$ $wrappedouter$;
	EXECUTE sql_text;
END;
$BODY$;

/**
 * Function: perform_example_task
 *
 * Purpose:
 *   Example task function demonstrating how to execute a custom operation,
 *   report progress to an AWS Lambda function, and handle errors via a callback.
 *
 * Arguments:
 *   - task_id (INT): Identifier for the task.
 *   - some_var (TEXT): Example variable for custom logic.
 *   - step_payload (JSON): Step context payload.
 *   - lambda_function_arn (TEXT): ARN of the Lambda function for progress reporting.
 *   - lambda_region (TEXT): AWS region of the Lambda function.
 *   - failure_callback (TEXT): Callback to execute on failure.
 *
 * Behavior:
 *   - Executes a custom operation (replace `some_operation()` as needed).
 *   - Reports task progress and output to Lambda via `report_task_progress`.
 *   - On error, executes the failure callback.
 *
 * Returns:
 *   - VOID
 */
CREATE OR REPLACE FUNCTION perform_example_task(
        task_id INT,
        some_var TEXT,
        step_payload JSON,
        lambda_function_arn TEXT,
        lambda_region TEXT,
        failure_callback TEXT
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
	sql_text TEXT;
BEGIN
	sql_text = $wrappedouter$ DO
	$wrappedinner$
	DECLARE
		task_result RECORD;
        task_id INT := $wrappedouter$||task_id||$wrappedouter$::INT;
        some_var TEXT := '$wrappedouter$||some_var||$wrappedouter$'::TEXT;
		step_payload JSON := '$wrappedouter$||(step_payload::TEXT)||$wrappedouter$'::JSON;
		lambda_function_arn TEXT := '$wrappedouter$||lambda_function_arn||$wrappedouter$'::TEXT;
		lambda_region TEXT := '$wrappedouter$||lambda_region||$wrappedouter$'::TEXT;
	BEGIN
	    -- Replace the following with actual task logic
	    -- If needed function should implement retry logic similar to tasked import function
    	PERFORM some_operation() INTO result;

		PERFORM report_task_progress(
			 lambda_function_arn,
			 lambda_region,
			 step_payload,
			 task_id,
		     -- Build jsonb object for task output. Adjust as needed.
		     jsonb_build_object(
                'field1', task_result.field1,
		        -- Use the type of the task output of your step implementation
			    'type', 'DefinedTaskOutput'
            )
		);

		EXCEPTION
		 	WHEN OTHERS THEN
		 		-- Task has failed -> execute failure callback
		 		BEGIN
		 			$wrappedouter$ || failure_callback || $wrappedouter$
		 		END;
	END;
	$wrappedinner$ $wrappedouter$;
	EXECUTE sql_text;
END;
$BODY$;