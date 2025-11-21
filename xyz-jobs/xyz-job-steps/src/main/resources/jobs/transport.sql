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
--TODO: Remove code-duplication of the following trigger functions!!
CREATE OR REPLACE FUNCTION import_from_s3_trigger_for_non_empty_layer() RETURNS trigger AS
$BODY$
DECLARE
    author TEXT := TG_ARGV[0];
    currentVersion BIGINT := TG_ARGV[1];
    isPartial BOOLEAN := TG_ARGV[2];
    onExists TEXT := TG_ARGV[3];
    onNotExists TEXT := TG_ARGV[4];
    onVersionConflict TEXT := TG_ARGV[5];
    onMergeConflict TEXT := TG_ARGV[6];
    historyEnabled BOOLEAN := TG_ARGV[7]::BOOLEAN;
    spaceContext TEXT := TG_ARGV[8];
    tables TEXT := TG_ARGV[9];
    format TEXT := TG_ARGV[10];
    entityPerLine TEXT := TG_ARGV[11];
    featureCount INT := 0;
    input TEXT;
    inputType TEXT;
BEGIN
    --TODO: Remove the following workaround once the caller-side was fixed
    onExists = CASE WHEN onExists = 'null' THEN NULL ELSE onExists END;
    onNotExists = CASE WHEN onNotExists = 'null' THEN NULL ELSE onNotExists END;
    onVersionConflict = CASE WHEN onVersionConflict = 'null' THEN NULL ELSE onVersionConflict END;
    onMergeConflict = CASE WHEN onMergeConflict = 'null' THEN NULL ELSE onMergeConflict END;

    -- TODO: remove support for CSV_JSON_WKB and CSV_GEOJSON if all import process are tasked based
    IF format = 'CSV_JSON_WKB' AND NEW.geo IS NOT NULL THEN
        --TODO: Extend feature_writer with possibility to provide geometry (as JSONB manipulations are quite slow)
        --TODO: Remove unnecessary xyz_reduce_precision call, because the FeatureWriter will do it anyways
        NEW.jsondata := jsonb_set(NEW.jsondata::JSONB, '{geometry}', xyz_reduce_precision(ST_ASGeojson(ST_Force3D(NEW.geo)), false)::JSONB);
        input = NEW.jsondata::TEXT;
        inputType = 'Feature';
    END IF;

    IF format = 'GEOJSON' OR  format = 'CSV_GEOJSON' THEN
        IF entityPerLine = 'Feature' THEN
            input = NEW.jsondata::TEXT;
            inputType = 'Feature';
        ELSE
            --TODO: Shouldn't the input be a FeatureCollection here? Seems to be a list of Features
            input = (NEW.jsondata::JSONB->'features')::TEXT;
            inputType = 'Features';
        END IF;
    END IF;

    --TODO: check how to use asyncify instead
    PERFORM context(
        jsonb_build_object(
            'stepId', get_stepid_from_work_table(TG_TABLE_NAME::REGCLASS) ,
            'schema', TG_TABLE_SCHEMA,
            'tables', string_to_array(tables, ','),
            'historyEnabled', historyEnabled,
            'context', CASE WHEN spaceContext = 'null' THEN null ELSE spaceContext END,
            'batchMode', inputType != 'Feature'
        )
    );

    SELECT write_features(
        input, inputType, author, false, currentVersion,
        onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial
    )::JSONB->'count' INTO featureCount;

    NEW.jsondata = NULL;
    NEW.geo = NULL;
    NEW.count = featureCount;

    RETURN NEW;
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
        new_geo := ST_Force3D(ST_GeomFromGeoJSON(jsondata->'geometry'));
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
 * Function: perform_import_from_s3
 * (used for tasked import with retries)
 *
 * Purpose:
 *   Imports data from an S3 bucket into a target table using the AWS S3 extension.
 *   Supports automatic retries with exponential backoff for transient errors.
 *
 * Arguments:
 *   - schema (TEXT): The schema name (not directly used in the function).
 *   - target_tbl (REGCLASS): The target table to import data into.
 *   - format (TEXT): The import format (used to fetch plugin config).
 *   - s3_bucket (TEXT): The S3 bucket name.
 *   - s3_key (TEXT): The S3 object key.
 *   - s3_region (TEXT): The AWS region of the S3 bucket.
 *   - max_attempts (INT, default 2): Maximum number of retry attempts.
 *   - attempts (INT, default 0): Current attempt count.
 *
 * Returns:
 *   - TEXT: Import statistics (e.g., '20 rows imported').
 *
 * Behavior:
 *   - Fetches plugin configuration for the given format.
 *   - Executes the import using aws_s3.table_import_from_s3.
 *   - On retryable errors (lock, duplicate key, invalid input, extra data), waits and retries.
 *   - Uses exponential backoff for delay between retries, capped at 10 seconds.
 *   - Raises an exception if max_attempts is exceeded.
 */
CREATE OR REPLACE FUNCTION perform_import_from_s3(
        schema TEXT,
        target_tbl REGCLASS,
        format TEXT,
        s3_bucket TEXT, s3_key TEXT, s3_region TEXT,
        max_attempts INT DEFAULT 2,
        attempts INT DEFAULT 0
	)
RETURNS TEXT
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
    config RECORD;
    import_statistics TEXT;
    base_delay_ms INT := 500;
    delay_ms INT;
BEGIN
    -- Calculate exponential backoff delay: base * 2^attempts, capped at 10 seconds
    delay_ms := LEAST(base_delay_ms * (2 ^ attempts), 10000);

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

    EXCEPTION
        WHEN SQLSTATE '55P03' OR  SQLSTATE '23505' OR  SQLSTATE '22P02' OR  SQLSTATE '22P04' THEN
            IF attempts < max_attempts THEN
                --0.5 s, 1 s, 2 s, 4 s, 8 s, 10 s .. max
                PERFORM pg_sleep(delay_ms / 1000.0);

                RETURN perform_import_from_s3(
                    schema,
                    target_tbl,
                    format,
                    s3_bucket,
                    s3_key,
                    s3_region,
                    max_attempts,
                    attempts + 1
                );
    ELSE
        RAISE EXCEPTION 'Import of ''%'' failed after ''%'' attempts. SQLSTATE: %, Message: %',
            s3_key, max_attempts, SQLSTATE, SQLERRM
                USING ERRCODE = SQLSTATE;
    END IF;
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
    v_total INT;
    v_started INT;
    v_finalized INT;
    task_item RECORD;
    ctx JSONB;
BEGIN
    SELECT context() INTO ctx;

    -- Get statistics
    EXECUTE format('SELECT COUNT(1),
            SUM((A.started = true)::int),
            SUM((A.finalized = true)::int)
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
        EXECUTE format('UPDATE %1$s C
                SET started = true
            WHERE C.task_id = %2$L;',
            get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE'),
            task_item.task_id);

        RETURN QUERY SELECT v_total, v_started, v_finalized, task_item.task_id, task_item.task_input;
    ELSIF v_total > v_finalized + v_started THEN
        -- There are unstarted tasks, but all are locked -> Wait & retry
        PERFORM pg_sleep(500);
        RETURN QUERY SELECT * FROM "jobs.transport".get_task_item_and_statistics();
    ELSE
        -- No unstarted tasks exist -> return no work
        RETURN QUERY SELECT v_total, v_started, v_finalized, -1, '{"type" : "Empty"}'::JSONB;
    END IF;
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
	    config RECORD;
        task_id INT := $wrappedouter$||task_id||$wrappedouter$::INT;
		content_query TEXT := $x$$wrappedouter$||coalesce(content_query,'')||$wrappedouter$$x$::TEXT;
        s3_bucket TEXT := '$wrappedouter$||s3_bucket||$wrappedouter$'::TEXT;
		s3_path TEXT := '$wrappedouter$||s3_path||$wrappedouter$'::TEXT;
		s3_region TEXT := '$wrappedouter$||s3_region||$wrappedouter$'::TEXT;
		step_payload JSON := '$wrappedouter$||(step_payload::TEXT)||$wrappedouter$'::JSON;
		lambda_function_arn TEXT := '$wrappedouter$||lambda_function_arn||$wrappedouter$'::TEXT;
		lambda_region TEXT := '$wrappedouter$||lambda_region||$wrappedouter$'::TEXT;
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
	            )INTO export_statistics;

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
 *   - Executes the import using perform_import_from_s3.
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
        SELECT perform_import_from_s3(
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

-- ####################################################################################################################   
-- Delete Functions below when movement to new tasked import is done --

/**
 * Get work_item. Used for synchronizing threads.
 */
CREATE OR REPLACE FUNCTION get_work_item(temporary_tbl REGCLASS)
    RETURNS JSONB
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
    work_items_left INT := 1;
    success_marker TEXT := 'SUCCESS_MARKER';
    target_state TEXT := 'RUNNING';
    work_item RECORD;
    result RECORD;
    updated_rows INT;
BEGIN
    -- Try to get a work item in SUBMITTED, RETRY, or FAILED state
    EXECUTE format('SELECT state, s3_path, execution_count FROM %1$s '
                   ||'WHERE state IN (''SUBMITTED'', ''RETRY'', ''FAILED'') ORDER BY random() LIMIT 1;',
                   temporary_tbl) INTO work_item;

    -- If a work item is found, try to update it
    IF work_item.state IS NOT NULL THEN
        EXECUTE format('UPDATE %1$s SET state = %2$L WHERE s3_path = %3$L AND state = %4$L RETURNING *',
                       temporary_tbl, target_state, work_item.s3_path, work_item.state) INTO result;

        GET DIAGNOSTICS updated_rows = ROW_COUNT;

        -- If updated, return the work item as JSONB
        IF updated_rows = 1 THEN
            RETURN jsonb_build_object(
                's3_bucket', result.s3_bucket,
                's3_path', result.s3_path,
                's3_region', result.s3_region,
                'filesize', result.data->'filesize',
                'state', result.state,
                'execution_count', result.execution_count,
                'data', result.data
            );
        ELSE
            RETURN jsonb_build_object('state', 'RETRY');
        END IF;
    END IF;

    -- Check if there are any items left in the RUNNING or SUBMITTED state
    EXECUTE format('SELECT count(1) FROM %1$s WHERE state IN (''RUNNING'',''SUBMITTED'');',
               temporary_tbl) into work_items_left;

    IF work_items_left > 0 THEN
        RETURN jsonb_build_object('state', 'LAST_ONES_RUNNING', 's3_path', success_marker);
    END IF;

    -- Check if a SUCCESS_MARKER exists, and attempt to update it
    EXECUTE format('SELECT s3_path, state FROM %1$s WHERE state = %2$L;', temporary_tbl, success_marker) INTO work_item;

    IF work_item.state IS NOT NULL THEN
        EXECUTE format('UPDATE %1$s SET state = %2$L, execution_count = execution_count + 1 '
                       ||'WHERE s3_path = %3$L AND state = %4$L RETURNING *',
                       temporary_tbl, work_item.state || '_' || target_state, work_item.s3_path, work_item.state) INTO result;

        GET DIAGNOSTICS updated_rows = ROW_COUNT;

        IF updated_rows = 1 THEN
            RETURN jsonb_build_object(
                's3_bucket', result.s3_bucket,
                's3_path', result.s3_path,
                's3_region', result.s3_region,
                'filesize', result.data->'filesize',
                'state', result.state,
                'execution_count', result.execution_count,
                'data', result.data
            );
        ELSE
            RETURN jsonb_build_object('state', 'RETRY');
        END IF;
    END IF;

    RETURN NULL;
END;
$BODY$;

/**
 *..
 */
CREATE OR REPLACE FUNCTION perform_work_item(work_item JSONB, format TEXT, content_query TEXT)
    RETURNS VOID
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
    ctx JSONB;
BEGIN
    SELECT context() into ctx;
    IF content_query IS NULL OR content_query = '' THEN
        PERFORM import_from_s3_perform(ctx->>'schema',
                    get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE'),
                    get_table_reference(ctx->>'schema', ctx->>'stepId', 'TRIGGER_TABLE'),
                    work_item ->> 's3_bucket',
                    work_item ->> 's3_path',
                    work_item ->> 's3_region',
                    format,
                    CASE WHEN (work_item -> 'filesize') = 'null'::jsonb THEN 0 ELSE (work_item -> 'filesize')::BIGINT END);
    ELSE
        PERFORM export_to_s3_perform(content_query, (work_item ->> 's3_bucket'), (work_item ->> 's3_path'), (work_item ->> 's3_region'));
    END IF;
END;
$BODY$;

/**
 * Report Success
 */
CREATE OR REPLACE FUNCTION report_success(success_callback TEXT)
    RETURNS void
    LANGUAGE 'plpgsql'
    VOLATILE PARALLEL SAFE
AS $BODY$
DECLARE
    ctx JSONB;
    sql_text TEXT;
BEGIN
    SELECT context() into ctx;

    sql_text = $wrappedouter$ DO
    $wrappedinner$
    DECLARE
        ctx JSONB := '$wrappedouter$||(ctx::TEXT)||$wrappedouter$'::JSONB;
	    job_results RECORD;
	    retry_count INT := 2;
	BEGIN
	    EXECUTE format('SELECT '
	                       || '   COUNT(*) FILTER (WHERE state = %1$L) AS finished_count,'
	                       || '   COUNT(*) FILTER (WHERE state = %2$L and execution_count=%3$L) AS failed_count,'
	                       || '   COUNT(*) AS total_count '
	                       || 'FROM %4$s WHERE NOT starts_with(state,''SUCCESS_MARKER'');',
	                   'FINISHED',
	                   'FAILED',
	                   retry_count,
                       get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE')
	            ) INTO job_results;

	    IF  (job_results.finished_count + job_results.failed_count) = job_results.total_count THEN
	        -- Will only be executed from last worker
	        IF job_results.total_count = job_results.failed_count  THEN
	            -- All Job-Threads are failed
	        ELSEIF job_results.failed_count > 0 AND (job_results.total_count > job_results.failed_count) THEN
	            -- Job-Threads partially failed
	        ELSE
	            -- All done invoke lambda
	            $wrappedouter$ || success_callback || $wrappedouter$
	        END IF;
	    ELSE
	        -- Job-Threads still in progress!
	    END IF;
	END;
	$wrappedinner$ $wrappedouter$;
    EXECUTE sql_text;
END;
$BODY$;

/**
 *..
 */
CREATE OR REPLACE PROCEDURE execute_transfer(format TEXT, success_callback TEXT, failure_callback TEXT, content_query TEXT = NULL)
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    ctx JSONB;
    work_item JSONB;
    sql_text TEXT;
BEGIN
    SELECT context() into ctx;
    SELECT * from get_work_item(get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE')) into work_item;
    --PERFORM write_log(work_item::text,'execute_transfer');

    COMMIT;
    IF work_item -> 'state' IS NULL THEN
        RETURN;
    ELSE
        PERFORM context(ctx);
        IF work_item ->> 'state' = 'RETRY' THEN
            -- Received a RETRY
            PERFORM asyncify(format('CALL execute_transfer(%1$L, %2$L, %3$L, %4$L );',
                                    format,
                                    success_callback,
                                    failure_callback,
                                    content_query
                    ), false, true );
            RETURN;
        ELSEIF work_item ->> 'state' = 'SUCCESS_MARKER_RUNNING' THEN
            EXECUTE format('SELECT report_success(%1$L);', success_callback);
            RETURN;
        END IF;
    END IF;

    sql_text = $wrappedouter$ DO
    $wrappedinner$
    DECLARE
		ctx JSONB := '$wrappedouter$||(ctx::TEXT)||$wrappedouter$'::JSONB;
        work_item JSONB := '$wrappedouter$||(work_item::TEXT)||$wrappedouter$'::JSONB;
        format TEXT := '$wrappedouter$||format||$wrappedouter$'::TEXT;
        content_query TEXT := $x$$wrappedouter$||coalesce(content_query,'')||$wrappedouter$$x$::TEXT;
		retry_count INT := 2;
    BEGIN
			BEGIN
				PERFORM context(ctx);
			    IF (work_item -> 'execution_count')::INT >= retry_count THEN
			        --TODO: find a solution to read a given hint in the failure_callback. Remove than the duplication.
                    RAISE EXCEPTION 'Error on processing file ''%''. Maximum retries are reached %. Details: ''%''',
			                (work_item ->>'s3_path'), retry_count, (work_item -> 'data' -> 'error' ->> 'sqlstate')
                    --USING HINT = 'Details: ' || 'details' ,
                    USING ERRCODE = 'XYZ50';
                END IF;

	            IF work_item ->> 's3_bucket' != 'SUCCESS_MARKER' THEN
	                PERFORM perform_work_item(work_item, format, content_query);
	            END IF;

				EXCEPTION
					WHEN OTHERS THEN
						-- Transfer has failed
						BEGIN
							$wrappedouter$ || failure_callback || $wrappedouter$
							RETURN;
						END;
			END;

            PERFORM asyncify(format('CALL execute_transfer(%1$L, %2$L, %3$L, %4$L);',
                format,
                '$wrappedouter$||REPLACE(success_callback, '''', '''''')||$wrappedouter$'::TEXT,
                '$wrappedouter$||REPLACE(failure_callback, '''', '''''')||$wrappedouter$'::TEXT,
                content_query
               ), false, true );
    END;
	$wrappedinner$ $wrappedouter$;
    EXECUTE sql_text;
END;
$BODY$;

/**
 *  Report progress by invoking lambda function with UPDATE_CALLBACK payload
 */
CREATE OR REPLACE FUNCTION report_progress(
    lambda_function_arn TEXT,
	lambda_region TEXT,
	step_payload JSON,
	progress_data JSON
)
RETURNS VOID
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
    lamda_response RECORD;
BEGIN
   --TODO: Add error handling
    SELECT aws_lambda.invoke(aws_commons.create_lambda_function_arn(lambda_function_arn, lambda_region),
                         json_build_object(
                                 'type','UPDATE_CALLBACK',
                                 'step', step_payload,
                                 'processUpdate', progress_data
                         ), 'Event') INTO lamda_response;
END
$BODY$;

/**
 * Perform single import from S3
 */
CREATE OR REPLACE FUNCTION import_from_s3_perform(schem TEXT, temporary_tbl REGCLASS ,target_tbl REGCLASS,
                                                  s3_bucket TEXT, s3_path TEXT, s3_region TEXT, format TEXT, filesize BIGINT)
    RETURNS void
    LANGUAGE 'plpgsql'
    VOLATILE
AS $BODY$
DECLARE
    import_statistics RECORD;
    config RECORD;
BEGIN
    select * from s3_plugin_config(format) into config;

    EXECUTE format(
            'SELECT/*lables({"type": "ImortFilesToSpace","bytes":%1$L})*/ aws_s3.table_import_from_s3( '
                ||' ''%2$s.%3$s'', '
                ||'	%4$L, '
                ||'	%5$L, '
                ||' aws_commons.create_s3_uri(%6$L,%7$L,%8$L)) ',
            filesize,
            schem,
            target_tbl,
            config.plugin_columns,
            config.plugin_options,
            s3_bucket,
            s3_path,
            s3_region
            ) INTO import_statistics;

    -- Mark item as successfully imported. Store import_statistics.
    EXECUTE format('UPDATE %1$s '
                       ||'set state = %2$L, '
                       ||'execution_count = execution_count + 1, '
                       ||'data = data || %3$L '
                       ||'WHERE s3_path = %4$L ',
                   temporary_tbl,
                   'FINISHED',
                   json_build_object('import_statistics', import_statistics),
                   s3_path);
EXCEPTION
    WHEN SQLSTATE '55P03' OR  SQLSTATE '23505' OR  SQLSTATE '22P02' OR  SQLSTATE '22P04' THEN
        /** Retryable errors:
                   55P03 (lock_not_available)
                23505 (duplicate key value violates unique constraint)
                22P02 (invalid input syntax for type json)
                22P04 (extra data after last expected column)
                38000 Lambda not available
        */
        EXECUTE format('UPDATE %1$s '
                           ||'set state = %2$L, '
                           ||'execution_count = execution_count +1, '
                           ||'data = data || ''{"error" : {"sqlstate":"%3$s"}}'' '
                           ||'WHERE s3_path = %4$L',
                       temporary_tbl,
                       'FAILED',
                       SQLSTATE,
                       s3_path);
END;
$BODY$;

/**
 * Enriches Feature - uses in plain trigger function
 */
CREATE OR REPLACE FUNCTION import_from_s3_trigger_for_empty_layer()
    RETURNS trigger
AS $BODY$
DECLARE
    author TEXT := TG_ARGV[0];
    curVersion BIGINT := TG_ARGV[1];
    target_table TEXT := TG_ARGV[2];
    retain_meta BOOLEAN := TG_ARGV[3]::BOOLEAN;
    feature RECORD;
    updated_rows INT;
BEGIN
    SELECT new_jsondata, new_geo, new_operation, new_id
        from import_from_s3_enrich_feature(NEW.jsondata::JSONB, NEW.geo, retain_meta)
    INTO feature;

    EXECUTE format('INSERT INTO "%1$s"."%2$s" (id, version, operation, author, jsondata, geo)
                        values(%3$L, %4$L, %5$L, %6$L, %7$L, %8$L);',
                   TG_TABLE_SCHEMA, target_table, feature.new_id, curVersion, feature.new_operation,
                   author, feature.new_jsondata, xyz_reduce_precision(feature.new_geo, false));

    NEW.jsondata = NULL;
    NEW.geo = NULL;
    NEW.count = 1;
    RETURN NEW;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE;
    
CREATE OR REPLACE FUNCTION import_from_s3_trigger_for_empty_layer_geojsonfc()
    RETURNS trigger
AS $BODY$
DECLARE
    author TEXT := TG_ARGV[0];
    curVersion BIGINT := TG_ARGV[1];
    target_table TEXT := TG_ARGV[2];
    retain_meta BOOLEAN := TG_ARGV[3]::BOOLEAN;
    elem JSONB;
    feature RECORD;
    updated_rows INT;
BEGIN

    --TODO: Should we also allow "Features"
    FOR elem IN SELECT * FROM jsonb_array_elements(((NEW.jsondata)::JSONB)->'features')
        LOOP
            IF NEW.geo IS NOT NULL THEN
                RAISE EXCEPTION 'Combination of FeatureCollection and WKB is not allowed!'
                    USING ERRCODE = 'XYZ40';
            END IF;

            SELECT new_jsondata, new_geo, new_operation, new_id
            from import_from_s3_enrich_feature(elem, null, retain_meta)
            INTO feature;

            EXECUTE format('INSERT INTO "%1$s"."%2$s" (id, version, operation, author, jsondata, geo)
                values(%3$L, %4$L, %5$L, %6$L, %7$L, %8$L )',
                           TG_TABLE_SCHEMA, target_table, feature.new_id, curVersion, feature.new_operation, author, feature.new_jsondata, xyz_reduce_precision(feature.new_geo, false));
        END LOOP;

    NEW.jsondata = NULL;
    NEW.geo = NULL;
    NEW.count = jsonb_array_length((NEW.jsondata)::JSONB->'features');
    RETURN NEW;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE;