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

CREATE OR REPLACE FUNCTION s3_plugin_config(format TEXT)
    RETURNS TABLE(plugin_columns TEXT, plugin_options TEXT)
    LANGUAGE 'plpgsql'
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
    ELSE
        RAISE EXCEPTION 'Format ''%'' not supported! ',format
            USING HINT = 'geojson | csv_geojson | csv_json_wkb are available',
                ERRCODE = 'XYZ40';
    END IF;

    RETURN NEXT;
END;
$BODY$;

/**
 * Get REGCLASS object for schema and table.
 */
CREATE OR REPLACE FUNCTION get_table_reference(schema TEXT, tbl TEXT, type TEXT = 'DEFAULT')
    RETURNS REGCLASS
    LANGUAGE 'plpgsql'
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
 *..
 */
CREATE OR REPLACE FUNCTION get_work_table_name(schema TEXT, step_id TEXT)
    RETURNS REGCLASS
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN (schema ||'.'|| '"job_data_' || step_id || '"')::REGCLASS;
END;
$BODY$;


/**
 * Get work_item. Used for synchronizing threads.
 */
CREATE OR REPLACE FUNCTION get_work_item(temporary_tbl REGCLASS)
    RETURNS JSONB
    LANGUAGE 'plpgsql'
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
                    (work_item -> 'filesize')::BIGINT);
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
        IF work_item ->> 'state' = 'RETRY' OR work_item ->> 'state' = 'LAST_ONES_RUNNING' THEN
            -- Received a RETRY
            PERFORM pg_sleep(10);
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
        content_query TEXT := '$wrappedouter$||(coalesce(content_query,''))||$wrappedouter$'::TEXT;
		retry_count INT := 2;
    BEGIN
			BEGIN
				PERFORM context(ctx);
			    IF (work_item -> 'execution_count')::INT >= retry_count THEN
			        --TODO: find a solution to read a given hint in the failure_callback. Remove than the duplication.
                    RAISE EXCEPTION 'Error on processing file ''%''. Maximum retries are reached %. Details: ''%''',
			                right(work_item ->>'s3_path', 36), retry_count, (work_item -> 'data' -> 'error' ->> 'sqlstate')
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

