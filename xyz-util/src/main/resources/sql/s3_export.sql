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
 * Performs Export to S3
 */
CREATE OR REPLACE FUNCTION export_to_s3_perform(content_query TEXT, s3_bucket TEXT, s3_path TEXT, s3_region TEXT)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    export_statistics RECORD;
    config RECORD;
    ctx JSONB;
BEGIN
    SELECT context() into ctx;

    SELECT * FROM s3_plugin_config('GEOJSON') INTO config;

    EXECUTE format(
            'SELECT * from aws_s3.query_export_to_s3( '
                ||' ''%1$s'', '
                ||' aws_commons.create_s3_uri(%2$L,%3$L,%4$L),'
                ||' %5$L )',
            format('select jsondata || jsonb_build_object(''''geometry'''', ST_AsGeoJSON(geo, 8)::jsonb) from (%1$s) X', REPLACE(content_query, $x$'$x$, $x$''$x$)),
            s3_bucket,
            s3_path,
            s3_region,
            REGEXP_REPLACE(config.plugin_options, '[\(\)]', '', 'g')
            )INTO export_statistics;

    -- Mark item as successfully imported. Store import_statistics.
    EXECUTE format('UPDATE %1$s '
                       ||'set state = %2$L, '
                       ||'execution_count = execution_count + 1, '
                       ||'data = data || %3$L '
                       ||'WHERE s3_path = %4$L ',
                   get_table_reference(ctx->>'schema', ctx->>'stepId' ,'JOB_TABLE'),
                   'FINISHED',
                    json_build_object('export_statistics', export_statistics),
                   s3_path);
END;
$BODY$;