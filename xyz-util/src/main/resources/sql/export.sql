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
CREATE OR REPLACE FUNCTION export_to_s3_perform(content_query TEXT, s3_bucket TEXT, s3_path TEXT, s3_region TEXT, format TEXT, filesize BIGINT)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
export_statistics RECORD;
    config RECORD;
BEGIN
select * from s3_plugin_config(format) into config;

EXECUTE format(
        'SELECT aws_s3.query_export_to_s3( '
            ||' ''%1$s'', '
            ||' aws_commons.create_s3_uri(%2$L,%3$L,%4$L),'
            ||' NULL,'
            ||' %5$L )',

        format('select jsondata || jsonb_build_object(''''geometry'''', ST_AsGeoJSON(geo, 8)::jsonb) from (%1$s) X',content_query),
        s3_bucket,
        s3_path,
        s3_region,
        config.plugin_options
        --REGEXP_REPLACE(config.import_config, '[\(\)]', '', 'g')
        )INTO export_statistics;
END;
$BODY$;