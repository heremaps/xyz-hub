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
