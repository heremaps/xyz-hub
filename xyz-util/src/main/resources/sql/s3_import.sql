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
DO $$
BEGIN
	CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;
	CREATE EXTENSION IF NOT EXISTS postgis_topology;
	CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;
	CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;
    BEGIN
	        CREATE EXTENSION IF NOT EXISTS plpython3u CASCADE;
            EXCEPTION WHEN OTHERS THEN
	            RAISE NOTICE 'Not able to install plpython3u extension';
    END;
    CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;
    BEGIN
	        CREATE EXTENSION IF NOT EXISTS aws_lambda CASCADE;
            EXCEPTION WHEN OTHERS THEN
	            RAISE NOTICE 'Not able to install aws_lambda extension';
    END;

    /**
     *TODO: Find a solution to remove the search_path
     */
    SET search_path=s3_import,transport,public;
END;
$$;

/**
 * Enriches Feature - uses in plain trigger function
 */
CREATE OR REPLACE FUNCTION import_from_s3_enrich_feature(IN jsondata JSONB, geo geometry(GeometryZ,4326))
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
    LANGUAGE plpgsql IMMUTABLE;

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
    feature RECORD;
    updated_rows INT;
BEGIN
        SELECT new_jsondata, new_geo, new_operation, new_id
            from import_from_s3_enrich_feature(NEW.jsondata::JSONB, NEW.geo)
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

/**
 * Import Trigger for non-empty-layers. (Entity FeatureCollection)
 */
CREATE OR REPLACE FUNCTION import_from_s3_trigger_for_empty_layer_geojsonfc()
    RETURNS trigger
AS $BODY$
DECLARE
    author TEXT := TG_ARGV[0];
    curVersion BIGINT := TG_ARGV[1];
    target_table TEXT := TG_ARGV[2];
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
            from import_from_s3_enrich_feature(elem, null)
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

/**
 * Import Trigger for non-empty-layers. (Entity Feature)
 */
--TODO: Remove code-duplication of the following trigger functions!!
CREATE OR REPLACE FUNCTION import_from_s3_trigger_for_non_empty_layer()
    RETURNS trigger
AS $BODY$
DECLARE
    author TEXT := TG_ARGV[0];
    currentVersion BIGINT := TG_ARGV[1];
    isPartial BOOLEAN := TG_ARGV[2];
    onExists TEXT := TG_ARGV[3];
    onNotExists TEXT := TG_ARGV[4];
    onVersionConflict TEXT := TG_ARGV[5];
    onMergeConflict TEXT := TG_ARGV[6];
    historyEnabled BOOLEAN := TG_ARGV[7]::BOOLEAN;
    context TEXT := TG_ARGV[8];
    extendedTable TEXT := TG_ARGV[9];
    format TEXT := TG_ARGV[10];
    entityPerLine TEXT := TG_ARGV[11];
    target_table TEXT := TG_ARGV[12];
    featureCount INT := 0;
    updated_rows INT;
BEGIN
    --TODO: check how to use asyncify instead
    PERFORM context(
            jsonb_build_object('schema', TG_TABLE_SCHEMA,
                               'table', target_table,
                               'historyEnabled', historyEnabled,
                               'context', CASE WHEN context = 'null' THEN null ELSE context END,
                               'extendedTable', CASE WHEN extendedTable = 'null' THEN null ELSE extendedTable END
            )
    );

    IF format = 'CSV_JSON_WKB' AND NEW.geo IS NOT NULL THEN
        --TODO: Extend feature_writer with possibility to provide geometry
        NEW.jsondata := jsonb_set(NEW.jsondata::JSONB, '{geometry}', xyz_reduce_precision(ST_ASGeojson(ST_Force3D(NEW.geo))::JSONB, false));
        SELECT write_feature(NEW.jsondata::TEXT,
                      author,
                      onExists,
                      onNotExists,
                      onVersionConflict,
                      onMergeConflict,
                      isPartial,
                      currentVersion,
                      false
        )->'count' INTO featureCount;
    END IF;

    IF format = 'GEOJSON' OR  format = 'CSV_GEOJSON' THEN
        IF entityPerLine = 'Feature' THEN
            SELECT write_feature( NEW.jsondata,
                                   author,
                                   onExists,
                                   onNotExists,
                                   onVersionConflict,
                                   onMergeConflict,
                                   isPartial,
                                   currentVersion,
                                   false
                    )->'count' INTO featureCount;
        ELSE
            --TODO: Extend feature_writer with possibility to provide featureCollection
            SELECT write_features((NEW.jsondata::JSONB->'features')::TEXT,
                                   author,
                                   onExists,
                                   onNotExists,
                                   onVersionConflict,
                                   onMergeConflict,
                                   isPartial,
                                   currentVersion,
                                   false
                    )->'count' INTO featureCount;
        END IF;
    END IF;

    NEW.jsondata = NULL;
    NEW.geo = NULL;
    NEW.count = featureCount;

    RETURN NEW;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE;

/**
 * Perform single import from S3
 */
CREATE OR REPLACE FUNCTION import_from_s3_perform(schem TEXT, temporary_tbl REGCLASS ,target_tbl REGCLASS,
                                              s3_bucket TEXT, s3_path TEXT, s3_region TEXT, format TEXT, filesize BIGINT)
    RETURNS void
    LANGUAGE 'plpgsql'
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
