--
-- Copyright (C) 2017-2020 HERE Europe B.V.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- SPDX-License-Identifier: Apache-2.0
-- License-Filename: LICENSE
--
-- SET search_path=xyz,h3,public,topology
-- CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;
-- CREATE EXTENSION IF NOT EXISTS postgis_topology;
-- CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;

-- DROP FUNCTION xyz_index_status();
-- DROP FUNCTION xyz_create_idxs_over_dblink(text, integer, integer, integer, text[], text, text, text,text, integer, text);
-- DROP FUNCTION xyz_space_bbox(text, text, integer);
-- DROP FUNCTION xyz_update_dummy_v5();
-- DROP FUNCTION xyz_index_check_comments(text, text);
-- DROP FUNCTION xyz_index_creation_on_property_object(text, text, text, text, text, character);
-- DROP FUNCTION xyz_maintain_idxs_for_space(text, text);
-- DROP FUNCTION xyz_create_idxs(text, integer, integer, integer, text[]);
-- DROP FUNCTION xyz_property_path_to_array(text);
-- DROP FUNCTION xyz_property_path(text);
-- DROP FUNCTION xyz_property_datatype(text, text, text, integer);
-- DROP FUNCTION xyz_property_statistic(text, text, integer);
-- DROP FUNCTION xyz_statistic_newest_spaces_changes(text, text[], integer);
-- DROP FUNCTION xyz_write_newest_idx_analyses(text);
-- DROP FUNCTION xyz_write_newest_statistics(text, text[], integer);
-- DROP FUNCTION xyz_statistic_all_spaces(text, text[], integer);
-- DROP FUNCTION xyz_property_evaluation(text, text, text, integer);
-- DROP FUNCTION xyz_index_proposals_on_properties(text, text);
-- DROP FUNCTION xyz_index_creation_on_property(text, text, text, character);
-- DROP FUNCTION xyz_geotype(geometry);
-- DROP FUNCTION xyz_index_name_for_property(text, text, character);
-- DROP FUNCTION xyz_index_list_all_available(text, text);
-- DROP FUNCTION xyz_index_name_dissolve_to_property(text,text);
-- DROP FUNCTION xyz_index_property_available(text, text, text);
-- DROP FUNCTION xyz_property_statistic_v2(text, text, integer);
-- DROP FUNCTION xyz_tag_statistic(text, text, integer);
-- DROP FUNCTION xyz_statistic_searchable(jsonb);
-- DROP FUNCTION xyz_statistic_xl_space(text, text, integer);
-- DROP FUNCTION xyz_statistic_space(text, text);
-- DROP FUNCTION xyz_statistic_xs_space(text, text);
-- DROP FUNCTION xyz_create_idxs_for_space(text, text);
-- DROP FUNCTION xyz_remove_unnecessary_idx(text, integer);
-- DROP FUNCTION xyz_index_dissolve_datatype(text);
-- DROP FUNCTION xyz_index_get_plain_propkey(text);
-- DROP FUNCTION IF EXISTS xyz_qk_point2lrc(geometry, integer);
-- DROP FUNCTION IF EXISTS xyz_qk_lrc2qk(integer,integer,integer);
-- DROP FUNCTION IF EXISTS xyz_qk_qk2lrc(text );
-- DROP FUNCTION IF EXISTS xyz_qk_lrc2bbox(integer,integer,integer);
-- DROP FUNCTION IF EXISTS xyz_qk_qk2bbox(text );
-- DROP FUNCTION IF EXISTS xyz_qk_point2qk(geometry,integer );
-- DROP FUNCTION IF EXISTS xyz_qk_bbox2zooml(geometry);
--
------ SAMPLE QUERIES ----
------ ENV: XYZ-CIT ; SPACE: QgQCHStH ; OWNER: psql
---------------------------------------------------------------------------------
-- xyz_index_status							:	select * from xyz_index_status();
-- xyz_create_idxs_over_dblink				:	select xyz_create_idxs_over_dblink('xyz', 20, 0, 2, ARRAY['postgres'], 'psql', 'xxx', 'xyz', 'localhost', 5432, 'xyz,h3,public,topology');
-- xyz_space_bbox							:	select * from xyz_space_bbox('xyz', 'QgQCHStH', 1000);
-- xyz_update_dummy_v5						:	select xyz_update_dummy_v5();
-- xyz_index_check_comments					:	select xyz_index_check_comments('xyz', 'QgQCHStH');
-- xyz_index_creation_on_property_object	:	select xyz_index_creation_on_property_object('xyz','QgQCHStH', 'feature_type', 'idx_QgQCHStH_a306a6c_a', 'number', 'a');
-- xyz_maintain_idxs_for_space				:	select xyz_maintain_idxs_for_space('xyz','QgQCHStH');
-- xyz_create_idxs  						:	select xyz_create_idxs_v2('public', 20, 0, 0, ARRAY['postgres']);
-- xyz_property_path_to_array				:	select * from xyz_property_path_to_array('foo.bar');
-- xyz_property_path						:	select * from xyz_property_path('foo.bar');
-- xyz_property_datatype					:	select * from xyz_property_datatype('xyz','QgQCHStH', 'feature_type', 1000);
-- xyz_property_statistic					:	select key,count,searchable from xyz_property_statistic('xyz', 'QgQCHStH', 1000);
-- xyz_statistic_newest_spaces_changes		:	select spaceid,tablesize,geometrytypes,properties,tags,count,bbox from xyz_statistic_newest_spaces_changes('xyz',ARRAY['psql'],8000);
-- xyz_write_newest_idx_analyses			:	select xyz_write_newest_idx_analyses('xyz');
-- xyz_write_newest_statistics				:	select xyz_write_newest_statistics('xyz', ARRAY['psql'], 10000);
-- xyz_statistic_all_spaces					:	select spaceid,tablesize,geometrytypes,properties,tags,count,bbox from xyz_statistic_all_spaces('xyz', ARRAY['psql'], 10000);
-- xyz_property_evaluation					:	select count,val,jtype from xyz_property_evaluation('xyz','QgQCHStH', 'fc', 1000);
-- xyz_index_proposals_on_properties		:	select prop_key,prop_type from xyz_index_proposals_on_properties('xyz','QgQCHStH');
-- xyz_index_creation_on_property			:	select xyz_index_creation_on_property('xyz','QgQCHStH', 'feature_type', 'a');
-- xyz_ext_version							:	select xyz_ext_version();
-- xyz_geotype								:	select * from xyz_geotype(ST_GeomFromText('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 42.25932)'));
-- xyz_index_name_for_property				:	select * from xyz_index_name_for_property('QgQCHStH', 'fc', 'm');
-- xyz_index_list_all_available				:	select idx_name, idx_property, src from xyz_index_list_all_available('xyz', 'QgQCHStH');
-- xyz_index_find_missing_system_indexes	:	select has_createdat,has_updatedat,spaceid,cnt from xyz_index_find_missing_system_indexes('xyz', ARRAY['psql']);
-- xyz_index_name_dissolve_to_property		:	select spaceid,propkey,source from xyz_index_name_dissolve_to_property('idx_QgQCHStH_a306a6c_m','QgQCHStH');
-- xyz_index_property_available				:	select * from xyz_index_property_available('xyz', 'QgQCHStH', 'feature_type');
-- xyz_property_statistic_v2				:	select key,count,searchable from xyz_property_statistic_v2('xyz', 'QgQCHStH', 1000);
-- xyz_tag_statistic						:	select key,count from xyz_tag_statistic('xyz', 'QgQCHStH', 1000);
-- xyz_statistic_searchable					:	select * from xyz_statistic_searchable('[{"searchable":true},{"searchable":false}]');
-- xyz_statistic_xl_space					:	select tablesize,geometrytypes,properties,tags,count,bbox,searchable from xyz_statistic_xl_space('xyz', 'QgQCHStH', 1000);
-- xyz_statistic_space						:	select tablesize,geometrytypes,properties,tags,count,bbox,searchable from xyz_statistic_space('xyz', 'QgQCHStH');
-- xyz_statistic_xs_space					:	select tablesize,geometrytypes,properties,tags,count,bbox,searchable from xyz_statistic_xs_space('xyz', 'QgQCHStH');
-- xyz_create_idxs_for_space				:	select xyz_create_idxs_for_space('xyz', 'QgQCHStH');
-- xyz_remove_unnecessary_idx				:	select xyz_remove_unnecessary_idx('xyz', 10000);
-- xyz_qk_grird								:	select xyz_qk_grird(3)
-- xyz_qk_child_calculation					:	select select * from xyz_qk_child_calculation('012',3,null)
-- xyz_count_estimation                     :   select xyz_count_estimation('select 1')
-- xyz_index_get_plain_propkey              :   select xyz_index_get_plain_propkey('foo.bar::string')
-- xyz_index_dissolve_datatype              :   select xyz_index_dissolve_datatype('foo.bar::array')
--
-- xyz_build_sortable_idx_values            :   select * from xyz_build_sortable_idx_values( '["pth1.pth2.field21:desc", "pth3.field22:asc", "field23:desc"]'::jsonb )
--
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
-- xyz_qk_point2lrc							:	select * from xyz_qk_point2lrc( ST_GeomFromText( 'POINT( -64.78767  32.29703)' ), 3 );
-- xyz_qk_lrc2qk							:	select xyz_qk_lrc2qk(3,2,3);
-- xyz_qk_qk2lrc							:	select xyz_qk_qk2lrc('032');
-- xyz_qk_lrc2bbox							:	select ST_ASText(xyz_qk_lrc2bbox(3,2,3));
-- xyz_qk_qk2bbox							:	select xyz_qk_qk2bbox( '001' );
-- xyz_qk_point2qk							:	select xyz_qk_point2qk(ST_GeomFromText( 'POINT( -64.78767  32.29703)' ), 3)
-- xyz_qk_bbox2zooml						:	select xyz_qk_bbox2zooml(
--													ST_GeomFromText('POLYGON((49.1430885846288 -122.003173828125,49.1430885846288 -122.001800537109,49.1439869452885
--													-122.001800537109,49.1439869452885 -122.003173828125,49.1430885846288 -122.003173828125))' ));
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
------ XYZ Index maintenance table	: xyz_config.xyz_idxs_status maintenance ----
---------------------------------------------------------------------------------
------ 	Field					: written through	:
---------------------------------------------------------------------------------
------ runts 					: xyz_write_newest_statistics()				: timestamp of last auto-index run
------ idx_creation_finished	: xyz_write_newest_statistics()				: true if all Indices are available (Auto+On-Demand Indexing). Get set to false if more than 3000
------ 							  xyz_maintain_idxs_for_space()				  rows changes in the table, or if new On-Demand Indices are getting created.
------ idx_proposals			: xyz_write_newest_idx_analyses()			: select * from xyz_index_proposals_on_properties('xyz','QgQCHStH');
------							  xyz_maintain_idxs_for_space()
------ idx_available			: xyz_write_newest_statistics()				: select * from xyz_index_list_all_available('xyz', 'QgQCHStH');
------							  xyz_maintain_idxs_for_space()
------							  xyz_maintain_idxs_for_space()
------							  xyz_index_check_comments()
------ spaceid					: xyz_write_newest_statistics()				: id of XYZ-space
------ count					: xyz_write_newest_statistics()				: row count of XYZ-space
------ prop_stat				: xyz_write_newest_statistics()				: select properties ->'value' from xyz_statistic_xl_space('xyz', 'QgQCHStH', 1000);
------ schem					: xyz_write_newest_statistics()				: schema in which the XYZ-Spaces are located
------ idx_manual				: xyz-psql-connector						: On-Demand Index configuration
------
----
-- select runts,idx_creation_finished,idx_proposals,idx_available,spaceid,count,prop_stat,schem,idx_manual
--		from xyz_config.xyz_idxs_status
--			where spaceid != 'idx_in_progess' order by count desc
------------------------------------------------------------------------------------------------
------------------------------------------------
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_ext_version()
  RETURNS integer AS
$BODY$
 select 142
$BODY$
  LANGUAGE sql IMMUTABLE;
------------------------------------------------
------------------------------------------------
-- ADD NEW COLUMN TO IDX_STATUS TABLE
DO $$
BEGIN
ALTER TABLE IF EXISTS xyz_config.xyz_idxs_status
    ADD COLUMN auto_indexing BOOLEAN;
EXCEPTION
	    WHEN duplicate_column THEN RAISE NOTICE 'column <auto_indexing> already exists in <xyz_idxs_status>.';
END;
$$;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_dissolve_datatype(text)
-- DROP FUNCTION xyz_index_dissolve_datatype(text);
CREATE OR REPLACE FUNCTION xyz_index_dissolve_datatype(propkey text)
  RETURNS text AS
$BODY$
	/**
	* Description: Get a specified datatype from a propkey.
	*
	*		prefix = foo.bar::array => array
	*
	* Parameters:
	*   		@propkey - path of json-key inside jsondata->'properties' object (eg. foo | foo.bar)
	*
	* Returns:
	*   datatype	- array / object / string / number / boolean
	*/
	DECLARE datatype TEXT;

	BEGIN
		IF (POSITION('::' in propkey) > 0) THEN
			datatype :=  lower(substring(propkey, position('::' in propkey)+2));
			IF datatype IN ('object','array','number','string','boolean') THEN
				RETURN datatype;
			END IF;
		END IF;
		RETURN NULL;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_statistic_history(text, text)
-- DROP FUNCTION xyz_statistic_history(text, text);
CREATE OR REPLACE FUNCTION xyz_statistic_history(
    IN schema text,
    IN spaceid text)
  RETURNS TABLE(tablesize jsonb, count jsonb, maxversion jsonb) AS
$BODY$
	/**
	* Description: Returns completet statisic about a big xyz-space. Therefor the results are including estimated values to reduce
	*		the runtime of the query.
	*
	* Parameters:
	*   @schema		- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the XYZ-space (tablename)
	*
	* Returns (table):
	*   tabelsize		- storage size of space
	*   count		- number of records found in space
	*/

	/**  Defines how much records a big table has */
	DECLARE big_space_threshold integer := 10000;

	/** used for big-spaces and get filled via pg_class */
	DECLARE estimate_cnt bigint;

	BEGIN
		IF substring(spaceid,length(spaceid)-3) != '_hst' THEN
			RETURN;
		END IF;

		SELECT reltuples into estimate_cnt FROM pg_class WHERE oid = concat('"',$1, '"."', $2, '"')::regclass;

		IF estimate_cnt > big_space_threshold THEN
			RETURN QUERY EXECUTE
			'SELECT	format(''{"value": %s, "estimated" : true}'', tablesize)::jsonb as tablesize,  '
			||'	format(''{"value": %s, "estimated" : true}'', count)::jsonb as count,  '
			||'	format(''{"value": %s, "estimated" : false}'', COALESCE(maxversion,0))::jsonb as maxversion  '
			||'	FROM ('
			||'		SELECT pg_total_relation_size('''||schema||'."'||spaceid||'"'') AS tablesize, '
			||'			(SELECT jsondata->''properties''->''@ns:com:here:xyz''->''version'' FROM "'||schema||'"."'||spaceid||'"'
			||'				order by jsondata->''properties''->''@ns:com:here:xyz''->''version'' DESC limit 1 )::TEXT::INTEGER as maxversion,'
			||'		       reltuples AS count '
			||'		FROM pg_class '
			||'	WHERE oid='''||schema||'."'||spaceid||'"''::regclass) A';
		ELSE
			RETURN QUERY EXECUTE
			'SELECT	format(''{"value": %s, "estimated" : true}'', tablesize)::jsonb as tablesize,  '
			||'	format(''{"value": %s, "estimated" : false}'', count)::jsonb as count,  '
			||'	format(''{"value": %s, "estimated" : false}'', COALESCE(maxversion,0))::jsonb as maxversion  '
			||'	FROM ('
			||'		SELECT pg_total_relation_size('''||schema||'."'||spaceid||'"'') AS tablesize, '
			||'			(SELECT jsondata->''properties''->''@ns:com:here:xyz''->''version'' FROM "'||schema||'"."'||spaceid||'"'
			||'				order by jsondata->''properties''->''@ns:com:here:xyz''->''version'' DESC limit 1 )::TEXT::INTEGER as maxversion,'
			||'		       (SELECT count(*) FROM "'||schema||'"."'||spaceid||'") AS count '
			||'		FROM pg_class '
			||'	WHERE oid='''||schema||'."'||spaceid||'"''::regclass) A';
		END IF;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_get_plain_propkey(text)
-- DROP FUNCTION xyz_index_get_plain_propkey(text);
CREATE OR REPLACE FUNCTION xyz_index_get_plain_propkey(propkey text)
  RETURNS text AS
$BODY$
	/**
	* Description: Get the plain propkey.
	*
	*		prefix = foo.bar::array => foo.bar
	*
	* Parameters:
	*   		@propkey - path of json-key inside jsondata->'properties' object (eg. foo | foo.bar)
	*
	* Returns:
	*   propkey	- json-key without datatype
	*/
	DECLARE datatype TEXT;

	BEGIN
		IF (POSITION('::' in propkey) > 0) THEN
			datatype :=  lower(substring(propkey, position('::' in propkey)+2));
			IF datatype IN ('object','array','number','string','boolean') THEN
				return substring(propkey, 0, position('::' in propkey));
			END IF;
		END IF;
		RETURN propkey;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_trigger_historywriter_versioned()
-- DROP FUNCTION xyz_trigger_historywriter_versioned();
CREATE OR REPLACE FUNCTION xyz_trigger_historywriter_versioned()
  RETURNS trigger AS
$BODY$
	DECLARE v text := (NEW.jsondata->'properties'->'@ns:com:here:xyz'->>'version');

	BEGIN
		IF TG_OP = 'INSERT' THEN
			EXECUTE
				format('INSERT INTO'
					||' %s."%s_hst" (uuid,jsondata,geo,vid)'
					||' VALUES( %L,%L,%L, %L)',TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.jsondata->'properties'->'@ns:com:here:xyz'->>'uuid', NEW.jsondata, NEW.geo,
						substring('0000000000'::text, 0, 10 - length(v)) ||
						v || '_' || (NEW.jsondata->>'id'));
			RETURN NEW;
		END IF;

		IF TG_OP = 'UPDATE' THEN
			EXECUTE
				format('INSERT INTO'
					||' %s."%s_hst" (uuid,jsondata,geo,vid)'
					||' VALUES( %L,%L,%L, %L)',TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.jsondata->'properties'->'@ns:com:here:xyz'->>'uuid', NEW.jsondata, NEW.geo,
						substring('0000000000'::text, 0, 10 - length(v)) ||
						v || '_' || (NEW.jsondata->>'id'));

			IF NEW.jsondata->'properties'->'@ns:com:here:xyz'->'deleted' IS NOT null AND NEW.jsondata->'properties'->'@ns:com:here:xyz'->'deleted' = 'true'::jsonb THEN
				EXECUTE
					format('DELETE FROM %s."%s" WHERE jsondata->>''id'' = %L',TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.jsondata->>'id');
			END IF;

			RETURN NEW;
		END IF;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_trigger_historywriter_full()
  RETURNS trigger AS
$BODY$
	DECLARE oldest_uuids text[];
	DECLARE max_version_cnt integer := COALESCE(TG_ARGV[0]::NUMERIC::INTEGER,10);
	DECLARE max_version_diff integer;
	DECLARE uuid_deletes text[];

	BEGIN
		IF TG_OP = 'INSERT' THEN
			EXECUTE
				format('INSERT INTO'
					||' %s."%s_hst" (uuid,jsondata,geo)'
					||' VALUES( %L,%L,%L)',TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.jsondata->'properties'->'@ns:com:here:xyz'->>'uuid', NEW.jsondata, NEW.geo);
			RETURN NEW;
		END IF;

		IF max_version_cnt != -1 THEN
			--IF MORE THAN max_version_cnt ARE EXISTING DELETE OLDEST ENTRIES
			EXECUTE
				format('SELECT array_agg(uuid)'
					|| 'FROM( '
					|| '	select uuid FROM %s."%s_hst" '
					|| '		WHERE jsondata->>''id'' = %L ORDER BY jsondata->''properties''->''@ns:com:here:xyz''->''updatedAt'' ASC'
					|| ') A',TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.jsondata->>'id'
				) into oldest_uuids;

			max_version_diff := array_length(oldest_uuids,1) - max_version_cnt;

			IF max_version_diff >= 0 THEN
				-- DELETE OLDEST ENTRIES
				FOR i IN 1..max_version_diff+1 LOOP
					select array_append(uuid_deletes, oldest_uuids[i])
						INTO uuid_deletes;
				END LOOP;
				EXECUTE
					format('DELETE FROM %s."%s_hst" WHERE uuid = ANY(%L)',TG_TABLE_SCHEMA, TG_TABLE_NAME, uuid_deletes);
			END IF;
		END IF;

		IF TG_OP = 'UPDATE' THEN
			EXECUTE
				format('INSERT INTO'
					||' %s."%s_hst" (uuid,jsondata,geo)'
					||' VALUES( %L,%L,%L)',TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.jsondata->'properties'->'@ns:com:here:xyz'->>'uuid', NEW.jsondata, NEW.geo);
			RETURN NEW;

		ELSEIF TG_OP = 'DELETE' THEN
			EXECUTE
				format('INSERT INTO'
					||' %s."%s_hst" (uuid,jsondata,geo)'
					||' VALUES( %L,%L,%L)',TG_TABLE_SCHEMA, TG_TABLE_NAME,
					OLD.jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' || '_deleted',
					jsonb_set(OLD.jsondata,'{properties,@ns:com:here:xyz}', ('{"deleted":true}'::jsonb  || (OLD.jsondata->'properties'->'@ns:com:here:xyz')::jsonb)),
					OLD.geo);
			RETURN OLD;
		END IF;
	END;
$BODY$
language plpgsql;

------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_trigger_historywriter()
  RETURNS trigger AS
$BODY$
	DECLARE path text[];
	DECLARE max_version_cnt integer := COALESCE(TG_ARGV[0]::NUMERIC::INTEGER,10);
	DECLARE max_version_diff integer;
	DECLARE uuid_deletes text[];

	BEGIN
        IF max_version_cnt != -1 THEN
            --IF MORE THAN max_version_cnt ARE EXISTING DELETE OLDEST ENTRIES
            EXECUTE
                format('SELECT array_agg(uuid)'
                    || 'FROM( '
                    || '	select uuid FROM %s."%s_hst" '
                    || '		WHERE jsondata->>''id'' = %L ORDER BY jsondata->''properties''->''@ns:com:here:xyz''->''updatedAt'' ASC'
                    || ') A',TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.jsondata->>'id'
                ) into path;

            max_version_diff := array_length(path,1) - max_version_cnt;

            IF max_version_diff >= 0 THEN
                -- DELETE OLDEST ENTRIES
                FOR i IN 1..max_version_diff+1 LOOP
                    select array_append(uuid_deletes, path[i])
                        INTO uuid_deletes;
                END LOOP;
                EXECUTE
                    format('DELETE FROM %s."%s_hst" WHERE uuid = ANY(%L)',TG_TABLE_SCHEMA, TG_TABLE_NAME, uuid_deletes);
            END IF;
        END IF;

		IF TG_OP = 'UPDATE' THEN
			EXECUTE
				format('INSERT INTO'
					||' %s."%s_hst" (uuid,jsondata,geo)'
					||' VALUES( %L,%L,%L)',TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.jsondata->'properties'->'@ns:com:here:xyz'->>'uuid', OLD.jsondata, OLD.geo);
			RETURN NEW;
		ELSEIF TG_OP = 'DELETE' THEN
			EXECUTE
				format('INSERT INTO'
					||' %s."%s_hst" (uuid,jsondata,geo)'
                    ||' VALUES( %L,%L,%L)',TG_TABLE_SCHEMA, TG_TABLE_NAME,
						OLD.jsondata->'properties'->'@ns:com:here:xyz'->>'uuid',
						jsonb_set(OLD.jsondata,'{properties,@ns:com:here:xyz}', ('{"deleted":true}'::jsonb  || (OLD.jsondata->'properties'->'@ns:com:here:xyz')::jsonb)),
						OLD.geo);
			RETURN OLD;
		END IF;
	END;
$BODY$
language plpgsql;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_count_estimation(query text)
    RETURNS integer AS
$BODY$
DECLARE
    rec   record;
    rows  integer;
BEGIN
    FOR rec IN EXECUTE 'EXPLAIN ' || query LOOP
            rows := substring(rec."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
            EXIT WHEN rows IS NOT NULL;
        END LOOP;
    --IF ROWS <= 1 THEN
    --    RETURN null;
    --END IF;
    RETURN rows;
END;
$BODY$
    LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_child_calculation(quadkey text,resolution integer, result text[])
 RETURNS TEXT[] AS
$BODY$
DECLARE
	i integer := 0;
BEGIN
	IF resolution = 0 THEN
		select into result array_append(result, quadkey);
		return result;
	END IF;

	resolution := resolution-1;
	LOOP
		select into result xyz_qk_child_calculation(concat(quadkey,i),resolution,result);

		EXIT WHEN i = 3;
		i := i + 1;
	END LOOP;
	RETURN result;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_status()
-- DROP FUNCTION xyz_index_status();
CREATE OR REPLACE FUNCTION xyz_index_status()
  RETURNS INTEGER AS
$BODY$
	/**
	* Description: Analyzes through pg_stat_activity table if an Index relevant process is running.
	*	Thereto are counting: xyz_write_newest_statistics(..), xyz_write_newest_idx_analyses(..), xyz_create_idxs_over_dblink
	*
	* Returns:
	*	Integer	- process detection via bitmask
	*			((status  &  (1<<0)) == (1<<0) = statistics creation is running
	*			((status  &  (1<<1)) == (1<<1) = analyze process is running
	*			((status  &  (1<<2)) == (1<<2) = index creation is running
	*			((status  &  (1<<3)) == (1<<3) = idx_semaphore=16 (disable indexing completely)
	*			((status  &  (1<<4)) == (1<<4) = idx_semaphore=32 (disable auto-indexer)
	*/

	DECLARE
		/** if status is 0 no IDX relevant processing is running */
		status INTEGER;
	BEGIN
		/** All psql-connector health-checks are arriving at the same time. We do not want to execute all status queries in parallel */
		PERFORM setseed((extract(epoch from now()) -floor(extract(epoch from now()))) * (pg_backend_pid()%100)*0.01);
		PERFORM pg_sleep((random() * (4-1+1) + 1)/10.0::numeric);

		/**
		* If count is set to 16, whole indexing (auto / on-demand) is deactivated
		*/
		SELECT count into status
			from xyz_config.xyz_idxs_status
				WHERE spaceid='idx_in_progress';

		IF status = 16 THEN
			RETURN status;
		END IF;

		SELECT  (COALESCE(bit_or(statitics_running), 0::bit )
				|| COALESCE(bit_or(analyses_running), 0::bit )
				|| COALESCE(bit_or(idx_running), 0::bit ))::bit(3)::integer into status
			 from(
			SELECT
			  (CASE WHEN (position('xyz_write_newest_statistics' IN query) > 0) THEN 1 ELSE 0 END)::bit as statitics_running ,
			  (CASE WHEN(position('xyz_write_newest_idx_analyses' IN query) > 0) THEN 1 ELSE 0 END)::bit as analyses_running ,
			  (CASE WHEN (position('xyz_create_idxs' IN query) > 0) THEN 1 ELSE 0 END)::bit as idx_running,
			  pid,
			  now() - pg_stat_activity.query_start AS duration,
			  query,
			  state
		FROM pg_stat_activity
		)A   where ((statitics_running||analyses_running||idx_running) not in (111::bit(3),000::bit(3)))
			AND state='active';

		RETURN status;
	END
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_create_idxs_over_dblink(text, integer, integer, integer, text[], text, text, text, text, integer, text)
-- DROP FUNCTION xyz_create_idxs_over_dblink(text, integer, integer, integer, text[], text, text, text, text, integer, text);
CREATE OR REPLACE FUNCTION xyz_create_idxs_over_dblink(
	schema text,
	lim integer,
	off integer,
	mode integer,
    owner_list text[],
	usr text,
	pwd text,
	dbname text,
	host text,
	port integer,
	searchp text)
		RETURNS void AS
	$BODY$
		/**
		* Description: Use dblink (requires installed extension) to trigger the index creation, to guaranty that the connection does not get interrupted through Lambda termination.
		*
		* Parameters:
		*   @schema	- schema in which the xyz-spaces are located
		*   @lim 	- max amount of spaces to iterate over
		*   @off 	- offset, required for parallel executions
		*   @mode 	- 0 = only indexing, 1 = statistics+indexing, 2 = statistic, analyzing, indexing (auto-indexing)
		*   @owner_list	- list of database users which has the tables created (owner). Normally is this only one user.
		*   @usr 	- database user
		*   @pwd	- database user password
		*   @dbname	- database name
		*   @port	- database port
		*   @searchp	- searchpath
		*/

		DECLARE
			v_conn_str  text := 'port='||port||' dbname='||dbname||' host='||host||' user='||usr||' password='||pwd||' options=-csearch_path='||searchp||'';
			v_query     text;
		BEGIN
			v_query := 'select xyz_create_idxs('''||schema||''',100, 0, '||mode||', '''||owner_list::text||''')';
			/** Requires the installed dblink extension - we use dblink to avoid connection interruption through Lambda termination */
			PERFORM * FROM dblink(v_conn_str, v_query) AS t1(test text);
		END;
	$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_space_bbox(text, text, integer)
-- DROP FUNCTION xyz_space_bbox(text, text, integer);
CREATE OR REPLACE FUNCTION xyz_space_bbox(
    schema text,
    space_id text,
   tablesamplecnt integer )
  RETURNS box2d AS
$BODY$
	/**
	* Description:  Covers the overall BBOX of the geometries within a space. If a tablesample value is provided only
	* 	a sample of the data get analyzed.
	*
	* Parameters:
	*   @schema			- schema in which the xyz-spaces are located
	*   @spaceid		- id of XYZ-space(tablename)
	*   @tablesamplecnt	- define the value which get further used in the tablesample statement. If it is null full table scans will be performed.
	*
	* Returns:
	*	@bboxx 			- overall BBOX of the geometries within a space
	*/

	DECLARE
		bboxx box2d;
	BEGIN

		IF tablesamplecnt IS NULL THEN
			EXECUTE
				format( 'select ST_Extent(geo) '
					||' FROM "'||schema||'"."'||space_id||'" '
				) INTO bboxx;
			RETURN bboxx;
		ELSE
			EXECUTE
				format( 'select ST_EstimatedExtent('''||schema||''','''||space_id||''', ''geo'')') INTO bboxx;
			IF bboxx IS NOT NULL THEN
				RETURN bboxx;
			END IF;
		END IF;

		/** IF we are here we cant get information via ST_EstimatedExtent so we are using a sample of the data */
		EXECUTE
			format( 'select ST_Extent(geo) '
				||' FROM '||schema||'."'||space_id||'" TABLESAMPLE SYSTEM_ROWS('||tablesamplecnt||') '
			) INTO bboxx;

		IF bboxx IS NULL THEN
			RETURN 'BOX(-180 -90, 180 90)'::box2d;
		END IF;

		RETURN bboxx;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_list_all_available(text, text)
-- DROP FUNCTION xyz_index_list_all_available(text, text);
CREATE OR REPLACE FUNCTION xyz_index_list_all_available(
    IN schema text,
    IN spaceid text)
  RETURNS TABLE(idx_name text, idx_property text, src character) AS
$BODY$
	/**
	* Description: This function return all properties which are indexed.
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of XYZ-space (tablename)
	*
	* Returns (table):
	*	idx_name		- Index-name
	*	idx_property	- Property name on which the Index is based on
	*	src				- source (a - automatic ; m - manual; s - system/unknown)
	*/

	DECLARE
		/** blacklist of XYZ-System indexes */
		ignore_idx text := 'NOT IN(''id'',''tags'',''geo'',''serial'')';
		av_idx_list record;
		comment_prefix text:='p.name=';
	BEGIN
		FOR av_idx_list IN
			   SELECT indexname, substring(indexname from char_length(spaceid) + 6) as idx_on, COALESCE((SELECT source from xyz_index_name_dissolve_to_property(indexname,spaceid)),'s') as source
				FROM pg_indexes
					WHERE
				schemaname = ''||schema||'' AND tablename = ''||spaceid||''
		LOOP
			src := av_idx_list.source;
			idx_name := av_idx_list.indexname;

			BEGIN
				/** Check if comment with the property-name is present */
				select * into idx_property
					from obj_description( (concat('"',av_idx_list.indexname,'"')::regclass ));

				EXCEPTION WHEN OTHERS THEN
					/** do nothing - This Index is not a property index! */
			END;

			IF idx_property IS NOT null THEN
				IF (position(''||comment_prefix||'' in ''||idx_property||'')) != 0 THEN
					/** we found the name of the property in the comment */
					idx_property := substring(idx_property, char_length(''||comment_prefix||'')+1);
				END IF;
			ELSE
				idx_property := av_idx_list.idx_on;
			END IF;

			RETURN NEXT;
		END LOOP;
	END
$BODY$
  LANGUAGE plpgsql VOLATILE;
 ------------------------------------------------
------------------------------------------------
-- Function: xyz_index_check_comments(text, text)
-- DROP FUNCTION xyz_index_check_comments(text, text);
CREATE OR REPLACE FUNCTION xyz_index_check_comments(
    schema text,
    space_id text)
  RETURNS void AS
$BODY$
	/**
	* Description: In some rare cases Comments are getting not written onto created Indices. For this we have that function
	*	place. It re-creates the missing comment by analyzing the Index-Definition. Comments are required to
	*	conclude which property inside the jsonb is indexed.
	*
	* Parameters:
	*   @schema			- schema in which the xyz-spaces are located
	*   @spaceid		- id of XYZ-space(tablename)
	*/

	DECLARE
		missing_idx_list record;
		jsonpath TEXT[];
		comment_text TEXT;
	BEGIN
		FOR missing_idx_list IN
			/** check which comments are missing */
			SELECT indexname,
				indexdef,
				(SELECT obj_description(format('%s."%s"',schema, indexname)::regclass))
					from pg_indexes
				WHERE 1=1
					AND schemaname = schema
					AND substring(indexname from char_length(indexname)-1) IN ('_a','_m')
					AND (select obj_description(format('%s."%s"',schema,indexname)::regclass)) IS NULL
					AND position(format('idx_%s',space_id) in indexname) > 0
		LOOP
			/** get Property path out from index definition */
			SELECT
				(
					select array_agg( ii.regexp_replace )
						from (
							select regexp_replace( (regexp_matches( indexdef, '\s*->+\s*\''[^'']*','g'))[1], '\s*->+\s*\''', '' )
						) ii
				)as jsonpath INTO jsonpath from pg_indexes
					where 1 = 1
					and schemaname = schema
					and indexname = missing_idx_list.indexname
					and strpos( indexdef, 'jsondata' ) > 0;

			/** generate comment out from Property path */
			SELECT concat('p.name=',
				array_to_string(
					array_remove(
						jsonpath,
						jsonpath[1]
					)
				,'.','*')
			) INTO comment_text;


			/** Add missing Comment */
			IF comment_text IS NOT NULL THEN
				RAISE NOTICE 'Add comment % to %',comment_text,space_id;

				EXECUTE format('COMMENT ON INDEX %s."%s" IS ''%s''', schema, missing_idx_list.indexname, comment_text);

				UPDATE xyz_config.xyz_idxs_status
					SET idx_available = (select jsonb_agg(FORMAT('{"property":"%s","src":"%s"}',idx_property, src)::jsonb) from (
						select * from xyz_index_list_all_available(schema,space_id)
							order by idx_property
					)b )
				WHERE space_id = spaceid;
			END IF;
		END LOOP;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_creation_on_property_object(text, text, text, text, text, character);
-- DROP FUNCTION xyz_index_creation_on_property_object(text, text, text, text, text, character);
CREATE OR REPLACE FUNCTION xyz_index_creation_on_property_object(
    schema text,
    spaceid text,
    propkey text,
    idx_name text,
    datatype text,
    source character)
  RETURNS text AS
$BODY$
	/**
	* Description: This function creates an index for a properties.key in a XYZ-space.
	*	The name of the indexed property get stored as an comment on the created index.
	*	Get comment with: select obj_description( ('xyz."idx_name"')::regclass )
	*	The index get stored as JSONB Type by use of BTREE or GIN.
	*	Its recommended to SET ENABLE_SEQSCAN = OFF;
	*
	* Parameters:
	*   @schema		- schema in which the XYZ-spaces are located
	*   @spaceid	- id of the XYZ-space (tablename)
	*   @propkey	- name of the property for a index should get created
	*   @idx_name	- name of the index (use xyz_index_name_for_property())
	*   @datatype	- data type of the property (use xyz_property_datatype())
	*   @source		- source = 'm' | 'a' <=> manual | automatic
	*
	* Returns:
	*	@idx_name	- name of the created Index
	*/

	DECLARE
        root_path text := '''properties''->';
		prop_path text;
		idx_type text := 'btree';
	BEGIN
		source = lower(source);
		prop_path := '''' || replace( regexp_replace( xyz_index_get_plain_propkey(propkey),'^f\.',''),'.','''->''') || ''''; 

        /** root level property detected */
        IF (lower(SUBSTRING(propkey from 0 for 3)) = 'f.') THEN
            root_path:='';
        END IF;

        IF source not in ('a','m') THEN
            RAISE NOTICE 'Source ''%'' not supported. Use ''m'' for manual or ''a'' for automatic!',source;
        END IF;

		/** In all other cases we are using btree */
		IF datatype = 'array' THEN
			idx_type = 'GIN';
		END IF;

        IF propkey = 'f.geometry.type' THEN
            /** special handling for geometryType */
            EXECUTE format('CREATE INDEX "%s" '
                ||'ON %s."%s" '
                ||' USING btree '
                ||' (GeometryType(geo))', idx_name, schema, spaceid, idx_type);
        ELSE
            EXECUTE format('CREATE INDEX "%s" '
                ||'ON %s."%s" '
                ||' USING %s '
                ||'((jsondata->%s %s))', idx_name, schema, spaceid, idx_type, root_path, prop_path);
        END IF;

		EXECUTE format('COMMENT ON INDEX %s."%s" '
				||'IS ''p.name=%s''',
			schema, idx_name, xyz_index_get_plain_propkey(propkey));

		RETURN idx_name;
	END
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
--- Begin : sortable indexes
------------------------------------------------

create or replace function xyz_build_sortable_idx_values( sortby_arr jsonb, out iname text, out icomment text, out ifields text )
as
$body$
declare
 selem record;
 dflip boolean := false;
 direction text := '';
 idx_postfix text := '';
 comma text := '';
 pathname text := '';
 fullpathname text := '';
 jpth text := '';
 jseg text := '';
begin

 icomment = '';
 ifields = '';

 for selem in
  select row_number() over () as pos, el::text as sentry, el::text ~* '^".+:desc"$' as isdesc from jsonb_array_elements( sortby_arr ) el
 loop

  if selem.pos = 1 and selem.isdesc then
	 dflip = true;
	end if;

	if selem.isdesc != dflip then
	 direction = 'desc';
	 idx_postfix = idx_postfix || '0';
	else
	 direction = '';
	 idx_postfix = idx_postfix || '1';
	end if;

	if length( icomment ) > 0 then
	 comma = ',';
	end if;

	pathname = regexp_replace(selem.sentry, '^"([^:]+)(:(asc|desc))*"$','\1','i');

	if pathname ~ '^f\.(createdAt|updatedAt)' then
	 fullpathname = regexp_replace(pathname,'^f\.','properties.@ns:com:here:xyz.');
	elsif pathname ~ '^f\.' then
	 fullpathname = regexp_replace(pathname,'^f\.','');
	else
	 fullpathname = 'properties.' || pathname;
	end if;

	jpth = 'jsondata';
	foreach jseg in array regexp_split_to_array(fullpathname,'\.')
	loop
	 jpth = format('(%s->''%s'')',jpth, jseg );
	end loop;

	ifields = ifields || format('%s %s,',jpth,direction);

	icomment = icomment || format('%s%s%s',comma, pathname , replace(direction,'desc',':desc'));

 end loop;

 ifields = format('%s (jsondata->>''id'') %s', ifields, direction );
 iname = format('idx_#spaceid#_%s_o%s', substr( md5( icomment ),1,7), idx_postfix );

end;
$body$
language plpgsql immutable;


create or replace function xyz_eval_o_idxs( schema text, space text )
 returns table ( iexists text, iproperty text, src character, iname text, icomment text, ifields text ) as
$body$
 with
  indata as ( select xyz_eval_o_idxs.schema as schema, xyz_eval_o_idxs.space as space ),
  availidx as ( select idx_name as iexists, idx_property as iproperty, src  from ( select (xyz_index_list_all_available( i.schema, i.space )).* from indata i ) r where src = 'o' ),
  reqidx as ( select distinct on (iname) replace(iname,'#spaceid#', o.space ) as iname, icomment, ifields
			  from
			  ( select i.space, (xyz_build_sortable_idx_values( jsonb_array_elements( nullif( s.idx_manual->'sortableProperties', 'null' ) ) )).*
                from xyz_config.xyz_idxs_status s, indata i
                where s.idx_creation_finished = false
                and s.spaceid = i.space
			  ) o
 		    )
 select e.iexists, e.iproperty, e.src, r.iname, r.icomment, r.ifields
 from availidx e full join reqidx r on ( e.iexists = r.iname )
 where 1 = 1
   and ((e.iexists is null) or (r.iname is null))
 order by e.iexists, r.iname
$body$
language sql immutable;

-- Function: xyz_maintain_o_idxs_for_space(text, text)
-- DROP FUNCTION xyz_maintain_o_idxs_for_space(text, text);
create or replace function xyz_maintain_o_idxs_for_space( schema text, space text)
  returns void as
$body$
 declare
  indata record;
 begin
  for indata in
	 select * from xyz_eval_o_idxs( schema, space )
  loop

   if indata.iexists is not null then

    raise notice '-- PROPERTY: % | SORTABLE | SPACE: % |> DELETE IDX: %!', indata.iproperty, space, indata.iexists;
	execute format('drop index if exists %s."%s" ', schema, indata.iexists );

   elsif indata.iname is not null then

	raise notice '-- PROPERTY: % | SORTABLE | SPACE: % |> CREATE SORT IDX: %!', indata.icomment, space, indata.iname;
    execute format('create index "%s" on %s."%s" using btree (%s) ', indata.iname, schema, space, indata.ifields );
    execute format('comment on index %s."%s" is ''%s''', schema, indata.iname, indata.icomment );

   end if;

  end loop;
 end;
$body$
language plpgsql volatile;

------------------------------------------------
--- End : sortable indexes
------------------------------------------------

------------------------------------------------
------------------------------------------------
-- Function: xyz_maintain_idxs_for_space(text, text)
-- DROP FUNCTION xyz_maintain_idxs_for_space(text, text);
CREATE OR REPLACE FUNCTION xyz_maintain_idxs_for_space(
    schema text,
    space text)
  RETURNS void AS
$BODY$
	/**
	* Description: This function creates all Indices for a given space. Therefore it uses the data which
	*	is stored in the xyz_config.xyz_idxs_status maintenance table.
	*	At first all required On-Demand Indices are getting created (if some exists). Afterwards
	*	the properties which are determined through the Auto-Indexer are getting indexed (if some could
	*	get found during the data-analysis.
	*
	* Parameters:
	*   @schema	- schema in which the XYZ-spaces are located
	*   @space - id of the XYZ-space (tablename)
	*/
    DECLARE xyz_space_exists record;

	DECLARE xyz_space_stat record;
	DECLARE xyz_manual_idx record;
	DECLARE xyz_needless_manual_idx record;
    DECLARE xyz_needless_auto_idx record;

	DECLARE xyz_idx_proposal record;
	DECLARE idx_false_list TEXT[];
    DECLARE is_auto_indexing boolean;

	BEGIN
		/** Check if table is present */
		select 1 into xyz_space_exists
			from pg_tables WHERE tablename =space and schemaname = schema;
		IF xyz_space_exists IS NULL THEN
			DELETE FROM xyz_config.xyz_idxs_status
				WHERE spaceid = space
					AND schem = schema
                    AND idx_manual IS NULL
                    AND auto_indexing IS NULL;
			RAISE NOTICE 'SPACE DOES NOT EXIST %."%" ', schema, space;
			RETURN;
		END IF;

		/** set indication that idx creation is running */
		UPDATE xyz_config.xyz_idxs_status
			SET idx_creation_finished = false
				WHERE spaceid = space
                  AND schem = schema;

		EXECUTE xyz_index_check_comments(schema, space);

		/** Analyze IDX-ON DEMAND aka MANUAL MODE */
		RAISE NOTICE 'ANALYSE MANUAL IDX on SPACE: %', space;

		/** Search missing ON-DEMAND IDXs */
		FOR xyz_manual_idx IN
			SELECT *,
				(SELECT xyz_index_name_for_property(space, property, 'm')) as idx_name,
				(SELECT xyz_property_datatype(schema, space, property, 5000)) as datatype,
				(SELECT * from xyz_index_property_available(schema, space, property)) as idx_available
					from (
				SELECT
				    (jsonb_each( case when idx_manual ? 'searchableProperties' then nullif( idx_manual->'searchableProperties', 'null' ) else idx_manual end )).key as property,
                    (jsonb_each( case when idx_manual ? 'searchableProperties' then nullif( idx_manual->'searchableProperties', 'null' ) else idx_manual end )).value::text::boolean as idx_required
						FROM xyz_config.xyz_idxs_status
					WHERE idx_creation_finished = false
						AND spaceid = space
                        AND schem = schema
			) A
		LOOP
			IF xyz_manual_idx.idx_required = false THEN
				/** Add property to blacklist */
				idx_false_list :=  array_append(idx_false_list,xyz_manual_idx.property);
			END IF;

			IF xyz_manual_idx.idx_required = true AND xyz_manual_idx.idx_available = false THEN
				RAISE NOTICE '-- PROPERTY: % | TYPE: % | SPACE: % |> CREATE MANUAL IDX: %!',xyz_manual_idx.property, xyz_manual_idx.datatype, space,  xyz_manual_idx.idx_name;
				BEGIN
					PERFORM xyz_index_creation_on_property_object(schema, space, xyz_manual_idx.property, xyz_manual_idx.idx_name, xyz_manual_idx.datatype, 'm');
					EXCEPTION WHEN OTHERS THEN
						RAISE NOTICE '-- PROPERTY: % | TYPE: % | SPACE: % |> ALREADY EXISTS: % - SKIP!', xyz_manual_idx.property, xyz_manual_idx.datatype, space, xyz_manual_idx.idx_name;
				END;
			ELSEIF xyz_manual_idx.idx_required = false AND xyz_manual_idx.idx_available = true THEN
				RAISE NOTICE '-- PROPERTY: % | TYPE: % | SPACE: % |> DELETE IDX: %!',xyz_manual_idx.property, xyz_manual_idx.datatype, space, xyz_manual_idx.idx_name;
				EXECUTE FORMAT ('DROP INDEX IF EXISTS %s."%s" ', schema, xyz_manual_idx.idx_name);
				/** If an automatic one exists delete it as well */
				EXECUTE FORMAT ('DROP INDEX IF EXISTS %s."%s" ', schema, xyz_index_name_for_property(space, xyz_manual_idx.property, 'a'));
			ELSE
				RAISE NOTICE '-- PROPERTY: % | TYPE: % | SPACE: % |> Nothing to do: %!',xyz_manual_idx.property, xyz_manual_idx.datatype, space, xyz_manual_idx.idx_name;
			END IF;
		END LOOP;

		/** Search created ON-DEMAND IDXs which are no longer getting used */
		FOR xyz_needless_manual_idx IN
			SELECT idx_name, idx_property
				FROM xyz_index_list_all_available(schema,space)
					WHERE src='m'
			EXCEPT
			SELECT idx_name, idx_property FROM(
				SELECT  xyz_index_get_plain_propkey((jsonb_each( case when idx_manual ? 'searchableProperties' then nullif( idx_manual->'searchableProperties', 'null' ) else idx_manual end )).key) as idx_property,
					xyz_index_name_for_property(space, (jsonb_each( case when idx_manual ? 'searchableProperties' then nullif( idx_manual->'searchableProperties', 'null' ) else idx_manual end )).key, 'm') as idx_name,
					(jsonb_each( case when idx_manual ? 'searchableProperties' then nullif( idx_manual->'searchableProperties', 'null' ) else idx_manual end )).value::text::boolean as idx_required
					FROM xyz_config.xyz_idxs_status
						where spaceid = space
                          AND schem = schema
			) A WHERE idx_required = true
		LOOP
			RAISE NOTICE '-- PROPERTY: % | SPACE: % |> DELETE UNWANTED IDX: %!',xyz_needless_manual_idx.idx_property, space, xyz_needless_manual_idx.idx_name;
			EXECUTE FORMAT ('DROP INDEX IF EXISTS %s."%s" ', schema, xyz_needless_manual_idx.idx_name);
		END LOOP;

        /** Check if auto-indexing is turend off - if yes, delete auto-indices */
        select auto_indexing from xyz_config.xyz_idxs_status into is_auto_indexing where spaceid = space;

        IF is_auto_indexing = false THEN
            BEGIN
                FOR xyz_needless_auto_idx IN
                    SELECT idx_name, idx_property
                    FROM xyz_index_list_all_available(schema,space)
                    WHERE src='a'
                        LOOP
                            RAISE NOTICE '-- PROPERTY: % | SPACE: % |> DELETE UNWANTED AUTO-IDX: %!',xyz_needless_auto_idx.idx_property, space, xyz_needless_auto_idx.idx_name;
                    EXECUTE FORMAT ('DROP INDEX IF EXISTS %s."%s" ', schema, xyz_needless_auto_idx.idx_name);
                END LOOP;
            END;
        END IF;

        /** Analyze IDX-Proposals aka AUTOMATIC MODE */
		SELECT * FROM xyz_config.xyz_idxs_status
			INTO xyz_space_stat
				WHERE idx_proposals IS NOT NULL
			AND idx_creation_finished = false
            AND (auto_indexing IS NULL OR auto_indexing = true)
			AND count >= 0
			AND spaceid = space
            AND schem = schema;

		IF xyz_space_stat.idx_proposals IS NOT NULL THEN
			RAISE NOTICE 'ANALYSE AUTOMATIC IDX PROPOSALS: % on SPACE:%', xyz_space_stat.idx_proposals, space;
		END IF;

		FOR xyz_idx_proposal IN
			SELECT value->>'property' as property, value->>'type' as type from jsonb_array_elements(xyz_space_stat.idx_proposals)
		LOOP
			IF idx_false_list @> ARRAY[xyz_idx_proposal.property] THEN
				RAISE NOTICE '-- PROPERTY: % | TYPE: % | SPACE: % |> IDX MANUAL DEACTIVATED -> SKIP!',xyz_idx_proposal.property, xyz_idx_proposal.type, space;
				CONTINUE;
			END IF;

			IF (SELECT * from xyz_index_property_available(schema, space, xyz_idx_proposal.property)) = true THEN
				RAISE NOTICE '-- PROPERTY: % | TYPE: % | SPACE: % |> IDX ALREADY EXISTS -> SKIP!', xyz_idx_proposal.property, xyz_idx_proposal.type, space;
				CONTINUE;
			END IF;

			BEGIN
				RAISE NOTICE '--PROPERTY: % | TYPE: % | SPACE: % |> CREATE AUTOMATIC IDX!',xyz_idx_proposal.property, xyz_idx_proposal.type, space;
				PERFORM xyz_index_creation_on_property(schema, space, xyz_idx_proposal.property,'a');

				EXCEPTION WHEN OTHERS THEN
					RAISE NOTICE '--PROPERTY: % | TYPE: % | SPACE: % |> IDX CREATION ERROR -> SKIP!',xyz_idx_proposal.property, xyz_idx_proposal.type, space;
			END;
		END LOOP;

        perform xyz_maintain_o_idxs_for_space( schema, space );

		/** set indication that idx creation is finished */
		UPDATE xyz_config.xyz_idxs_status
			SET idx_creation_finished = true,
				idx_proposals = null,
			    idx_available = (select jsonb_agg(FORMAT('{"property":"%s","src":"%s"}',idx_property, src)::jsonb) from (
				select * from xyz_index_list_all_available(schema, space)
					order by idx_property
			)b
		)
		WHERE spaceid = space
          AND schem = schema;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_create_idxs(text, integer, integer, integer, text[])
-- DROP FUNCTION xyz_create_idxs(text, integer, integer, integer, text[]);
CREATE OR REPLACE FUNCTION xyz_create_idxs(
    schema text,
    lim integer,
    off integer,
    mode integer,
    owner_list text[])
  RETURNS void AS
$BODY$
	/**
	* Description: This function iterates over all spaces which are marked with idx_creation_finished=false in
	*	the xyz_config.xyz_idxs_status maintenance table.
	*
	* Parameters:
	*   @schema	- schema in which the xyz-spaces are located
	*   @lim - max amount of spaces to iterate over
	*   @off - offset, required for parallel executions
	*/

	DECLARE xyz_space_stat record;
	DECLARE xyz_idx_proposal record;
	DECLARE big_space_threshold integer := 10000;

	BEGIN
		IF mode = 1 OR mode = 2 THEN
			RAISE NOTICE 'WRITE NEWEST STATISTICS!';
			PERFORM xyz_write_newest_statistics(schema, owner_list, big_space_threshold);
		END IF;

		IF mode = 2 THEN
			RAISE NOTICE 'WRITE NEWEST ANALYSES!';
			PERFORM xyz_write_newest_idx_analyses(schema);
		END IF;

		FOR xyz_space_stat IN
			SELECT * FROM xyz_config.xyz_idxs_status A
				LEFT JOIN pg_tables B ON (B.tablename = A.spaceid)
			WHERE
				idx_creation_finished = false
				AND b.tablename is not null
				AND schem = schema
			 ORDER BY count, spaceid
				LIMIT lim OFFSET off
		LOOP
			RAISE NOTICE 'MAINTAIN IDX FOR: % !',xyz_space_stat.spaceid;
			PERFORM xyz_maintain_idxs_for_space(schema, xyz_space_stat.spaceid);
		END LOOP;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_property_path_to_array(text)
-- DROP FUNCTION xyz_property_path_to_array(text);
CREATE OR REPLACE FUNCTION xyz_property_path_to_array(propertypath text)
  RETURNS text[] AS
$BODY$
	/**
	* Description: Converts a jsonpath (e.g.: jsondata.foo.bar) to a text array ["jsondata","foo","bar"]
	*
	* Parameters:
	*   @propertypath	- path down to the json-field (e.g.: jsondata.foo.bar)
	*
	* Returns:
	*	text[] with json path segments
	*/

	DECLARE property TEXT;
	DECLARE jsonarray TEXT[];

	BEGIN
		FOR property IN
			SELECT * from regexp_split_to_table(propertypath,'\.')
		LOOP
			select array_append(jsonarray,property) into jsonarray;
		END LOOP;

		RETURN jsonarray;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_property_path(text)
-- DROP FUNCTION xyz_property_path(text);
CREATE OR REPLACE FUNCTION xyz_property_path(propertypath TEXT)
  RETURNS TEXT AS
$BODY$
	/**
	* Description: Converts a jsonpath (e.g.: jsondata.foo.bar) to a jsonb notation ('jsondata'->'foo'->'bar')
	*
	* Parameters:
	*   @propertypath	- path down to the json-field (e.g.: jsondata.foo.bar)
	*
	* Returns:
	*	text with jsonb notation ('jsondata'->'foo'->'bar')
	*/

	DECLARE property TEXT;
	DECLARE jsonpath TEXT := '';

	BEGIN
		FOR property IN
			SELECT * from regexp_split_to_table(propertypath,'\.')
		LOOP
			jsonpath := jsonpath || '''' ||property || '''' || '->';
		END LOOP;

		jsonpath := substring(jsonpath from 0 for char_length(jsonpath)-1);

		RETURN jsonpath;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_property_datatype(text, text, text, integer)
-- DROP FUNCTION xyz_property_datatype(text, text, text, integer);
CREATE OR REPLACE FUNCTION xyz_property_datatype(
	schema text,
    spaceid text,
    propertypath text,
    tablesamplecnt integer)
  RETURNS TEXT AS
$BODY$
	/**
	* Description:
	*	Tries to detect the data type of a jsonkey. {"foo" : "bar"} => "bar"=string
	*
	* Parameters:
	*   @spaceid		- id of the XYZ-space (tablename)
	*   @propertypath	- path down to the json-field (e.g.: jsondata.foo.bar)
	*   @tablesamplecnt	- define the value which get further used in the tablesample statement. If it is null full table scans s will be performed.
	*
	* Returns:
	*	datatype		- string | number | boolean | object | array | null
	*/

	DECLARE datatype TEXT := xyz_index_dissolve_datatype(propertypath);
	DECLARE json_proppath TEXT;

	BEGIN
		IF (datatype IS NOT NULL) THEN
			RETURN datatype;
		END IF;

		SELECT xyz_property_path(propertypath) into json_proppath;

		EXECUTE format('SELECT jsonb_typeof((jsondata->''properties''->%s)::jsonb)::text '
			||'	FROM %s."%s" TABLESAMPLE SYSTEM_ROWS(%s) '
			||'where jsondata->''properties''->%s  is not null limit 1',
				json_proppath, schema, spaceid, tablesamplecnt, json_proppath)
			INTO datatype;

		IF datatype IS NULL THEN
			RETURN 'unknown';
		END IF;

		RETURN datatype;
		EXCEPTION WHEN OTHERS THEN
			RETURN 'unknown';
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_property_statistic(text, text, integer)
-- DROP FUNCTION xyz_property_statistic(text, text, integer);
CREATE OR REPLACE FUNCTION xyz_property_statistic_v2(
    IN schema text,
    IN spaceid text,
    IN tablesamplecnt integer)
  RETURNS TABLE(key text, count integer, searchable boolean, datatype text) AS
$BODY$
	/**
	* Description: Returns table which includes a overview about properties.keys, their counts and the information if they are searchable.
	*	e.g: => 	fc, 544, true
	*				name, 544, false
	*				oda, 213, false
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the XYZ-space (tablename)
	*   @tablesamplecnt	- define the value which get further used in the tablesample statement. If it is null full table scans s will be performed.
	*
	* Returns (table):
	*	key				- property.key
	*	count			- number how often the property.key is present in the XYZ-space
	*	datatype		- string | number | boolean | object | array | null
	*/

	DECLARE
		/** used for big-spaces and get filled via pg_class */
		estimate_cnt bigint;

		/** list of indexes which are already available in the space */
		idxlist jsonb;
	BEGIN
		SELECT COALESCE(jsonb_agg(idx_property),'""'::jsonb) into idxlist
			FROM( select distinct split_part(idx_property,',',1) as idx_property from xyz_index_list_all_available($1,$2)
		WHERE src IN ('a','m','o') and not idx_property ~ '^f\..*'  ) A;

		IF tablesamplecnt is NULL
			THEN
				RETURN QUERY EXECUTE
					'SELECT DISTINCT ON(propkey) * FROM (  '
					|| '	SELECT propkey, '
					|| '		(SELECT COALESCE(COUNT(*)::numeric::INTEGER , 0) as count '
					|| '			FROM "'||schema||'"."'||spaceid||'" '
					|| '			WHERE jsondata#> xyz_property_path_to_array(''properties.''||propkey)  IS NOT NULL), '
					|| '		true as searchable, '
					|| '		(SELECT * from xyz_property_datatype('''||schema||''','''||spaceid||''', propkey, 1000)) as datatype '
					|| '	   FROM( '
					|| '		select distinct split_part(idx_property,'','',1) as propkey from xyz_index_list_all_available('''||schema||''','''||spaceid||''') '
					|| '			WHERE src IN (''a'',''m'', ''o'') '
					|| '	   ) B group by propkey '
					|| '	UNION '
					|| '	SELECT  propkey, '
					|| '		COALESCE(COUNT(*)::numeric::INTEGER, 0) as count, '
					|| '		(SELECT '''||idxlist||''' @>  to_jsonb(propkey)) as searchable, '
					|| '		(SELECT * from xyz_property_datatype('''||schema||''','''||spaceid||''',propkey,1000)) as datatype '
					|| '			FROM( 	'
					|| '				SELECT jsonb_object_keys(jsondata->''properties'') as propkey '
					|| '				FROM "'||schema||'"."'||spaceid||'" '
					|| '		) A WHERE propkey!=''@ns:com:here:xyz'' '
					|| '			GROUP BY propkey ORDER by propkey,count DESC '
					|| ') C';
		ELSE
			SELECT reltuples into estimate_cnt FROM pg_class WHERE oid = concat('"',$1, '"."', $2, '"')::regclass;

			RETURN QUERY EXECUTE
				'SELECT DISTINCT ON(propkey) * FROM (  '
				|| '	SELECT propkey, '
				|| '		(SELECT TRUNC(((COUNT(*)/1000::real) * '||estimate_cnt||')::numeric, 0)::INTEGER as count '
				|| '			FROM "'||schema||'"."'||spaceid||'" TABLESAMPLE SYSTEM_ROWS('||tablesamplecnt||') '
				|| '			WHERE jsondata#> xyz_property_path_to_array(''properties.''||propkey)  IS NOT NULL), '
				|| '		true as searchable, '
				|| '		(SELECT * from xyz_property_datatype('''||schema||''','''||spaceid||''',propkey,'||tablesamplecnt||')) as datatype '
				|| '	   FROM( '
				|| '		select distinct split_part(idx_property,'','',1) as propkey from xyz_index_list_all_available('''||schema||''','''||spaceid||''') '
				|| '			WHERE src IN (''a'',''m'', ''o'') '
				|| '	   ) B group by propkey '
				|| '	UNION '
				|| '	SELECT  propkey, '
				|| '		TRUNC(((COUNT(*)/1000::real) * '||estimate_cnt||')::numeric, 0)::INTEGER as count, '
				|| '		(SELECT '''||idxlist||''' @>  to_jsonb(propkey)) as searchable, '
				|| '		(SELECT * from xyz_property_datatype('''||schema||''','''||spaceid||''',propkey,'||tablesamplecnt||')) as datatype '
				|| '			FROM( '
				|| '				SELECT jsonb_object_keys(jsondata->''properties'') as propkey '
				|| '				FROM "'||schema||'"."'||spaceid||'" TABLESAMPLE SYSTEM_ROWS('||tablesamplecnt||') '
				|| '		) A WHERE propkey!=''@ns:com:here:xyz'' '
				|| '			GROUP BY propkey ORDER by propkey,count DESC '
				|| ') C';
		END IF;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_statistic_newest_spaces_changes(text, text[], integer)
-- DROP FUNCTION xyz_statistic_newest_spaces_changes(text, text[], integer);
CREATE OR REPLACE FUNCTION xyz_statistic_newest_spaces_changes(
    IN schema text,
    IN owner_list text[],
    IN min_table_count integer)
  RETURNS TABLE(spaceid text, tablesize jsonb, geometrytypes jsonb, properties jsonb, tags jsonb, count jsonb, bbox jsonb) AS
$BODY$
	/**
	*  Description: Returns complete statistic about all xyz-spaces:
	*		- which are new to the system
	*		- where more than 3000 rows as changed (added/deleted)
	*
	*  Parameters:
	*    @schema			- schema in which the XYZ-spaces are located
	*    @spaceid			- id of the XYZ-space (tablename)
	*    @min_table_count	- defines the count of rows a table have to has to get included.
	*
	*  Returns (table):
	*    spaceid			- id of the space
	*    tabelsize			- storage size of space
	*    geometrytypes		- list of geometry types which are present
	*    properties			- list of available properties and their counts
	*    tags				- number of tags found in space
	*    count				- number of records found in space
	*    bbox				- bbox in which the space objects are located
	*/

	/** List of all xyz-spaces */
	DECLARE xyz_spaces record;

	/** used to store xyz-space statistic results */
	DECLARE xyz_space_stat record;

	BEGIN

	FOR xyz_spaces IN
		SELECT relname as spaceid, E.spaceid as stat_spaceid, reltuples as current_cnt, E.count as old_cnt,
			(E.count-reltuples) as diff
		FROM pg_class C
			LEFT JOIN pg_tables D ON (D.tablename = C.relname)
			LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
			LEFT JOIN xyz_config.xyz_idxs_status E ON (E.spaceid = C.relname)
		WHERE relkind='r' AND nspname = ''||schema||'' AND array_position(owner_list, tableowner::text) > 0
			/** More than 3000 objecs has changed OR space is new and has more than min_table_count entries */
			AND ((ABS(COALESCE(E.count,0) - COALESCE(reltuples,0)) > 3000 AND reltuples > min_table_count )  OR ( E.count IS null AND reltuples > min_table_count ))
			AND relname != 'spatial_ref_sys'
			ORDER BY reltuples
	LOOP
		BEGIN
			spaceid := xyz_spaces.spaceid;

            --skip history tables
            IF substring(spaceid,length(spaceid)-3) = '_hst' THEN
                CONTINUE;
            END IF;

			EXECUTE format('SELECT tablesize, geometrytypes, properties, tags, count, bbox from xyz_statistic_space(''%s'',''%s'')',schema , xyz_spaces.spaceid)
				INTO tablesize, geometrytypes, properties, tags, count, bbox;
			RETURN NEXT;
			EXCEPTION WHEN OTHERS THEN
				RAISE NOTICE 'ERROR CREATING STATISTIC ON SPACE %',  xyz_spaces.spaceid;
		END;
	END LOOP;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_write_newest_idx_analyses(text)
-- DROP FUNCTION xyz_write_newest_idx_analyses(text);
CREATE OR REPLACE FUNCTION xyz_write_newest_idx_analyses(schema text)
  RETURNS void AS
$BODY$
	/**
	*  Description: Updates xyz_config.xyz_idxs_status maintenance table with idx_analyses data and information
	*   which Indices are currently existing. Therefore xyz_index_proposals_on_properties() and xyz_index_list_all_available()
	*   functions are getting used. Finally idx_proposalsm,idx_available on the maintenance table are getting updated accordingly.
	*   All spaces are getting analyzed which are marked with idx_creation_finished=false in the maintenance table.
	*
	*  Parameters:
	*    @schema	- schema in which the xyz-spaces are located
	*/

	DECLARE xyz_space_stat record;
	DECLARE idx_prop jsonb;
	DECLARE idx_av jsonb;

	BEGIN
		FOR xyz_space_stat IN
			SELECT * FROM xyz_config.xyz_idxs_status
				WHERE idx_creation_finished = false
					AND count > 0 --to avoid processing of spaces which are not already present (manual IDX)
					AND count < 5000000 --temp
					AND (auto_indexing IS NULL OR auto_indexing = true)
		LOOP
			BEGIN
				IF xyz_space_stat.prop_stat != '[]'::jsonb THEN
					select jsonb_agg(FORMAT('{"property":"%s","type":"%s"}',prop_key, prop_type)::jsonb) INTO idx_prop from (
						select * from xyz_index_proposals_on_properties(schema,xyz_space_stat.spaceid)
							order by prop_key
					)B;
				END IF;

				select jsonb_agg(FORMAT('{"property":"%s","src":"%s"}',idx_property, src)::jsonb) INTO idx_av from (
					select * from xyz_index_list_all_available(schema,xyz_space_stat.spaceid)
						--where src='a'
						order by idx_property
				)A;

				UPDATE xyz_config.xyz_idxs_status
					SET idx_proposals = idx_prop,
					    idx_available = idx_av
						WHERE spaceid = xyz_space_stat.spaceid;
			END;
		END LOOP;
		PERFORM pg_sleep(1.5);
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_write_newest_statistics(text, text[], integer)
-- DROP FUNCTION xyz_write_newest_statistics(text, text[], integer);
CREATE OR REPLACE FUNCTION xyz_write_newest_statistics(
    schema text,
    owner_list text[],
    min_table_count integer)
  RETURNS void AS
$BODY$
	/**
	*  Description: writes newest statistic data into the xyz_config.xyz_idxs_status maintenance table. This data is getting used
	*	later on from the Auto-Indexer to create index-proposals.
	*
	*  Parameters:
	*    @schema			- schema in which the XYZ-spaces are located
	*    @owner_list		- list of database users which has the tables created (owner). Normally is this only one user.
	*    @min_table_count	- defines the count of rows a table have to has to get included.
	*/

	DECLARE xyz_spaces record;
	DECLARE xyz_idxs_status_exists text;
	BEGIN
		SELECT COALESCE (ex::text!='xyz_config.xyz_idxs_status')
			into xyz_idxs_status_exists
				from to_regclass('xyz_config.xyz_idxs_status') as ex;

		IF xyz_idxs_status_exists IS NULL THEN
			CREATE TABLE IF NOT EXISTS xyz_config.xyz_idxs_status
			(
			  runts timestamp with time zone,
			  spaceid text NOT NULL,
			  schem text,
			  idx_available jsonb,
			  idx_proposals jsonb,
			  idx_creation_finished boolean,
			  count bigint,
			  prop_stat jsonb,
			  idx_manual jsonb,
              auto_indexing jsonb,
			  CONSTRAINT xyz_idxs_status_pkey PRIMARY KEY (spaceid)
			);
			INSERT INTO xyz_config.xyz_idxs_status (spaceid,count) VALUES ('idx_in_progress','0');
		END IF;

		FOR xyz_spaces IN
			SELECT spaceid
				FROM xyz_config.xyz_idxs_status C
					LEFT JOIN pg_class A ON (A.relname = C.spaceid)
					LEFT JOIN pg_tables D ON (D.tablename = C.spaceid)
				WHERE (
						D.tablename is null
						AND C.spaceid != 'idx_in_progress'
						AND C.count IS NOT NULL
					)
				    OR (
						(COALESCE(reltuples,0) < min_table_count OR C.count IS NULL)
						AND (idx_manual IS NULL OR idx_manual = '{}')
                        AND auto_indexing IS NULL
						AND C.spaceid != 'idx_in_progress'
                    )OR (
                        substring(C.spaceid,length(C.spaceid)-3) = '_hst'
                    )
		LOOP
			RAISE NOTICE 'Remove deleted space %',xyz_spaces.spaceid;
			DELETE FROM xyz_config.xyz_idxs_status
				WHERE spaceid = xyz_spaces.spaceid;
		END LOOP;

		xyz_spaces := NULL;

		FOR xyz_spaces IN
			SELECT * from xyz_statistic_newest_spaces_changes(schema, owner_list , min_table_count)
		LOOP
			INSERT INTO xyz_config.xyz_idxs_status  as x_s (runts,spaceid,schem,count,prop_stat,idx_creation_finished)
			VALUES
			   (
			      CURRENT_TIMESTAMP,
			      xyz_spaces.spaceid,
			      schema,
			      (xyz_spaces.count->>'value')::bigint,
			      (xyz_spaces.properties->'value')::jsonb,
			      false
			   )
			ON CONFLICT (spaceid)
			DO
			UPDATE
				SET runts = CURRENT_TIMESTAMP,
				    count = (xyz_spaces.count->>'value')::bigint,
				    prop_stat =  (xyz_spaces.properties->'value')::jsonb,
				    idx_creation_finished = false
					WHERE x_s.spaceid = xyz_spaces.spaceid;
		END LOOP;
		PERFORM pg_sleep(1.5);
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_statistic_all_spaces(text, text[], integer)
-- DROP FUNCTION xyz_statistic_all_spaces(text, text[], integer);
CREATE OR REPLACE FUNCTION xyz_statistic_all_spaces(
    IN schema text,
    IN owner_list text[],
    IN min_table_count integer)
  RETURNS TABLE(spaceid text, tablesize jsonb, geometrytypes jsonb, properties jsonb, tags jsonb, count jsonb, bbox jsonb) AS
$BODY$
	/**
	* Description: Returns complete statistic about all xyz-spaces. Takes a lot of time depending on the amount of spaces.
	*
	* Parameters:
	*   @schema			- schema in which the xyz-spaces are located
	*   @owner_list		- list of database users which has the tables created (owner). Normally is this only one user.
	*   @min_table_count- defines the count of rows a table have to has to get included.
	*
	* Returns (table):
	*   spaceid			- id of the space
	*   tabelsize		- storage size of space
	*   geometrytypes	- list of geometrytypes which are present
	*   properties		- list of available properties and their counts
	*   tags			- number of tags found in space
	*   count			- number of records found in space
	*   bbox			- bbox in which the space objects are located
	*/

	/** List of all xyz-spaces */
	DECLARE xyz_spaces record;

	/** used to store xyz-space statistic results */
	DECLARE xyz_space_stat record;

	BEGIN

	FOR xyz_spaces IN
		SELECT relname as spaceid,reltuples as cnt
			FROM pg_class C
				LEFT JOIN pg_tables D ON (D.tablename = C.relname)
				LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
			WHERE relkind='r' AND nspname = ''||schema||'' AND array_position(owner_list, tableowner::text) > 0
				ORDER BY reltuples DESC
	LOOP
		spaceid := xyz_spaces.spaceid;

		IF min_table_count > 0 AND min_table_count > xyz_spaces.cnt THEN
			RETURN;
		ELSE
			EXECUTE format('SELECT tablesize, geometrytypes, properties, tags, count, bbox from xyz_statistic_space(''%s'',''%s'')',schema , xyz_spaces.spaceid)
				INTO tablesize, geometrytypes, properties, tags, count, bbox;
			RETURN NEXT;
		END IF;
	END LOOP;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_property_evaluation(text, text, text, integer)
-- DROP FUNCTION xyz_property_evaluation(text, text, text, integer);
CREATE OR REPLACE FUNCTION xyz_property_evaluation(
    IN schema text,
    IN spaceid text,
    IN propkey text,
    IN tablesamplecnt integer)
  RETURNS TABLE(count bigint, val text, jtype text) AS
$BODY$
	/**
	*  Description: Returns table which includes a overview about properties.key->values and their counts.
	*		e.g.:	 properties.fc
	*				=> 	233, 1, integer
	*					22,  2, integer
	*					577, 5, integer
	*	Only primitive data type are allowed: string,number,boolean - object,array are getting ignored
	*	If more than one data type exists in properties.key->values the result will be NULL (properties.fc = 5 AND properties.fc = 'foo')
	*
	* Parameters:
	*   @schema			- schema in which the xyz-spaces are located
	*   @spaceid		- id of the space (tablename)
	*   @propkey		- name of the property which should get evaluated. properties.>key<
	*   @tablesamplecnt	- define the value which get further used in the tablesample statement. If it is null full table scans s will be performed.
	*
	* Returns (table):
	*	count			- indicate how often the value exists
	*	val				- value of json.key
	*	jtype			- data type of value
	*/

	/** to store a distinct list of properties.key->values (properties.fc => { 1,2,3,4,5} */
	DECLARE prop_values_distinct record;

	/** used for big spaces */
	DECLARE tablesample_expression text := '';

	/** to collect object-Types (string,text,boolean,array..) of properties.key->values */
	DECLARE object_types text[];

	BEGIN
		propkey := replace(propkey, '''', '''''');

		IF tablesamplecnt is not NULL THEN tablesample_expression := concat('TABLESAMPLE SYSTEM_ROWS(',tablesamplecnt,')');
		END IF;

		/** ignore: array, object, null */
		EXECUTE format('SELECT array_agg(obj_type) FROM( '
				||'SELECT DISTINCT jsonb_typeof((jsondata->''properties''->%L)::jsonb) as obj_type '
				||'		from  %s."%s" %s '
				||'	WHERE jsonb_typeof((jsondata->''properties''->''%s'')::jsonb) in (''string'', ''number'', ''boolean'')) B '
				, propkey, schema, spaceid, tablesample_expression, propkey)
			INTO object_types;

		IF object_types is NULL OR cardinality(object_types) != 1 THEN
			-- Condition not met / only one primitive object Type allowed
			RAISE NOTICE 'object_types not supported: % ', object_types;
			RETURN;
		END IF;

		/** search distinct property values which can be found in properties.propkey */
		FOR prop_values_distinct IN
			EXECUTE FORMAT(
				'SELECT count(*) as cnt, COALESCE((jsondata->''properties''->%L)::text,'''') as val '
				|| ' from %s."%s" %s GROUP BY val ', propkey, schema, spaceid, tablesample_expression)
		LOOP
			/** count how often the current value exists */
			jtype := object_types[1];
			val := prop_values_distinct.val;
			count := prop_values_distinct.cnt;

			RETURN NEXT;
		END LOOP;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_proposals_on_properties(text, text)
-- DROP FUNCTION xyz_index_proposals_on_properties(text, text);
CREATE OR REPLACE FUNCTION xyz_index_proposals_on_properties(
    IN schema text,
    IN _spaceid text)
  RETURNS TABLE(prop_key text, prop_type text) AS
$BODY$
	/**
	*	Description: Returns table which includes a list with flat properties.key's and their corresponding data types of the values.
	*	This list represents proposals which can be used for index creations on property root-level.
	*
	*	Only primitive data types are allowed (string,number,boolean). Already indexed properties are getting ignored.
	*
	*	e.g.: => 	fc, integer
	*				name, string
	*
	* Parameters:
	*   @schema		- schema in which the xyz-spaces are located
	*   @spaceid	- id of the space (tablename)
	*
	* Returns (table):
	*	prop_key	- property.key which should get indexed
	*	prop_type	- data type of prop_key values
	*/

	DECLARE
		index_limit integer := 8;
		cnt bigint;
		prop_stat record;
		prop_popularity real;
		prop_eval record;

		prop_val_popularity real;
		prop_is_relevant boolean;

		value_type text;
		value_loop_i integer;

		auto_tablescan integer := null;
		semantic_hit boolean;
	BEGIN
		SELECT reltuples into cnt FROM pg_class WHERE oid = concat('"',$1, '"."', $2, '"')::regclass;
		if cnt is null OR cnt <= 50000 THEN
			EXECUTE format('SELECT count(*) from %s."%s"', schema,_spaceid)
				INTO cnt;
		END IF;

		RAISE NOTICE 'TABLE CNT %',cnt;

		IF cnt > 10000 THEN
			auto_tablescan := 1000;
		END IF;

		/** Check which properties are available in the space */
		FOR prop_stat IN
			/** Ignore manual deactivated idx */
			SELECT * from(
				SELECT *,
					(SELECT (case when idx_manual ? 'searchableProperties' then nullif( idx_manual->'searchableProperties', 'null' ) else idx_manual end)->key from xyz_config.xyz_idxs_status WHERE spaceid = _spaceid) as manual
					from (
						SELECT * from xyz_property_statistic(schema, _spaceid, auto_tablescan)
				) A
			) B where manual = 'true' OR manual is NULL LIMIT index_limit
		LOOP
			prop_popularity := (prop_stat.count::real / cnt::real);

			IF prop_popularity > 0.8 THEN
				/** Property is in more than 80% of rows available */
				RAISE NOTICE '-- KEY: % KEY_COUNT: % INDEXED: % PROP-POPULARITY: %', prop_stat.key, prop_stat.count, prop_stat.searchable, prop_popularity;

				/** Check if index is already available */
				IF prop_stat.searchable THEN
					RAISE NOTICE '-- SKIP - Prop % because its already indexed!', prop_stat.key;
					CONTINUE;
				END IF;

				/** TODO: Semantic property name check */
				/**
				*	EXECUTE format('SELECT xyz_index_proposals_semantic_check(%L)',  prop_stat.key)
				*	INTO semantic_hit;
				*
				*	IF semantic_hit THEN
				*		prop_key := prop_stat.key::text;
				*		EXECUTE format('SELECT jsonb_typeof((jsondata->''properties''->%L)::jsonb)::text '
				*				||'	FROM %s."%s" '
				*				||'where jsondata->''properties''->>%L is not null limit 1',
				*				prop_key, schema, _spaceid, prop_key)
				*			INTO prop_type;
				*		-- Only permit allowed datatypes
				*		IF prop_type in ('string', 'number', 'boolean') THEN
				*			RAISE NOTICE '-- Semantic Hit - Prop %!', prop_stat.key;
				*			RAISE NOTICE '------>> ADD: % %',prop_stat.key, prop_type;
				*			RETURN NEXT;
				*		END IF;
				*		CONTINUE;
				*	END IF;
				*/

				prop_is_relevant := false;
				value_type := null;
				value_loop_i := 0;

				FOR prop_eval IN EXECUTE FORMAT(
					'SELECT * FROM( '
					||'	SELECT row_number() over() as res_cnt, * FROM('
					||'		SELECT count,val,jtype FROM( '
					||'			SELECT count,val,jtype FROM xyz_property_evaluation(%L,%L,%L,%L) order by count DESC limit 5 '
					||'		)iii order by count ASC '
					||'	)i '
					||')ii order by 1 desc', schema, _spaceid, prop_stat.key, auto_tablescan)
				LOOP
					/** If only 1 value or 2 different values for are properties.key are available we do not create an index */
					IF value_loop_i = 0 AND prop_eval.res_cnt < 3 THEN
						RAISE NOTICE '------ SKIP - Prop % with only % different values!', prop_stat.key, prop_eval.res_cnt;
						EXIT;
					END IF;

					/** If we can find two different data-types we can't recommend an index-creation */
					IF value_type != null AND prop_eval.jtype != value_type THEN
						RAISE NOTICE '------ SKIP - Prop % has values with different types! % != %', prop_stat.key, value_type, prop_eval.jtype;
						EXIT;
					END IF;

					prop_val_popularity := (prop_eval.count::real / cnt::real);

					/** NO value should exist more than 33% records of the table */
					IF prop_val_popularity > 0.33 THEN
						RAISE NOTICE '------ SKIP - Val: % of Prop: % is to popular: %', prop_eval.val, prop_stat.key, prop_val_popularity;
						EXIT;
					END IF;

					/**
					* property has more than 2 values
					* each value has populariyt of < 33%
					* only one data_type is available
					*/
					prop_is_relevant := true;
					value_type := prop_eval.jtype;
					value_loop_i := value_loop_i + 1;
				END LOOP;

				IF prop_is_relevant THEN
					RAISE NOTICE'------>> ADD: % %',prop_stat.key,prop_eval.jtype;
					prop_key := prop_stat.key::text;
					prop_type := prop_eval.jtype::text;
					RETURN NEXT;
				END IF;
			ELSE
				RAISE NOTICE '-- SKIP (PROP-POP to low!) KEY: % KEY_COUNT: % INDEXED: % PROP-POPULARITY: %', prop_stat.key, prop_stat.count, prop_stat.searchable, prop_popularity;
			END IF;
		END LOOP;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_creation_on_property(text, text, text, character
-- DROP FUNCTION xyz_index_creation_on_property(text, text, text, character);
CREATE OR REPLACE FUNCTION xyz_index_creation_on_property(
    schema text,
    spaceid text,
    propkey text,
    source character)
  RETURNS text AS
$BODY$
	/**
	* Description:
	*	This function creates concurrently an index for a properties.key in a XYZ-space.
	*	The name of the indexed property get stored as an comment on the created index.
	*	Get comment with: select obj_description( ('xyz."idx_name"')::regclass )
	*	The index get stored as JSONB Type by use of BTREE.
	*	Its recommended to SET ENABLE_SEQSCAN = OFF;
	*
	* Parameters:
	*   @schema		- schema in which the XYZ-spaces are located
	*   @spaceid	- id of the XYZ-space (tablename)
	*   @propkey	- name of the property for a index should get created
	*	@source		- 'a'|'m' a=automatic m=manual
	*
	* Returns:
	*   idx_name - name of the created Index
	*/

	DECLARE
		idx_name text;
	BEGIN
		source = lower(source);

		IF source not in ('a','m') THEN
			RAISE EXCEPTION 'Source ''%'' not supported. Use ''m'' for manual or ''a'' for automatic!',source;
		END IF;

		SELECT * into idx_name
			FROM xyz_index_name_for_property(spaceid, propkey, source);

		/** TODO: CREATE INDEX CONCURRENTLY - make use of dblink "%s" ' */
		EXECUTE format('CREATE INDEX "%s" '
				||'ON %s."%s" '
				||'((jsondata->''properties''->''%s''))',
			idx_name, schema, spaceid, propkey);

		EXECUTE format('COMMENT ON INDEX %s."%s" '
				||'IS ''p.name=%s''',
			schema, idx_name, propkey);

		RETURN idx_name;
	END
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_geotype(geometry)
-- DROP FUNCTION xyz_geotype(geometry);
CREATE OR REPLACE FUNCTION xyz_geotype(geo geometry)
  RETURNS text AS
$BODY$
	/**
	* Description: Returns Geometry Types which are fit to XYZ models
	*
	* Parameters:
	*	@geo			- Geometry which should get translated
	*
	* Returns:
	*	geometryText	- Simple XYZ-Conform text representation of geometry
	*/

	BEGIN
		IF geo is NULL THEN
			RETURN 'NULL';
		ELSE
			CASE ST_GeometryType(geo)
			     WHEN 'ST_Point' THEN return 'Point';
			     WHEN 'ST_LineString' THEN return 'LineString';
			     WHEN 'ST_Polygon' THEN return 'Polygon';
			     WHEN 'ST_MultiPoint' THEN return 'MultiPoint';
			     WHEN 'ST_MultiLineString' THEN return 'MultiLineString';
			     WHEN 'ST_MultiPolygon' THEN return 'MultiPolygon';
			     ELSE return 'UNKNOWN';
			END CASE;
		END IF;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_name_for_property(text, text, character)
-- DROP FUNCTION xyz_index_name_for_property(text, text, character);
CREATE OR REPLACE FUNCTION xyz_index_name_for_property(
    spaceid text,
    propkey text,
    source character)
  RETURNS text AS
$BODY$
	/**
	* Description: Create an index-name for a properties.key which is further used in XYZ-scope.
	*
	*	idx_name = {prefix}_{spaceid}_{propkey}_{source}
	*	prefix = 'idx'
	*	propkey = (md5(properties.key1)).substring(0,8)
	*	source = 'm' | 'a' <=> manual | automatic
	*
	*	E.g.:	idx_osm-building-world_098df_i_m
	*
	* Parameters:
	*   @spaceid	- id of the space (and tablename)
	*   @propkey	- name of the property for wich an index-name should get created. properties.>key<
	*   @source		- 'a'|'m' a=automatic m=manual
	*
	* Returns:
	*	idx_name 	- XYZ-Index name which getting used for the Indices Creations.
	*/

	DECLARE prefix text :='idx';
		idx_name text;

	BEGIN
		source = lower(source);

		IF source not in ('a','m') THEN
			RAISE EXCEPTION 'Source ''%'' not supported. Use ''m'' for manual or ''a'' for automatic!',source;
		END IF;

		select * into idx_name from concat(prefix, '_', spaceid, '_', substring(md5(propkey), 0 ,8 ),'_',source);

		RETURN idx_name;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_find_missing_system_indexes(text, text[])
-- DROP FUNCTION xyz_index_find_missing_system_indexes(text, text[]);
CREATE OR REPLACE FUNCTION xyz_index_find_missing_system_indexes(
    IN schema text,
    IN owner_list text[])
  RETURNS TABLE(has_createdat boolean, has_updatedat boolean, spaceid text, cnt bigint) AS
$BODY$
	/**
	* Description: Find missing XYZ-System Indices (createdAt,updatedAt).
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @owner_list		- list of database users which has the tables created (owner). Normally is this only one user.
	*
	* Returns (table):
	*	has_createdat	- true if Index is present
	*	has_updatedat	- true if Index is present
	*	spaceid			- id of XYZ-space
	*	cnt				- row count of XYZ-space
	*/

	BEGIN
		RETURN QUERY
			SELECT  COALESCE((idx_available ? 'createdAt'), false) AS has_createdAt,
				COALESCE((idx_available ? 'updatedAt'), false) AS has_updatedAt,
				s_id::text, table_cnt from (
				SELECT relname as s_id,reltuples::bigint as table_cnt ,
					(SELECT jsonb_agg(FORMAT('"%s"',idx_property)::jsonb) from (
						SELECT * from xyz_index_list_all_available(''||schema||'',relname)
							WHERE idx_property IN ('createdAt','updatedAt') and src = 's'
							order by idx_property
						)A
					) as idx_available
				FROM pg_class C
					LEFT JOIN pg_tables D ON (D.tablename = C.relname)
					LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
				WHERE relkind='r' AND nspname = ''||schema||'' AND array_position(owner_list, tableowner::text) > 0
					ORDER BY reltuples ASC, spaceid
			) B WHERE idx_available IS NULL OR jsonb_array_length(idx_available) < 2;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_name_dissolve_to_property(text,text)
-- DROP FUNCTION xyz_index_name_dissolve_to_property(text,text);
CREATE OR REPLACE FUNCTION xyz_index_name_dissolve_to_property(IN idx_name text, space_id text)
  RETURNS TABLE(spaceid text, propkey text, source character) AS
$BODY$
	/**
	* Description: Get spaceid, propkey and source from Index-name.
	*
	*		Index-Name: {prefix}_{spaceid}_{propkey}_{source}
	*		prefix = 'idx'
	*		propkey = (md5(properties.key1)).substring(0,8)
	*		source = 'm' | 'a' <=> manual | automatic
	*
	* Parameters:
	*	@idx_name	- name of Index
	*	@space_id	- id of XYZ-space
	*
	* Returns:
	*   spaceid	- id of XYZ-space(tablename)
	*   propkey	- (md5(properties.key1)).substring(0,8) of the property for which an index-name should get created. properties.>key<
	*   source	- 'a'|'m' a=automatic m=manual
	*/

	DECLARE
		idx_split text[];
	BEGIN
		/** idx begins with idx_{spaceid} so we cut this part out */
		SELECT * INTO idx_split FROM regexp_split_to_array(substring(idx_name from char_length(space_id) + 6), '_');

		spaceid := space_id;
		propkey := xyz_index_get_plain_propkey(idx_split[1]);
		source :=  regexp_replace(idx_split[2],'o1[01]*$','o');

		RETURN NEXT;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_index_property_available(text, text, text)
-- DROP FUNCTION xyz_index_property_available(text, text, text);
CREATE OR REPLACE FUNCTION xyz_index_property_available(
    schema text,
    spaceid text,
    propkey text)
  RETURNS boolean AS
$BODY$
	/**
	* Description: This function can be used to check if a json-property is indexed
	*
	*		Index-Name: {prefix}_{spaceid}_{propkey}_{source}
	*		prefix = 'idx'
	*		propkey = (md5(properties.key1)).substring(0,8)
	*		source = 'm' | 'a' <=> manual | automatic
	*
	* Parameters:
	*   @schema		- schema in which the XYZ-spaces are located
	*   @spaceid	- id of XYZ-space(tablename)
	*   @propkey	- path of json-key inside jsondata->'properties' object (eg. foo | foo.bar)
	*
	* Returns:
	*   true		- if property is indexed.
	*/

	DECLARE ret boolean;

	BEGIN
		SELECT indexname IS NOT NULL FROM pg_indexes
			WHERE schemaname = ''||schema||'' AND tablename = ''||spaceid||''
			AND (	indexname = xyz_index_name_for_property(''||spaceid||'',''||propkey||'','a')
			       OR
				-- Check if its really needed
				indexname = xyz_index_name_for_property(''||spaceid||'',''||propkey||'','m')
			    )
		INTO ret;
		return ret is not null;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_property_statistic(text, text, integer)
-- DROP FUNCTION xyz_property_statistic(text, text, integer);
CREATE OR REPLACE FUNCTION xyz_property_statistic(
    IN schema text,
    IN spaceid text,
    IN tablesamplecnt integer)
  RETURNS TABLE(key text, count integer, searchable boolean) AS
$BODY$
	/**
	* Description: Returns table which includes a overview about properties.keys and their counts.
	*	e.g. 	=> 	fc,		544, false
	*				name, 	544, false
	*				oda, 	213, false
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the XYZ-space (tablename)
	*   @tablesamplecnt	- define the value which get further used in the tablesample statement. If it is null full table scans s will be performed.
	*
	* Returns (table):
	*	key				- path of json-key inside jsondata->'properties' object (eg. foo | foo.bar)
	*	count			- how often does the key exists in the dataset
	*	searchable		- true if an index is existing
	*/

	DECLARE
		/** used for big-spaces and get filled via pg_class */
		estimate_cnt bigint;

		/** list of indexes which are already available in the space */
		idxlist jsonb;
	BEGIN
		/** @TODO: check if idx_list is null */
		SELECT COALESCE(jsonb_agg(idx_property),'""'::jsonb) into idxlist
			FROM( select idx_property from xyz_index_list_all_available($1,$2)
		WHERE src IN ('a','m') )A;

		IF tablesamplecnt is NULL THEN
			RETURN QUERY EXECUTE
				'SELECT  key, '
				|| '	 COUNT(key)::numeric::INTEGER as count, '
				|| '	 (SELECT position(key in '''||idxlist||''') > 0) as searchable '
				|| 'FROM( '
				|| '	SELECT jsonb_object_keys(jsondata->''properties'') as key '
				|| '		FROM "'||schema||'"."'||spaceid||'" '
				|| ' 	) a '
				|| 'WHERE key!=''@ns:com:here:xyz'' GROUP BY key ORDER by count DESC, key';
		ELSE
			SELECT reltuples into estimate_cnt FROM pg_class WHERE oid = concat('"',$1, '"."', $2, '"')::regclass;

			RETURN QUERY EXECUTE
				'SELECT  key, '
				|| '	 TRUNC(((COUNT(key)/'||tablesamplecnt||'::real)* '||estimate_cnt||')::numeric, 0)::INTEGER as count, '
				|| '	 (SELECT position(key in '''||idxlist||''') > 0) as searchable '
				|| 'FROM( '
				|| '	SELECT jsonb_object_keys(jsondata->''properties'') as key '
				|| '		FROM "'||schema||'"."'||spaceid||'" TABLESAMPLE SYSTEM_ROWS('||tablesamplecnt||') '
				|| ' 	) a '
				|| 'WHERE key!=''@ns:com:here:xyz'' GROUP BY key ORDER by count DESC, key ';
		END IF;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_tag_statistic(text, text, integer)
-- DROP FUNCTION xyz_tag_statistic(text, text, integer);
CREATE OR REPLACE FUNCTION xyz_tag_statistic(
    IN schema text,
    IN spaceid text,
    IN samplecnt integer)
  RETURNS TABLE(key jsonb, count integer) AS
$BODY$
	/**
	* Description: Returns table which includes a overview about properties.@ns:com:here:xyz->tags and their counts.
	*	e.g.  => 	foo, 544
	*				bar, 123
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the XYZ-space (tablename)
	*   @tablesamplecnt	- define the value which get further used in the tablesample statement. If it is null full table scans will be performed.
	*
	* Returns (table):
	*	tag				- tag name
	*	count			- how often does the tag exists in the dataset
	*/

	DECLARE
		/** used for big-spaces and get filled via pg_class */
		estimate_cnt bigint;
	BEGIN
		IF samplecnt is NULL THEN
			RETURN QUERY EXECUTE
				'SELECT  tag, '
				|| '	 COUNT(tag)::numeric::INTEGER as count '
				|| 'FROM( '
				|| '	SELECT jsonb_array_elements(jsondata->''properties''->''@ns:com:here:xyz''->''tags'') as tag '
				|| '		FROM "'||schema||'"."'||spaceid||'" '
				|| ' 	) a '
				|| 'GROUP BY tag ORDER by count DESC, tag';
		ELSE
			SELECT reltuples into estimate_cnt FROM pg_class WHERE oid = concat('"',$1, '"."', $2, '"')::regclass;

			RETURN QUERY EXECUTE
				'SELECT  tag, '
				|| '	 TRUNC(((COUNT(tag)/'||samplecnt||'::real)* '||estimate_cnt||')::numeric, 0)::INTEGER as count '
				|| 'FROM( '
				|| '	SELECT jsonb_array_elements(jsondata->''properties''->''@ns:com:here:xyz''->''tags'') as tag '
				|| '		FROM "'||schema||'"."'||spaceid||'" TABLESAMPLE SYSTEM_ROWS('||samplecnt||') '
				|| ' 	) a '
				|| 'GROUP BY tag ORDER by count DESC, tag';
		END IF;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_statistic_searchable(jsonb)
-- DROP FUNCTION xyz_statistic_searchable(jsonb);
CREATE OR REPLACE FUNCTION xyz_statistic_searchable(prop_stat jsonb)
  RETURNS text AS
$BODY$
	/**
	* Description: Determine which root-properties are searchable in a XYZ-space.
	*	ALL = means that all root.properties are searchable
	*	PARTIAL = means that some root.properties are searchable
	*	Not used anymore: (NONE = means that no root.propertiy is searchable)
	*
	* Returns:
	*	ALL | PARTIAL
	*/

	DECLARE is_searchable_cnt integer := 0;
	DECLARE prop_cnt integer;
	DECLARE cur_rec record;
  BEGIN
	select jsonb_array_length(prop_stat)
		into prop_cnt;
	FOR cur_rec IN
		SELECT COALESCE((t->'searchable')::text, 'false')::boolean as searchable from jsonb_array_elements(prop_stat) as t
	LOOP
		IF cur_rec.searchable = true THEN
			is_searchable_cnt = is_searchable_cnt + 1;
		END IF;
	END LOOP;

	IF is_searchable_cnt = 0 THEN
		/** Was NONE, but we have id,updatedAt,createAt as SystemProperties -> so the space is PARITAL searchable */
		return 'PARTIAL';
	ELSEIF is_searchable_cnt < prop_cnt THEN
		return 'PARTIAL';
	ELSEIF is_searchable_cnt = prop_cnt THEN
		return 'ALL';
	END IF;
  END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_statistic_xl_space(text, text, integer)
-- DROP FUNCTION xyz_statistic_xl_space(text, text, integer);
CREATE OR REPLACE FUNCTION xyz_statistic_xl_space(
    IN schema text,
    IN spaceid text,
    IN tablesamplecnt integer)
  RETURNS TABLE(tablesize jsonb, geometrytypes jsonb, properties jsonb, tags jsonb, count jsonb, bbox jsonb, searchable text) AS
$BODY$
	/**
	* Description: Returns completet statisic about a big xyz-space. Therefor the results are including estimated values to reduce
	*		the runtime of the query.
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the XYZ-space (tablename)
	*   @tablesamplecnt	- define the value which get further used in the tablesample statement. If it is null full table scans will be performed.
	*
	* Returns (table):
	*   tabelsize		- storage size of space
	*   geometrytypes	- list of geometry-types which are present
	*   properties		- list of available properties and their counts
	*   tags			- number of tags found in space
	*   count			- number of records found in space
	*   bbox			- bbox in which the space objects are located
	*   searchable		- ALL | PARTIAL
	*/

	BEGIN
		RETURN QUERY EXECUTE
			'SELECT	format(''{"value": %s, "estimated" : true}'', tablesize)::jsonb as tablesize,  '
			||'	format(''{"value": %s, "estimated" : true}'', COALESCE(geometrytypes,''[]''))::jsonb as geometrytypes,  '
			||'	format(''{"value": %s, "estimated" : true}'', COALESCE((prop->''properties''),''[]''))::jsonb, '
			||'	format(''{"value": %s, "estimated" : true}'', COALESCE(tags,''[]''))::jsonb as tags,  '
			||'	format(''{"value": %s, "estimated" : true}'', count)::jsonb as count,  '
			||'	format(''{"value": "%s", "estimated" : true}'', bbox)::jsonb as bbox,  '
			||'	prop->>''searchable'' as searchable  FROM ('
			||'	SELECT pg_total_relation_size('''||schema||'."'||spaceid||'"'') AS tablesize, '
			||'	(SELECT jsonb_agg(type) as geometryTypes from ('
			||'		SELECT distinct xyz_geotype(geo) as type '
			||'			FROM "'||schema||'"."'||spaceid||'" TABLESAMPLE SYSTEM_ROWS('||tablesamplecnt||') '
			||'		) geo_type '
			||'	), '
			||'	(select format(''{"searchable": "%s", "properties" : %s}'',searchable, COALESCE (properties,''[]''))::jsonb '
			||'		 from ( '
			||'			select jsonb_agg( '
			||'				case when t = 0 OR t = c then  '
			||'					( select row_to_json( prop ) from ( select key, count, datatype ) prop )  '
			||'				else  '
			||'					( select row_to_json( prop ) from ( select key, count, searchable, datatype ) prop )  '
			||'				end  '
			||'			) as properties, '
			||'			xyz_statistic_searchable(jsonb_agg( (select row_to_json( prop ) from ( select key, count, searchable, datatype ) prop ) ) )as searchable '
			||'			from (  '
			||'				select *, (sum( searchable::integer ) over ()) t, count(1) over() c  '
			||'				from (  '
			||'					select key,count, searchable, datatype '
			||'						FROM xyz_property_statistic_v2('''||schema||''','''||spaceid||''', '||tablesamplecnt||')  '
			||'				) oo  '
			||'			) ooo '
			||'		)d '
			||'	) as prop, '
			||'	(SELECT COALESCE(jsonb_agg(tag_stat), ''[]''::jsonb) as tags '
			||'		FROM ( '
			||'			select * FROM xyz_tag_statistic('''||schema||''','''||spaceid||''', '||tablesamplecnt||') '
			||'		) as tag_stat '
			||'	),'
			||'	reltuples AS count, '
			||' (SELECT xyz_space_bbox('''||schema||''','''||spaceid||''', '||tablesamplecnt||')) AS bbox '
			||'		FROM pg_class '
			||'	WHERE oid='''||schema||'."'||spaceid||'"''::regclass) A';
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_statistic_space(text, text)
-- DROP FUNCTION xyz_statistic_space(text, text);
CREATE OR REPLACE FUNCTION xyz_statistic_space(
    IN schema text,
    IN spaceid text)
  RETURNS TABLE(tablesize jsonb, geometrytypes jsonb, properties jsonb, tags jsonb, count jsonb, bbox jsonb, searchable text) AS
$BODY$
	/**
	* Description: Returns complete statistic about a XYZ-space. The thresholds for small and big tables are defined here.
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the space (tablename)
	*
	* Returns (table):
	*   tabelsize		- storage size of space
	*   geometrytypes	- list of geometrytypes which are present
	*   properties		- list of available properties and their counts
	*   tags			- number of tags found in space
	*   count			- number of records found in space
	*   bbox			- bbox in which the space objects are located
	*	searchable		- ALL | PARTIAL
	*/

	/**  Defines how much records a big table has */
	DECLARE big_space_threshold integer := 10000;

	/** Defines the value for the tablesample statement */
	DECLARE tablesamplecnt integer := 1000;

	/** used for big-spaces and get filled via pg_class */
	DECLARE estimate_cnt bigint;

	BEGIN
		SELECT reltuples into estimate_cnt FROM pg_class WHERE oid = concat('"',$1, '"."', $2, '"')::regclass;

		IF estimate_cnt > big_space_threshold THEN
			RETURN QUERY EXECUTE 'select * from xyz_statistic_xl_space('''||schema||''', '''||spaceid||''' , '||tablesamplecnt||')';
		ELSE
			RETURN QUERY EXECUTE 'select * from xyz_statistic_xs_space('''||schema||''','''||spaceid||''')';
		END IF;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_statistic_xs_space(text, text)
-- DROP FUNCTION xyz_statistic_xs_space(text, text);
CREATE OR REPLACE FUNCTION xyz_statistic_xs_space(
    IN schema text,
    IN spaceid text)
  RETURNS TABLE(tablesize jsonb, geometrytypes jsonb, properties jsonb, tags jsonb, count jsonb, bbox jsonb, searchable text) AS
$BODY$
	/**
	* Description: Returns complete statistic about a small XYZ-space. It should not get used to analyze big spaces because
	*	full table scans are getting performed.
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the XYZ-space (tablename)
	*
	* Returns (table):
	*   tabelsize		- storage size of space
	*   geometrytypes	- list of geometrytypes which are present
	*   properties		- list of available properties and their counts
	*   tags			- number of tags found in space
	*   count			- number of records found in space
	*   bbox			- bbox in which the space objects are located
	*   estimated		- true if values are estimated
	*   searchable		- ALL
	*/

	BEGIN
		RETURN QUERY EXECUTE
			'SELECT	format(''{"value": %s, "estimated" : true}'', tablesize)::jsonb as tablesize,  '
			||'	format(''{"value": %s, "estimated" : false}'', COALESCE(geometrytypes,''[]''))::jsonb as geometrytypes, '
			||'	format(''{"value": %s, "estimated" : false}'', COALESCE(properties,''[]''))::jsonb, '
			||'	format(''{"value": %s, "estimated" : false}'', COALESCE(tags,''[]''))::jsonb as tags,  '
			||'	format(''{"value": %s, "estimated" : false}'', count)::jsonb as count,  '
			||'	format(''{"value": "%s", "estimated" : false}'', bbox)::jsonb as bbox,  '
			||'	''ALL''::text AS searchable  FROM ('
			||'		SELECT pg_total_relation_size('''||schema||'."'||spaceid||'"'') AS tablesize, '
			||'		(SELECT jsonb_agg(type) as geometryTypes from ( '
			||'			SELECT distinct xyz_geotype(geo) as type '
			||'				FROM "'||schema||'"."'||spaceid||'" '
			||'			) geo_type '
			||'		),'
			||'		(SELECT COALESCE(jsonb_agg(prop_stat), ''[]''::jsonb) as properties '
			||'			FROM ( '
			||'				select key,count,datatype FROM xyz_property_statistic_v2('''||schema||''','''||spaceid||''', null) '
			||'			)as prop_stat '
			||'		), '
			||'		(SELECT COALESCE(jsonb_agg(tag_stat), ''[]''::jsonb) as tags '
			||'			FROM ( '
			||'				select * FROM xyz_tag_statistic('''||schema||''','''||spaceid||''', null) '
			||'			) as tag_stat '
			||'		), '
			||'		(SELECT count(*) FROM "'||schema||'"."'||spaceid||'") AS count, '
			||'		(SELECT xyz_space_bbox('''||schema||''','''||spaceid||''', null)) AS bbox '
			||'			FROM pg_class '
			||'		WHERE oid='''||schema||'."'||spaceid||'"''::regclass) A';
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_create_idxs_for_space(text, text)
-- DROP FUNCTION xyz_create_idxs_for_space(text, text);
CREATE OR REPLACE FUNCTION xyz_create_idxs_for_space(
    schema text,
    space text)
  RETURNS void AS
$BODY$
	/**
	* Description: Creates missing Indices on a XYZ-Space based on the index-proposals written down in the xyz_config.xyz_idxs_status maintenance table.
	*	Only used manually in case of errors.
	*
	* Parameters:
	*   @schema			- schema in which the XYZ-spaces are located
	*   @spaceid		- id of the XYZ-space (tablename)
	*/

	DECLARE xyz_space_stat record;
	DECLARE xyz_idx_proposal record;

	BEGIN
		FOR xyz_space_stat IN
			SELECT * FROM xyz_config.xyz_idxs_status
				WHERE idx_proposals IS NOT NULL
					AND idx_creation_finished = false
					AND count >= 10000
					AND spaceid = space
		LOOP
			RAISE NOTICE 'CREATE IDX FOR: % - PROPOSALS ARE: %',xyz_space_stat.spaceid,xyz_space_stat.idx_proposals;

			/** set indication that idx creation is running */
			UPDATE xyz_config.xyz_idxs_status
				SET idx_creation_finished = false
					WHERE spaceid = xyz_space_stat.spaceid;

			FOR xyz_idx_proposal IN
				SELECT value->>'property' as property, value->>'type' as type from jsonb_array_elements(xyz_space_stat.idx_proposals)
			LOOP
				RAISE NOTICE '---PROPERTY: % - TYPE: %',xyz_idx_proposal.property, xyz_idx_proposal.type;

				BEGIN
					PERFORM xyz_index_creation_on_property(schema,xyz_space_stat.spaceid, xyz_idx_proposal.property,'a');

					EXCEPTION WHEN OTHERS THEN
						RAISE NOTICE '---IDX FOR PROPERTY % already exists on %s - ABBORT!',xyz_space_stat.spaceid,xyz_idx_proposal.property;
				END;
			END LOOP;

			/** set indication that idx creation is finished */
			UPDATE xyz_config.xyz_idxs_status
				SET idx_creation_finished = true,
					idx_proposals = null,
				    idx_available = (select jsonb_agg(FORMAT('{"property":"%s","src":"%s"}',idx_property, src)::jsonb) from (
					select * from xyz_index_list_all_available(schema,xyz_space_stat.spaceid)
						order by idx_property
				)b )
				WHERE spaceid = xyz_space_stat.spaceid;
		END LOOP;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- Function: xyz_remove_unnecessary_idx(text, integer)
-- DROP FUNCTION xyz_remove_unnecessary_idx(text, integer);
CREATE OR REPLACE FUNCTION xyz_remove_unnecessary_idx(
    schema text,
    min_table_count integer)
  RETURNS void AS
$BODY$
	/**
	* Description: Deletes unwanted Indices from XYZ-Space. If a space has less then min_table_count objects, it should not have Indices.
	*	All properties can be used for searches in a small space. So we can delete the existing ones.
	*	Currently only manually used.
	*
	* Parameters:
	*   @schema				- schema in which the XYZ-spaces are located
	* 	@min_table_count	- defines the count of rows a table have to has to get included.
	*/

	DECLARE xyz_space_list record;
	DECLARE xyz_idx text;

	BEGIN
		FOR xyz_space_list IN
			select spaceid, idx, count from(
				select spaceid,count,(select array_agg(idx_name) from xyz_index_list_all_available(schema,spaceid) where src='a') as idx
					FROM (
					select spaceid,count from xyz_config.xyz_idxs_status where count < min_table_count
				)A
			)B where idx IS NOT null ORDER BY count
		LOOP
			RAISE NOTICE 'DELETE IDX FROM: % [%]',xyz_space_list.spaceid , xyz_space_list.count;

			FOREACH xyz_idx IN ARRAY xyz_space_list.idx
			LOOP
				RAISE NOTICE '-- DROP INDEX   %s."%" ', schema, xyz_idx;
				EXECUTE FORMAT ('DROP INDEX IF EXISTS %s."%s" ', schema, xyz_idx);
			END LOOP;

			RAISE NOTICE '- DELETE SPACE ENTRY FORM xyz_idxs_status TABLE: % ',xyz_space_list.spaceid;
			EXECUTE FORMAT ('DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid=''%s''', xyz_space_list.spaceid);
		END LOOP;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
------------------------------------------------
------ ADD property projection functions
CREATE OR REPLACE FUNCTION _prj_jsonb_each(jdoc jsonb)
RETURNS TABLE( key text, value jsonb) AS
$body$
declare
begin
 if ( jsonb_typeof( jdoc ) = 'object' ) then
  return query select jsonb_each.key, jsonb_each.value from jsonb_each( jdoc );
 elseif ( jsonb_typeof( jdoc ) = 'array' ) then
  return query select null::text as key, jsonb_array_elements.value from jsonb_array_elements( jdoc );
 else
  return query select null::text as key, jdoc as value;
 end if;
end
$body$
language plpgsql immutable;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION prj_flatten(jdoc jsonb)
RETURNS TABLE(level integer, jkey text, jval jsonb) AS
$body$
with recursive searchobj( level, jkey, jval ) as
(
  select 0::integer as level, key as jkey, value as jval from jsonb_each( jdoc )
 union all
  select i.level + 1 as level, i.jkey || '.' || coalesce(key, ((row_number() over ( partition by i.jkey ) ) - 1)::text ), i.value as jval
  from
  (  select level, jkey, (_prj_jsonb_each( jval )).*
     from searchobj
     where 1 = 1
      and jsonb_typeof( jval ) in ( 'object', 'array' )
      and level < 100
   ) i
)
select level, jkey, jval from searchobj
where 1 = 1
  and jsonb_typeof( jval ) in ( 'string', 'number', 'boolean', 'null' )
$body$
LANGUAGE sql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION prj_input_validate(plist text[])
RETURNS text[] AS
$body$
select array_agg( i.jpth ) from
( with t1 as ( select distinct unnest( plist ) jpth )
  select l.jpth from t1 l join t1 r on ( strpos( l.jpth, r.jpth ) = 1 )
  group by 1 having count(1) = 1
) i
$body$
LANGUAGE sql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION prj_rebuild_elem( jpath text, jval jsonb )
RETURNS jsonb AS
$body$
 select
  ( string_agg( case when ia then '[' else format('{"%s":',fn) end,'' ) || jval::text || reverse( string_agg( case when ia then ']' else '}' end,'' ) ) )::jsonb
 from
  ( select fn, ( fn ~ '^\d+$' ) as ia  from regexp_split_to_table( jpath, '\.') fn ) i
$body$
LANGUAGE sql IMMUTABLE;
------------------------------------------------
------------------------------------------------
create or replace function prj_jmerge(CurrentData jsonb,newData jsonb)
 returns jsonb as
$body$
 select case jsonb_typeof(CurrentData)
   when 'object' then
    case jsonb_typeof(newData)
     when 'object' then (
       select jsonb_object_agg(k,
                  case
                   when e2.v is null then e1.v
                   when e1.v is null then e2.v
                   when e1.v = e2.v then e1.v
                   else prj_jmerge(e1.v, e2.v)
                 end )
       from      jsonb_each(CurrentData) e1(k, v)
       full join jsonb_each(newData) e2(k, v) using (k)
     )
     else newData
    end
   when 'array' then CurrentData || newData
   else newData
 end
$body$
language sql immutable;
------------------------------------------------
------------------------------------------------
create or replace function prj_build( jpaths text[], indata jsonb )
returns jsonb as
$body$
declare
 rval jsonb := '{}'::jsonb;
 r jsonb;
 jpath text;
 jpatharr text[];
begin

 jpaths = prj_input_validate( jpaths );

 foreach jpath in array jpaths
 loop
  jpatharr = regexp_split_to_array( jpath, '\.');
  r = ( indata#> jpatharr );

  if( r notnull ) then
   if ( cardinality( jpatharr ) = 1 ) then
    rval := jsonb_set( rval, jpatharr, r );
   else
    rval := prj_jmerge( rval, prj_rebuild_elem( jpath, r ));
   end if;
  end if;
 end loop;

 return rval;
end
$body$
language plpgsql immutable;
------------------------------------------------
---------------- QUADKEY_FUNCTIONS -------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_point2lrc( geo geometry(Point,4326), lev integer )
	RETURNS TABLE(rowY integer, colX integer, level integer) AS $$
DECLARE
    longitude   numeric := ST_X( geo );
    latitude    numeric := ST_Y( geo );
    sinLatitude numeric;
    numRowsCols constant integer := 1 << lev;
BEGIN

  sinLatitude = sin( latitude * pi() / 180.0 );

  colX := floor(((longitude + 180.0) / 360.0) * numRowsCols);
  rowY := floor((0.5 - ln((1 + sinLatitude) / (1 - sinLatitude)) / (4 * pi())) * numRowsCols );
  level := lev;

  RETURN next;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_lrc2qk(rowY integer, colX integer, level integer )  RETURNS text AS $$
DECLARE
    qk text := '';
    digit integer;
    digits char(1)[] = array[ '0', '1', '2', '3' ];
    mask bit(32);
BEGIN

  for i in reverse level .. 1 LOOP
   digit = 1;
   mask = 1::bit(32) << ( i - 1 );

   if (colX::bit(32) & mask) <> 0::bit(32) then
    digit = digit + 1;
   end if;

   if (rowY::bit(32) & mask) <> 0::bit(32) then
    digit = digit + 2;
   end if;

   qk = qk || digits[ digit ];

  end loop;

  return qk;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_qk2lrc( qid text )
	RETURNS TABLE(rowY integer, colX integer, level integer) AS $$
BEGIN
 level = length(qid);
 rowY = 0;
 colX = 0;

 for i in 1 .. level loop
  colX = colX << 1;
  rowY = rowY << 1;

  case substr( qid, i, 1 )
	   when '0' then null; -- nop
	   when '1' then colX = colX + 1;
	   when '2' then rowY = rowY + 1;
	   when '3' then colX = colX + 1; rowY = rowY + 1;
   end case;
 end loop;

 RETURN next;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_lrc2bbox(rowY integer, colX integer, level integer)
	RETURNS geometry AS $$
DECLARE
 numRowsCols constant integer := 1 << level;
 tileSize    constant numeric := 2.0 * pi() / numRowsCols;

 RAD_TO_WGS84 constant numeric := 180. / pi();

 maxX numeric;
 maxY numeric;
 minX numeric;
 minY numeric;

BEGIN

  maxX = ((-pi()) + tileSize * (colX + 1)) * RAD_TO_WGS84;
  minX = ((-pi()) + tileSize * colX) * RAD_TO_WGS84;
  maxY = pi() - tileSize * rowY;
  minY = pi() - tileSize * (rowY + 1);

  maxY = atan( (exp(maxY) - exp(-maxY)) / 2  ) * RAD_TO_WGS84;
  minY = atan( (exp(minY) - exp(-minY)) / 2  ) * RAD_TO_WGS84;

  return ST_MakeEnvelope( minX, minY, maxX, maxY, 4326 );

END;
$$ LANGUAGE plpgsql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_qk2bbox( qid text )
	RETURNS geometry AS $$
DECLARE
	geo geometry;
BEGIN
	select into geo xyz_qk_lrc2bbox(rowY,colX,level) from(
		select rowY,colX,level from xyz_qk_qk2lrc( qid )
	)A;
 return geo;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_point2qk( geo geometry(Point,4326), level integer )
	RETURNS text AS $$
DECLARE
	xyzTile record;
BEGIN
	select * from xyz_qk_point2lrc( geo, level ) into xyzTile;
 return xyz_qk_lrc2qk(xyzTile.rowY, xyzTile.colX, level);
END;
$$ LANGUAGE plpgsql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_bbox2zooml( geometry )
  RETURNS integer AS
$body$ -- select round( ( ln( 360 ) - ln( st_xmax(i.env) - st_xmin(i.env) )  )/ ln(2) )::integer as zm
 select round( ( 5.88610403145016 - ln( st_xmax(i.env) - st_xmin(i.env) )  )/ 0.693147180559945 )::integer as zm
 from ( select st_envelope( $1 ) as env ) i
$body$
LANGUAGE sql IMMUTABLE;
------------------------------------------------
------ftm - fast tile mode ------------------------------------------
CREATE OR REPLACE FUNCTION ftm_SimplifyPreserveTopology( geo geometry, tolerance float)
  RETURNS geometry AS
$BODY$
 select case ST_NPoints( geo ) < 20 when true then geo else st_simplifypreservetopology( geo, tolerance ) end
$BODY$
  LANGUAGE sql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION ftm_Simplify( geo geometry, tolerance float)
  RETURNS geometry AS
$BODY$
 select case ST_NPoints( geo ) < 20 when true then geo else (select case st_issimple( i.g ) when true then i.g else null end from ( select st_simplify( geo, tolerance,false ) as g ) i ) end
$BODY$
  LANGUAGE sql IMMUTABLE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_postgis_selectivity( tbl regclass, att_name text, geom geometry )
 returns double precision
language 'plpgsql'
cost 1
volatile strict parallel safe
as
$body$
declare
begin
 return _postgis_selectivity( tbl, att_name, geom );
 exception when others then
  return 0.0;
end;
$body$;
------------------------------------------------
------------------------------------------------
