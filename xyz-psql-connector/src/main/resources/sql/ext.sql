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
------------------------------------------------
------------------------------------------------
--
-- used by xyz-psql-connector\src\main\java\com\here\xyz\psql\DatabaseWriter.java
-- and will be obsolet with removal of java code
--
-- xyz_write_versioned_modification_operation
-- xyz_simple_upsert
-- xyz_simple_update
-- xyz_simple_delete
-- xyz_simple_conflictCheck
-- xyz_geoFromWkb
--

CREATE OR REPLACE FUNCTION xyz_reduce_precision(geo GEOMETRY, enable_logging boolean = TRUE)
    RETURNS GEOMETRY AS
$BODY$
DECLARE
 sgeo geometry;
BEGIN

  if not st_isvalid(geo) then
   RETURN geo;
  end if;

  sgeo := st_geomfromtext(st_astext(ST_SnapToGrid(geo, 0.00000001),8),4326); -- ST_ReducePrecision(geo, 0.00000001);

  IF GeometryType(sgeo) = GeometryType(geo) THEN
   RETURN sgeo;  -- only if type did not changed
  ELSE
   RETURN geo;
  END IF;

  EXCEPTION WHEN OTHERS THEN
    IF enable_logging THEN
        RAISE WARNING 'xyz_reduce_precision: Invalid geometry detected: %',ST_AsGeoJson(geo);
    END IF;

  RETURN geo;

END
$BODY$
LANGUAGE plpgsql immutable parallel safe;
------------------------------------------------
------------------------------------------------
CREATE OR  REPLACE FUNCTION xyz_random_string(length integer)
    RETURNS text AS
$BODY$
    DECLARE
        chars text[] := '{0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z}';
        result text := '';
        i integer := 0;
    BEGIN
            IF length < 0 THEN
                RAISE EXCEPTION 'Given length cannot be less than 0';
        END IF;
        FOR i IN 1..length LOOP
            result := result || chars[1+random()*(array_length(chars, 1)-1)];
        END LOOP;

        RETURN result;
    END;
$BODY$
LANGUAGE plpgsql VOLATILE PARALLEL RESTRICTED;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
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
    LANGUAGE plpgsql VOLATILE PARALLEL RESTRICTED;
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
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
create or replace function xyz_qk_envelope2lrc(geo geometry, lvl integer)
    returns table(rowy integer, colx integer, level integer) as
$body$
declare
    ibox geometry := st_envelope( geo );
	topLeft geometry := st_setsrid( st_point( st_xmin(ibox), st_ymax(ibox)),4326);
	botright geometry := st_setsrid( st_point( st_xmax(ibox), st_ymin(ibox) ),4326);
    numrowscols constant integer := 1 << lvl;
	tl record;
	br record;
    bColX integer := 0;
	saveCounter integer := 0;
begin
 level = lvl;
 tl = xyz_qk_point2lrc(topLeft,lvl);
 br = xyz_qk_point2lrc(botright,lvl);
 bColX = br.colx;

 if( br.colx < tl.colx ) then
  bColX = br.colx + numrowscols;
 end if;
 for i in tl.rowy .. br.rowy loop
  for j in tl.colx .. bColX loop
   rowy = i;
   colx = (j % numrowscols);
   saveCounter = saveCounter + 1;

   if( saveCounter < 10000 ) then
    return next;
   else
    return;
   end if;

  end loop;
 end loop;

end;
$body$
language plpgsql immutable parallel safe;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql VOLATILE PARALLEL RESTRICTED;
------------------------------------------------
------------------------------------------------
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
	    spaceid := xyz_get_root_table(spaceid);
		FOR av_idx_list IN
			   SELECT indexname, substring(indexname from char_length(spaceid) + 6) as idx_on, COALESCE((SELECT source from xyz_index_name_dissolve_to_property(indexname,spaceid)),'s') as source
				FROM pg_indexes
					WHERE
				schemaname = ''||schema||'' AND tablename = ''||spaceid||'' AND starts_with(indexname, 'idx_')
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
  LANGUAGE plpgsql STABLE PARALLEL RESTRICTED;
 ------------------------------------------------
------------------------------------------------
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
			END IF;
		END LOOP;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_index_creation_on_property_object(
    schema text,
    spaceid text,
    propkey text,
    idx_name text,
    datatype text,
    source character,
    target_column text DEFAULT 'jsondata')
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
        root_path text := CASE WHEN target_column = 'jsondata'
                            THEN '''properties''->'
                          ELSE ''
                          END;
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
            EXECUTE format('CREATE INDEX IF NOT EXISTS "%s" '
                ||'ON %s."%s" '
                ||' USING btree '
                ||' (GeometryType(geo))', idx_name, schema, spaceid, idx_type);
        ELSE
            EXECUTE format('CREATE INDEX IF NOT EXISTS "%s" '
                ||'ON %s."%s" '
                ||' USING %s '
                ||'((%s->%s %s))', idx_name, schema, spaceid, idx_type, target_column, root_path, prop_path);
        END IF;

		EXECUTE format('COMMENT ON INDEX %s."%s" '
				||'IS ''p.name=%s''',
			schema, idx_name, xyz_index_get_plain_propkey(propkey));

		RETURN idx_name;
	END
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_property_datatype(
    schema text,
    spaceid text,
    propertypath text,
    tablesamplecnt integer,
    target_column text DEFAULT 'jsondata'
)
RETURNS TEXT AS
$BODY$
/**
 * Description:
 *   Tries to detect the data type of a jsonkey. {"foo" : "bar"} => "bar"=string
 *
 * Parameters:
 *   @schema          - schema name
 *   @spaceid         - id of the XYZ-space (tablename)
 *   @propertypath    - path down to the json-field (e.g.: jsondata.foo.bar)
 *   @tablesamplecnt  - define the value which get further used in the tablesample statement. If it is null full table scans will be performed.
 *   @target_column   - column name to inspect (default: 'jsondata').
 *                      If 'jsondata' → function looks inside ->'properties'.
 *                      Otherwise → function looks directly into that column.
 *
 * Returns:
 *   datatype         - string | number | boolean | object | array | null | unknown
 */

DECLARE
    datatype TEXT := xyz_index_dissolve_datatype(propertypath);
    json_proppath TEXT;
    column_expr TEXT;
BEGIN
    IF (datatype IS NOT NULL) THEN
        RETURN datatype;
    END IF;

    SELECT xyz_property_path(propertypath) INTO json_proppath;

    -- Decide whether to append ->'properties' or not
    IF target_column = 'jsondata' THEN
        column_expr := format('%I->''properties''->%s', target_column, json_proppath);
    ELSE
        column_expr := format('%I->%s', target_column, json_proppath);
    END IF;

    EXECUTE format(
        'SELECT jsonb_typeof((%s)::jsonb)::text
            FROM %I.%I TABLESAMPLE SYSTEM_ROWS(%s)
         WHERE %s IS NOT NULL
            LIMIT 1',
        column_expr, schema, spaceid, tablesamplecnt, column_expr
    )
    INTO datatype;

    IF datatype IS NULL THEN
        RETURN 'unknown';
    END IF;

    RETURN datatype;

EXCEPTION WHEN OTHERS THEN
    RETURN 'unknown';
END;
$BODY$
LANGUAGE plpgsql VOLATILE PARALLEL RESTRICTED;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_property_statistic_v2(
    IN schema text,
    IN spaceid text,
    IN tablesamplecnt integer)
  RETURNS TABLE(key text, count bigint, searchable boolean, datatype text) AS
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
					|| '		(SELECT COALESCE(COUNT(*)::numeric::BIGINT , 0) as count '
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
					|| '		COALESCE(COUNT(*)::numeric::BIGINT, 0) as count, '
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
				|| '		(SELECT TRUNC(((COUNT(*)/1000::real) * '||estimate_cnt||')::numeric, 0)::BIGINT as count '
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
				|| '		TRUNC(((COUNT(*)/1000::real) * '||estimate_cnt||')::numeric, 0)::BIGINT as count, '
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
CREATE OR REPLACE FUNCTION xyz_is_space_table(schema TEXT, tableName TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    -- TODO: Improve this function to not rely on the table's name anymore
    IF length(tableName) < 6 THEN
        RETURN TRUE;
    END IF;
    RETURN tableName != 'spatial_ref_sys' AND -- It's the spatial reference system table fro postGIS (system table)
           substring(tableName, length(tableName) - 4) != '_head' AND -- It's a HEAD partition
           tableName  !~ '.+\_p[0-9]'; -- It's a history partition
END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
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
			EXECUTE format('SELECT tablesize, geometrytypes, properties, tags, count, bbox from xyz_statistic_space(''%s'',''%s'', false)',schema , xyz_spaces.spaceid)
				INTO tablesize, geometrytypes, properties, tags, count, bbox;
			RETURN NEXT;
		END IF;
	END LOOP;
	END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql VOLATILE PARALLEL RESTRICTED;
------------------------------------------------
------------------------------------------------
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

		/** TODO: CREATE INDEX CONCURRENTLY - Use asyncify()! */
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
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql STABLE PARALLEL RESTRICTED;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql STABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
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
  LANGUAGE plpgsql IMMUTABLE;
------------------------------------------------
------------------------------------------------
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

CREATE OR REPLACE FUNCTION xyz_statistic_space_v1(
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
		SELECT reltuples into estimate_cnt FROM pg_class WHERE oid = ( select concat('"',$1, '"."', xyz_get_head_table(schema, spaceid), '"')::regclass);

		IF estimate_cnt > big_space_threshold THEN
			RETURN QUERY EXECUTE 'select * from xyz_statistic_xl_space('''||schema||''', ''' || xyz_get_head_table(schema, spaceid) || ''' , '||tablesamplecnt||')';
		ELSE
			RETURN QUERY EXECUTE 'select * from xyz_statistic_xs_space('''||schema||''',''' || xyz_get_head_table(schema, spaceid) || ''')';
		END IF;
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION xyz_statistic_space_v2(
    IN schema text,
    IN spaceid text)
  RETURNS TABLE(tablesize jsonb, geometrytypes jsonb, properties jsonb, tags jsonb, count jsonb, bbox jsonb, searchable text) AS
$BODY$

with indata as ( select xyz_statistic_space_v2.schema as s, xyz_statistic_space_v2.spaceid as t ),
--with indata as ( select 'public' as s, 'exportTestdata03' as t ),
--with indata as ( select 'public' as s, 'x-psql-test' as t ),
iindata as
( select row_number() over () as idx, r.* from
	(	select i.s as schem, unnest( array_remove( array[i.t, m.meta#>>'{extends,intermediateTable}', m.meta#>>'{extends,extendedTable}'], null ) ) as tbl from xyz_config.space_meta m right join indata i on ( m.schem = i.s and m.h_id = regexp_replace( i.t, '_head$', '' ))  ) r
),
iiindata as ( select * from iindata i, lateral ( select * from xyz_statistic_space_v1(i.schem,i.tbl) ) l ),
c1 as ( select jsonb_build_object( 'value', sum( (tablesize->'value')::bigint ), 'estimated', max( (tablesize->>'estimated') )::boolean) as tablesize from iiindata ),
c2 as
( select jsonb_build_object( 'value', coalesce( jsonb_agg( distinct e1 ),'[]'::jsonb ), 'estimated', coalesce( max( e2::text )::boolean, false ) ) as geometrytypes
  from ( select jsonb_array_elements( geometrytypes->'value' ) as e1, geometrytypes->'estimated' as e2 from iiindata ) o
),
c3 as
( select jsonb_build_object('value',coalesce( jsonb_agg( v ),'[]'::jsonb ),'estimated', coalesce( max( e::text )::boolean, false ) ) as properties
  from
  ( select jsonb_build_object('key',e1->>'key','count', sum( (e1->'count')::bigint ),'datatype', max( e1->>'datatype' )) || case when max( e1->>'searchable' ) isnull then '{}'::jsonb else jsonb_build_object( 'searchable', max( e1->>'searchable' )::boolean ) end as v, max( e2::text ) as e
    from ( select jsonb_array_elements( properties->'value' ) as e1, properties->'estimated' as e2 from iiindata ) o
    group by o.e1->>'key'
  ) oo
),
c4 as
( select jsonb_build_object( 'value', coalesce( jsonb_agg( distinct e1 ),'[]'::jsonb ), 'estimated', coalesce( max( e2::text )::boolean, true ) ) as tags
  from ( select jsonb_array_elements( tags->'value' ) as e1, tags->'estimated' as e2 from iiindata ) oo
),
c5 as
(	select jsonb_build_object( 'value', e1 , 'estimated', e2 ) as count
  from
  ( select  sum((count->>'value')::bigint)::bigint as e1, max( count->>'estimated' )::boolean as e2 from iiindata ) oo
),
c6 as
( select jsonb_build_object( 'value', coalesce(e1,'') , 'estimated', coalesce(e2,false) ) as bbox
  from ( select  st_extent( (bbox->>'value')::box2d::geometry )::text as e1, max( bbox->>'estimated' )::boolean as e2 from iiindata where strpos(bbox->>'value','BOX(') > 0 ) oo
),
c7 as ( select max( searchable ) as searchable from iiindata ),
outdata as ( select * from c1,c2,c3,c4,c5,c6,c7 )
select * from outdata
--select * from iiindata
$BODY$
LANGUAGE sql VOLATILE;

create or replace function xyz_statistic_space( schema text, spaceid text, ctx_extend boolean)
  returns table(tablesize jsonb, geometrytypes jsonb, properties jsonb, tags jsonb, count jsonb, bbox jsonb, searchable text) AS
$body$
begin

 if ctx_extend then
  return query select * from xyz_statistic_space_v1( schema, spaceid );
 else
  return query select * from xyz_statistic_space_v2( schema, spaceid );
 end if;

end;
$body$
language plpgsql volatile;

------------------------------------------------
------------------------------------------------
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
			||'		(SELECT count(*) FROM "'||schema||'"."'||spaceid||'" where operation not in (''H'',''J'',''D'') ) AS count, '
			||'		(SELECT xyz_space_bbox('''||schema||''','''||spaceid||''', null)) AS bbox '
			||'			FROM pg_class '
			||'		WHERE oid='''||schema||'."'||spaceid||'"''::regclass) A';
        END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
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
language plpgsql immutable PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION _prj_flatten(jdoc jsonb, depth integer )
RETURNS TABLE(level integer, jkey text, jval jsonb) AS
$body$
with
 indata1  as ( select jdoc as jdata ),
 indata2  as ( select coalesce( depth, 100 )::integer as depth ), -- if unset, restrict leveldepth to 100, safetyreason
 outdata as
 ( with recursive searchobj( level, jkey, jval ) as
  (
    select 0::integer as level, key as jkey, value as jval
    from jsonb_each( jsonb_set('{}','{s}', (select jdata from indata1) ))
   union all
    select i.level + 1 as level, i.jkey || coalesce( '.' || key, '[' || ((row_number() over ( partition by i.jkey ) ) - 1)::text || ']' ), i.value as jval
    from
    (  select level, jkey, (_prj_jsonb_each( jval )).*
       from searchobj, indata2 i2
       where 1 = 1
       and jsonb_typeof( jval ) in ( 'object', 'array' )
       and level < i2.depth
    ) i
  )
  select level, nullif( regexp_replace(jkey,'^s\.?','' ),'') as jkey, jval from searchobj, indata2 i2
  where 1 = 1
    and
    ( ( level = i2.depth ) or ( jsonb_typeof( jval ) in ( 'string', 'number', 'boolean', 'null' ) and level < i2.depth ) )
 )
 select level, jkey, jval from outdata
$body$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION prj_flatten(jdoc jsonb)
RETURNS TABLE(level integer, jkey text, jval jsonb) AS
$body$
 select * from _prj_flatten( jdoc, 100 )
$body$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION _prj_diff( left_jdoc jsonb, right_jdoc jsonb )
RETURNS TABLE(level integer, jkey text, jval_l jsonb, jval_r jsonb ) AS
$body$
with
 inleft  as ( select level, jkey, jval from _prj_flatten( left_jdoc , 1 ) ),
 inright as ( select level, jkey, jval from _prj_flatten( right_jdoc, 1 ) ),
 outdiff as
 ( with recursive diffobj( level, jkey, jval_l, jval_r ) as
   (
     select coalesce( r.level, l.level ) as level, coalesce( l.jkey, r.jkey ) as jkey, l.jval as jval_l, r.jval as jval_r
     from inleft l full outer join inright r on ( r.jkey = l.jkey  )
     where (l.jkey isnull) or (r.jkey isnull) or ( l.jval != r.jval )
	union all
	 select ( i.level + nxt.level ) as level ,
		    case nxt.jkey ~ '^\[\d+\]$'
	         when false then  i.jkey || '.' || nxt.jkey
			 else i.jkey || nxt.jkey
		    end as jkey,  /*i.jval_l, i.jval_r,*/
		   nxt.jval_l, nxt.jval_r
	 from diffobj i,
		  lateral
		  ( select coalesce( r.level, l.level ) as level, coalesce( l.jkey, r.jkey ) as jkey, l.jval as jval_l, r.jval as jval_r
		    from _prj_flatten( i.jval_l,1) l full outer join _prj_flatten(i.jval_r,1) r on ( r.jkey = l.jkey  )
		    where (l.jkey isnull) or (r.jkey isnull) or ( l.jval != r.jval )
		  ) nxt
	 where 1 = 1
	   and jsonb_typeof( i.jval_l ) in ( 'object', 'array' )
	   and jsonb_typeof( i.jval_l ) = jsonb_typeof( i.jval_r )
   )
   select * from diffobj
   where 1 = 1
     and ( 	  (jval_l isnull)
		   or (jval_r isnull)
		   or jsonb_typeof( jval_l ) in ( 'string', 'number', 'boolean', 'null' )
		   or jsonb_typeof( jval_r ) in ( 'string', 'number', 'boolean', 'null' )
		   or (    jsonb_typeof( jval_l ) != jsonb_typeof( jval_r )
			   and jsonb_typeof( jval_l ) in ( 'object', 'array' )
			   and jsonb_typeof( jval_r ) in ( 'object', 'array' )
			  )
		 )
 )
select * from outdiff order by jkey
$body$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION prj_diff( left_jdoc jsonb, right_jdoc jsonb )
RETURNS jsonb AS
$body$
 select jsonb_agg( jsonb_build_array( jkey, jval_l, jval_r ) ) from _prj_diff( left_jdoc, right_jdoc ) d
$body$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION prj_input_validate(plist text[])
RETURNS text[] AS
$body$
select array_agg( i.jpth ) from
( with t1 as ( select distinct unnest( plist ) jpth )
  select l.jpth from t1 l join t1 r on ( l.jpth = r.jpth or strpos( l.jpth, r.jpth || '.' ) = 1 )
  group by 1 having count(1) = 1
) i
$body$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
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
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
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
language sql immutable PARALLEL SAFE;
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
language plpgsql immutable PARALLEL SAFE;
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
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
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
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
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
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_lrc2bbox(rowY integer, colX integer, level integer)
	RETURNS geometry AS
$$
 select st_transform(ST_TileEnvelope(level, colX, rowY), 4326)
$$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
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
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
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
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_qk_bbox2zooml( geometry )
  RETURNS integer AS
$body$ -- select round( ( ln( 360 ) - ln( st_xmax(i.env) - st_xmin(i.env) )  )/ ln(2) )::integer as zm
 select round( ( 5.88610403145016 - ln( st_xmax(i.env) - st_xmin(i.env) )  )/ 0.693147180559945 )::integer as zm
 from ( select st_envelope( $1 ) as env ) i
$body$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------ftm - fast tile mode ------------------------------------------
CREATE OR REPLACE FUNCTION ftm_SimplifyPreserveTopology( geo geometry, tolerance float)
  RETURNS geometry AS
$BODY$
 select case ST_NPoints( geo ) < 20 when true then geo else st_simplifypreservetopology( geo, tolerance ) end
$BODY$
  LANGUAGE sql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION ftm_Simplify( geo geometry, tolerance float)
  RETURNS geometry AS
$BODY$
 select case ST_NPoints( geo ) < 20 when true then geo else (select case st_issimple( i.g ) when true then i.g else null end from ( select st_simplify( geo, tolerance,false ) as g ) i ) end
$BODY$
  LANGUAGE sql IMMUTABLE PARALLEL SAFE;
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
CREATE OR REPLACE FUNCTION max_bigint() RETURNS BIGINT AS
$BODY$
BEGIN
    RETURN 9223372036854775807::BIGINT;
END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION operation_2_human_readable(operation CHAR) RETURNS TEXT AS
$BODY$
BEGIN
    RETURN CASE WHEN operation = 'I' OR operation = 'H' THEN 'insert' ELSE (CASE WHEN operation = 'U' OR operation = 'J' THEN 'update' ELSE 'delete' END) END;
END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_isHideOperation(operation CHAR) RETURNS BOOLEAN AS
$BODY$
BEGIN
    RETURN operation = 'H' OR operation = 'J';
END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_write_versioned_modification_operation(id TEXT, version BIGINT, operation CHAR, jsondata JSONB, geo GEOMETRY, schema TEXT, tableName TEXT, concurrencyCheck BOOLEAN, partitionSize BIGINT, versionsToKeep INT, pw TEXT, baseVersion BIGINT, minTagVersion BIGINT)
    RETURNS INTEGER AS
$BODY$
    DECLARE
        author TEXT := (jsondata->'properties'->'@ns:com:here:xyz'->>'author')::TEXT;
        updated_rows INTEGER;
        minVersion BIGINT;
    BEGIN
        -- First update the affected old version of the feature to make its next_version pointing to the new version
        IF operation != 'I' AND operation != 'H' THEN
            IF concurrencyCheck THEN
                IF baseVersion IS NULL THEN
                    RAISE EXCEPTION 'Error while trying to % feature with ID % in version %.', operation_2_human_readable(operation), id, version
                        USING HINT = 'Base version is missing. Concurrency check can not be performed.';
                END IF;

                EXECUTE
                    format('UPDATE %I.%I SET next_version = %L WHERE id = %L AND next_version = %L AND version = %L',
                           schema, tableName, version, id, max_bigint(), baseVersion);

                GET DIAGNOSTICS updated_rows = ROW_COUNT;
                IF updated_rows != 1 THEN
                    RAISE EXCEPTION 'Conflict while trying to % feature with ID % in version %.', operation_2_human_readable(operation), id, version
                        USING HINT = 'Base version ' || baseVersion::TEXT || ' is not matching the current HEAD version.',
                            ERRCODE = 'XYZ49';
                END IF;
            ELSE
                EXECUTE
                    format('UPDATE %I.%I SET next_version = %L WHERE id = %L AND next_version = %L AND version < %L',
                           schema, tableName, version, id, max_bigint(), version);

                GET DIAGNOSTICS updated_rows = ROW_COUNT;
                IF updated_rows != 1 THEN
                    -- This can only happen if the HEAD version of the feature was deleted in the table for some reason
                    RAISE EXCEPTION 'Unexpected error while trying to % feature with ID % in version %.', operation_2_human_readable(operation), id, version
                        USING HINT = 'Previous (HEAD) version of the feature is missing.',
                            ERRCODE = 'XYZ50';
                END IF;
            END IF;
        ELSE
            -- Ignore concurrency check for inserts and try to update the previous versions
            --TODO: Activate concurrency check for inserts as well
            EXECUTE
                format('UPDATE %I.%I SET next_version = %L WHERE id = %L AND next_version = %L AND version < %L',
                       schema, tableName, version, id, max_bigint(), version);
        END IF;

        -- Now actually insert the new version of the feature (NOTE: The order is important here to not violate the (id, next_version) uniqueness constraint)
        EXECUTE
            format('INSERT INTO %I.%I (id, version, operation, author, jsondata, geo) VALUES (%L, %L, %L, %L, %L, %L)',
                   schema, tableName, id, version, operation, author, jsondata, xyz_geoFromWkb(geo) );

        -- If the current history partition is nearly full, create the next one already
        IF version % partitionSize > partitionSize - 50 THEN
            EXECUTE xyz_create_history_partition(schema, tableName, (floor(version / partitionSize) + 1)::BIGINT, partitionSize);
        END IF;

        -- Delete old changesets from the history to keep only as many versions as specified through "versionsToKeep" if necessary
        IF version % 1000 = 0 THEN -- Perform the check only on every 1000th transaction
            minVersion := version - versionsToKeep + 1;
            IF minTagVersion != -1 THEN
                minVersion := least(minTagVersion, minVersion);
            END IF;
            IF minVersion > 0 THEN
                EXECUTE xyz_delete_changesets_async(schema, tableName, partitionSize, minVersion, pw);
            END IF;
        END IF;

        RETURN 1;
    END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_simple_upsert(id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo GEOMETRY, schema TEXT, tableName TEXT, concurrencyCheck BOOLEAN, uniqueConstraintExists BOOLEAN)
    RETURNS INTEGER AS
$BODY$
    DECLARE
        insertQuery TEXT;
        updated_rows INTEGER;
    BEGIN
        insertQuery = 'INSERT INTO %I.%I AS tbl (id, version, operation, author, jsondata, geo) VALUES (%L, %L, %L, %L, %L, %L)';
        IF concurrencyCheck THEN
            -- This query will throw an error in case of a conflict
            EXECUTE
                format(insertQuery,
                       schema, tableName, id, version, operation, author, jsondata, xyz_geoFromWkb(geo));
        ELSE
            IF uniqueConstraintExists THEN
                -- This query will perform an update instead of throwing an error in case of a conflict
                insertQuery = insertQuery || ' ON CONFLICT (id, next_version) DO UPDATE SET ' ||
                              'version = greatest(tbl.version, EXCLUDED.version), ' ||
                              'operation = CASE WHEN xyz_isHideOperation(EXCLUDED.operation) THEN ''J'' ELSE ''U'' END, ' ||
                              'author = EXCLUDED.author, ' ||
                              'jsondata = EXCLUDED.jsondata, ' ||
                              'geo = EXCLUDED.geo';
            END IF;

            EXECUTE
                format(insertQuery,
                       schema, tableName, id, version, operation, author, jsondata, xyz_geoFromWkb(geo));
        END IF;

        GET DIAGNOSTICS updated_rows = ROW_COUNT;
        RETURN updated_rows;
    END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_simple_update(id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo GEOMETRY, schema TEXT, tableName TEXT, concurrencyCheck BOOLEAN, baseVersion BIGINT)
    RETURNS INTEGER AS
$BODY$
BEGIN
    RETURN xyz_simple_update(id, version, operation, author,
                             jsondata, geo, schema, tableName,
                             concurrencyCheck, baseVersion, true);
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_simple_update(id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo GEOMETRY, schema TEXT, tableName TEXT, concurrencyCheck BOOLEAN, baseVersion BIGINT, raiseErrors BOOLEAN)
    RETURNS INTEGER AS
$BODY$
    DECLARE
        updated_rows INTEGER;
    BEGIN
        EXECUTE
            format('UPDATE %I.%I SET version = %L, operation = %L, author = %L, jsondata = %L, geo = %L WHERE id = %L'
                       || xyz_simple_conflictCheck(concurrencyCheck, baseVersion),
                schema, tableName, version, operation, author, jsondata, xyz_geoFromWkb(geo), id);

        GET DIAGNOSTICS updated_rows = ROW_COUNT;

        IF raiseErrors THEN
            IF concurrencyCheck THEN
                IF updated_rows != 1 THEN
                    RAISE EXCEPTION 'Conflict while trying to % feature with ID % in version %.', operation_2_human_readable(operation), id, version
                        USING HINT = 'Base version ' || CASE WHEN baseVersion IS NULL THEN '' ELSE baseVersion::TEXT END || ' is not matching the current HEAD version.',
                            ERRCODE = 'XYZ49';
                END IF;
            ELSE
                IF updated_rows != 1 THEN
                    -- This can only happen if the feature was deleted in the meantime
                    RAISE EXCEPTION 'Conflict while trying to % feature with ID % in version %.', operation_2_human_readable(operation), id, version
                        USING HINT = 'Feature was deleted in the meantime.',
                            ERRCODE = 'XYZ49';
                END IF;
            END IF;
        END IF;

        RETURN updated_rows;
    END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_simple_delete(id TEXT, schema TEXT, tableName TEXT, concurrencyCheck BOOLEAN, baseVersion BIGINT)
    RETURNS INTEGER AS
$BODY$
    DECLARE
        updated_rows INTEGER;
    BEGIN
        EXECUTE
            format('DELETE FROM %I.%I WHERE id = %L'
                       || xyz_simple_conflictCheck(concurrencyCheck, baseVersion),
                   schema, tableName, id);

        GET DIAGNOSTICS updated_rows = ROW_COUNT;

        IF concurrencyCheck AND updated_rows != 1 THEN
            RAISE EXCEPTION 'Conflict while trying to % feature with ID %.', operation_2_human_readable('D'), id
                USING HINT = 'Base version ' || CASE WHEN baseVersion IS NULL THEN '' ELSE baseVersion::TEXT END || ' is not matching the current HEAD version.',
                    ERRCODE = 'XYZ49';
        END IF;

        RETURN updated_rows;
    END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_simple_conflictCheck(concurrencyCheck BOOLEAN, baseVersion BIGINT)
    RETURNS TEXT AS
$BODY$
    BEGIN
        RETURN CASE WHEN NOT concurrencyCheck
            THEN
                ''
            ELSE
                CASE WHEN baseVersion IS NULL
                    THEN
                        format(' AND next_version = %L', max_bigint())
                    ELSE
                        format(' AND version = %L', baseVersion)
                    END
            END;
    END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_geoFromWkb(geo GEOMETRY)
    RETURNS GEOMETRY AS
$BODY$
    BEGIN
        RETURN CASE WHEN geo::geometry IS NULL THEN NULL ELSE xyz_reduce_precision( ST_Force3D(ST_GeomFromWKB(geo::BYTEA, 4326)) ) END;
    END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'load_feature_version_input') THEN
    CREATE TYPE load_feature_version_input AS (
        id          TEXT,
        version     BIGINT
    );
    END IF;
    --more types here...
END$$;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_create_history_partition(schema TEXT, rootTable TEXT, partitionNo BIGINT, partitionSize BIGINT)
    RETURNS VOID AS
$BODY$
BEGIN
    RAISE NOTICE 'Creating new history partition for %.% with partition no % ...',
        schema, rootTable, partitionNo;
    EXECUTE
        format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
            schema, (rootTable || '_p' || partitionNo), schema, rootTable,
            partitionSize * partitionNo, partitionSize * (partitionNo + 1));
    RAISE NOTICE 'Partition no % was successfully created (or existed already) for %.%.',
        partitionNo, schema, rootTable;
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_delete_changesets_async(schema TEXT, tableName TEXT, partitionSize BIGINT, minVersion BIGINT, pw TEXT)
    RETURNS VOID AS
$BODY$
BEGIN
    PERFORM asyncify('SELECT xyz_delete_changesets(''' || schema || ''', ''' || tableName || ''', ' ||  partitionSize || ', ' || minVersion || ')', pw);
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_delete_changesets(schema TEXT, tableName TEXT, partitionSize BIGINT, minVersion BIGINT) RETURNS VOID AS
$BODY$
DECLARE
    minRangeMin BIGINT;
BEGIN
    -- Get range-min value of the oldest history partition
    minRangeMin := (WITH partition_ranges as (
        SELECT substring(pg_get_expr(relpartbound, oid, true), 19) as range_expression
            FROM pg_class WHERE relname LIKE tableName || '_p%' AND relkind = 'r'
    ) SELECT min(substring(range_expression, 0, position('''' IN range_expression))::BIGINT) FROM partition_ranges);

    -- Drop according partitions
    FOR partitionNo IN (minRangeMin / partitionSize)..((minVersion + 1) / partitionSize - 1) LOOP
        EXECUTE 'DROP TABLE IF EXISTS "' || schema || '"."' || tableName || '_p' || partitionNo || '"';
    END LOOP;

    -- Purge the remainder in the new oldest partition
    EXECUTE 'DELETE FROM "' || schema || '"."' || tableName || '" WHERE next_version <= ' || minVersion;
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_advanced_delete_changesets(
	schema text,
	tablename text,
	partitionsize bigint,
	versions_to_keep bigint,
	min_tag_version bigint,
	pw text)
RETURNS void AS
$BODY$
DECLARE
    statistic RECORD;
	calculated_min_version BIGINT;
BEGIN
    execute format('select '
                   || 'meta->''minAvailableVersion'' as min_available_version, '
                   || '(meta->''userMinVersion'')::BIGINT as user_min_version, '
                   || '(select max(version) from "%1$s"."%2$s") as max_version, '
                   || '%3$L::bigint as versions_to_keep '
                   || 'from xyz_config.space_meta '
                   || 'where '
                   || 'h_id=%2$L', schema, tablename, versions_to_keep )
    INTO statistic;

    IF statistic IS NULL THEN
		-- Table not found in space_meta table - abort!
		RETURN;
    END IF;

	calculated_min_version := COALESCE(greatest(statistic.user_min_version, statistic.max_version - statistic.versions_to_keep + 1),0);

	IF min_tag_version >= 0 THEN
		-- Tag has priority. Delete nothing below the minTagVersion!
		calculated_min_version := least(min_tag_version , calculated_min_version);
    END IF;

	RAISE NOTICE 'PURGE - max_version:% min_available_version:% user_min_version:% calculated_min_version:%',
		statistic.max_version, statistic.min_available_version, statistic.user_min_version, calculated_min_version;

	IF calculated_min_version < 0 THEN
		RAISE NOTICE 'calculated_min_version is negative - ignore!';
		RETURN;
    END IF;

	IF statistic.min_available_version::BIGINT >= calculated_min_version THEN
		RAISE NOTICE 'PURGE - Requested versions are already deleted!';
		RETURN;
    END IF;

	PERFORM asyncify('SELECT xyz_delete_changesets(''' || schema || ''', ''' || tableName || ''', ' ||  partitionSize || ', ' || calculated_min_version || ')', pw);

    update xyz_config.space_meta
        set meta = meta || jsonb_build_object('minAvailableVersion', calculated_min_version)
    where h_id = tablename;
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_get_head_table(schema TEXT, tableName TEXT) RETURNS TEXT AS
$BODY$
BEGIN
    IF xyz_table_exists(schema, tableName || '_head') THEN
        RETURN tableName || '_head';
    ELSE
        RETURN tableName;
    END IF;
END
$BODY$
LANGUAGE plpgsql STABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_get_root_table(tableName TEXT) RETURNS TEXT AS
$BODY$
BEGIN
    IF tableName LIKE '%_head' THEN
        RETURN substring(tableName, 1, length(tableName) - 5);
    ELSE
        RETURN tableName;
    END IF;
END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION xyz_table_exists(schema TEXT, tableName TEXT) RETURNS BOOLEAN AS
$BODY$
DECLARE
    existsResult RECORD;
BEGIN
    EXECUTE
        format('SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = %L AND tablename = %L) AS tableExists', schema, tableName) INTO existsResult;
    RETURN existsResult.tableExists;
END
$BODY$
LANGUAGE plpgsql STABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
-- **NOTE:** This variant of the asyncify function is only to be called from JDBC
CREATE OR REPLACE FUNCTION asyncify(query TEXT, password TEXT) RETURNS VOID AS
$BODY$
BEGIN
    PERFORM set_config('xyz.password', password, false);
    PERFORM _asyncify(query, false, false);
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- **NOTE:** This variant of the asyncify function is only to be called from JDBC
CREATE OR REPLACE FUNCTION asyncify(query TEXT, password TEXT, procedureCall BOOLEAN) RETURNS VOID AS
$BODY$
BEGIN
    PERFORM set_config('xyz.password', password, false);
    PERFORM _asyncify(query, false, procedureCall);
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- **NOTE:** This variant of the asyncify function is only to be called from within other async functions (that have been called through asyncify themselves)
CREATE OR REPLACE FUNCTION asyncify(query TEXT) RETURNS VOID AS
$BODY$
BEGIN
    PERFORM _asyncify(query, true, false);
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- **NOTE:** This variant of the asyncify function is only to be called from within other async functions (that have been called through asyncify themselves)
CREATE OR REPLACE FUNCTION asyncify(query TEXT, deferAfterCommit BOOLEAN) RETURNS VOID AS
$BODY$
BEGIN
    PERFORM _asyncify(query, deferAfterCommit, false);
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- **NOTE:** This variant of the asyncify function is only to be called from within other async functions (that have been called through asyncify themselves)
CREATE OR REPLACE FUNCTION asyncify(query TEXT, deferAfterCommit BOOLEAN, procedureCall BOOLEAN) RETURNS VOID AS
$BODY$
BEGIN
    PERFORM _asyncify(query, deferAfterCommit, procedureCall);
END
$BODY$
    LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
-- **NOTE:** This variant of the asyncify function is private and should only be called by the other two variants of the asyncify function
CREATE OR REPLACE FUNCTION _asyncify(query TEXT, deferAfterCommit BOOLEAN, procedureCall BOOLEAN) RETURNS VOID AS
$BODY$
DECLARE
    password TEXT := current_setting('xyz.password');
    connectionName TEXT := xyz_random_string(10);
BEGIN
    IF deferAfterCommit THEN
        --Defer the execution (spawn-point) of the query to the end of this thread's execution, to make sure that this thread's transaction has been fully completed / committed before
        PERFORM set_config('xyz.next_thread', query, false);
    ELSE
--         PERFORM CASE WHEN ARRAY['conn'] <@ dblink_get_connections() THEN dblink_disconnect('conn') END;
--         RAISE NOTICE '~~~~~~~~~~~ Connection name %', connectionName;
        PERFORM dblink_connect(connectionName, 'host = localhost dbname = ' || current_database() || ' user = ' || CURRENT_USER || ' password = ' || password
                || ' application_name = ''' || current_setting('application_name') || '''');
--         PERFORM pg_sleep(1);
        IF strpos(query, '/*labels(') != 1 THEN
            --Attach the same labels to the recursive async call
            query = '/*labels(' || get_query_labels() || ')*/ ' || query;
        END IF;
        PERFORM dblink_send_query(connectionName, _create_asyncify_query_block(query, password, procedureCall));
        PERFORM dblink_disconnect(connectionName);
    END IF;
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
CREATE OR REPLACE FUNCTION _create_asyncify_query_block(query TEXT, password TEXT, procedureCall BOOLEAN) RETURNS TEXT AS
$BODY$
BEGIN
    IF procedureCall THEN
        RETURN $outer$
            DO
            $block$
            BEGIN
                SET client_min_messages TO ERROR;
                SET search_path = $outer$ || current_setting('search_path') || $outer$;
                PERFORM context('$outer$ || context()::TEXT ||  $outer$'::JSONB);
                PERFORM set_config('xyz.password', '$outer$ || password || $outer$', false);
                $outer$ || query || $outer$
                COMMIT;
                --Start the follow up thread if one has been registered
                PERFORM CASE WHEN current_setting('xyz.next_thread', true) IS NOT NULL THEN asyncify(current_setting('xyz.next_thread'), false) END;
            END
            $block$;
        $outer$;
    ELSE
        RETURN $block$
            SET client_min_messages TO ERROR;
            SET search_path = $block$ || current_setting('search_path') || $block$;
            SELECT context('$block$ || context()::TEXT ||  $block$'::JSONB);
            SELECT set_config('xyz.password', '$block$ || password || $block$', false);
            START TRANSACTION;
            $block$ || query || $block$;
            COMMIT;
            --Start the follow up thread if one has been registered
            SELECT CASE WHEN current_setting('xyz.next_thread', true) IS NOT NULL THEN asyncify(current_setting('xyz.next_thread'), false) END;
        $block$;
    END IF;
END
$BODY$
LANGUAGE plpgsql VOLATILE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION get_query_labels() RETURNS JSON AS
$BODY$
DECLARE
    labels JSON;
BEGIN
    SELECT substring(query, strpos(query, '/*labels(') + 9, strpos(query, ')*/') - 9 - strpos(query, '/*labels('))::JSON FROM pg_stat_activity WHERE strpos(query, '/*labels(') > 0 AND pid = pg_backend_pid() INTO labels;
    return labels;
END
$BODY$
LANGUAGE plpgsql STABLE PARALLEL SAFE;
------------------------------------------------
------------------------------------------------
CREATE OR REPLACE FUNCTION streamId() RETURNS TEXT AS
$BODY$
BEGIN
    RETURN current_setting('xyz.streamId');
EXCEPTION WHEN OTHERS THEN RETURN 'no-streamId';
END
$BODY$
    LANGUAGE plpgsql VOLATILE PARALLEL RESTRICTED;

CREATE OR REPLACE FUNCTION streamId(sid TEXT) RETURNS VOID AS
$BODY$
BEGIN
    PERFORM set_config('xyz.streamId', sid, true);
END
$BODY$
LANGUAGE plpgsql VOLATILE PARALLEL RESTRICTED;

