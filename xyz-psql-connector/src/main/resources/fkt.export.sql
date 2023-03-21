
set search_path to tmp,public,topology

CREATE OR REPLACE FUNCTION htile(qk text, isbase4encoded boolean) RETURNS TABLE(rowy integer, colx integer, lev integer, hkey bigint)
    LANGUAGE plpgsql
    AS $$
declare
	level integer;
begin     
    --if(quadkey == nu7ll || !quadkey.matches("[0123]{1,31}"))
    --  throw new IllegalArgumentException("Quadkey '"+quadkey+"' is invalid!");
	IF NOT isBase4Encoded THEN
		qk = htile_number_to_base(qk::bigint, 4);
	END IF;

    lev = length(qk);
--	RAISE NOTICE 'test % ',lev;
    hkey = htiles_convert_qk_to_longk(qk);

    /** Remove first level bit and convert to x */
    colX = htiles_utils_modify_bits(hkey & ((1 << lev * 2) - 1),'extract');
    /** Remove first level bit and convert to y */
    rowY = htiles_utils_modify_bits(hkey & ((1 << lev * 2) - 1) >> 1, 'extract');
	
	RETURN next;
end
$$;

CREATE OR REPLACE FUNCTION htile(x integer, y integer, level integer) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
begin     
	return htiles_convert_xy_to_long_key(x, y) | (1 << (level * 2));
end
$$;

CREATE OR REPLACE FUNCTION htile_bbox(rowy integer, colx integer, lev integer) RETURNS public.geometry
    LANGUAGE plpgsql
    AS $$
declare
	height float;
	width float;
	minX float;
    minY float;
    maxX float;
    maxY float;
begin     	
	IF lev = 0 THEN
		height = 180;
	ELSE
		height = 360.0 / (1 << lev);
	END IF;
	
	width =  360.0 / (1 << lev);		
	minX = width * colX - 180;
    minY = height * rowY - 90;
    maxX = width * (colX + 1) - 180;
    maxY = height * (rowY + 1) - 90;
	
--	RAISE NOTICE '% % % % ',minX,minY,maxX,maxY;
	return ST_MakeEnvelope( minX, minY, maxX, maxY );
end
$$;


CREATE OR REPLACE FUNCTION htile_number_to_base(num bigint, base integer) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
WITH RECURSIVE n(i, n, r) AS (
    SELECT -1, num, 0
  UNION ALL
    SELECT i + 1, n / base, (n % base)::INT
    FROM n
    WHERE n > 0
)
SELECT substring(string_agg(ch, ''),2)
FROM (
  SELECT CASE
           WHEN r BETWEEN 0 AND 9 THEN r::TEXT
           WHEN r BETWEEN 10 AND 35 THEN chr(ascii('a') + r - 10)
           ELSE '%'
         END ch
  FROM n
  WHERE i >= 0
  ORDER BY i DESC
) ch
$$;

CREATE OR REPLACE FUNCTION htiles_convert_qk_to_longk(qk text) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
declare
	level integer;
	k integer;
	hkey bigint;
begin     
	level = length(qk);
	hkey = 0;
	k = level -1;
	
	FOR i IN 1 .. level
	LOOP		
		-- RAISE NOTICE '% %',i,substring(qk,level-(i-1),1)::integer;
		hkey = hkey+ (substring(qk,level-(i-1),1)::integer << (i-1) * 2);		
	END LOOP;
	RETURN hkey | (1 << (level * 2));		 
end
$$;


CREATE OR REPLACE FUNCTION htiles_convert_xy_to_long_key(x integer, y integer) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
begin    
    x = htiles_utils_modify_bits(x,'interleave');
    y = htiles_utils_modify_bits(y,'interleave');
    
	return x | (y << 1);    		
end
$$;


CREATE OR REPLACE FUNCTION htiles_utils_modify_bits(num bigint, bmode text) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
declare
	magicNumbers bigint[] := ARRAY[
		6148914691236517205,	--110000101001000100100010100011010010001001000110110010100010111001000000101
		3689348814741910323,	--11001100110011001100110011001100110011001100110011001100110011
		1085102592571150095,	--111100001111000011110000111100001111000011110000111100001111
		71777214294589695,		--11111111000000001111111100000000111111110000000011111111
		281470681808895,		--111111111111111100000000000000001111111111111111
		4294967295				--11111111111111111111111111111111
	];
begin
	if bmode = 'interleave' THEN
		num = (num | (num << 16)) & magicNumbers[5];	 
		num = (num | (num << 8)) & magicNumbers[4];
		num = (num | (num << 4)) & magicNumbers[3];
		num = (num | (num << 2)) & magicNumbers[2];
		num = (num | (num << 1)) & magicNumbers[1];
	elseif bmode = 'extract' THEN   		
		num = num & magicNumbers[1];
		num = (num | (num >> 1)) & magicNumbers[2];
		num = (num | (num >> 2)) & magicNumbers[3];
		num = (num | (num >> 4)) & magicNumbers[4];
		num = (num | (num >> 8)) & magicNumbers[5];
		num = (num | (num >> 16)) & magicNumbers[6];		
	else 
		RAISE EXCEPTION 'Invalid mode - choose "interleave" or "extract"';
	end if;

	 
	-- RAISE NOTICE 'HKey %',num;
	return num;
end
$$;


CREATE OR REPLACE FUNCTION number_from_base(num text, base integer) RETURNS numeric
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
SELECT sum(exp * cn)
FROM (
  SELECT base::NUMERIC ^ (row_number() OVER () - 1) exp,
         CASE
           WHEN ch BETWEEN '0' AND '9' THEN ascii(ch) - ascii('0')
           WHEN ch BETWEEN 'a' AND 'z' THEN 10 + ascii(ch) - ascii('a')
         END cn
  FROM regexp_split_to_table(reverse(lower(num)), '') ch(ch)
) sub
$$;


CREATE OR REPLACE FUNCTION qk_inhabited(iqk text, mlevel integer, geo public.geometry) RETURNS TABLE(qk text)
    LANGUAGE plpgsql IMMUTABLE
    AS $$
declare
 bFound boolean := false;
 g geometry;
begin

 for lastDigit in 0..3 loop
  qk = iqk || lastDigit;
  
  g = xyz_qk_qk2bbox( qk );
  bFound = ( (geo && g) and st_intersects( geo, g ));

  if bFound then
   if length( qk ) >= mlevel then
    return next;
   else
    return query
     select r.qk from qk_inhabited(qk, mlevel, geo ) r;
   end if;
  end if;
 end loop;

end
$$;


CREATE OR REPLACE FUNCTION qk_inhabited_g(iqk text, mlevel integer, geo public.geometry) RETURNS TABLE(qk text, clip public.geometry)
    LANGUAGE plpgsql IMMUTABLE
    AS $$
declare
 bFound boolean := false;
 g geometry;
begin

 for lastDigit in 0..3 loop
  qk = iqk || lastDigit;
  
  g = rdr.qid2bboxl( qk );
  bFound = ( (geo && g) and st_intersects( geo, g ));

  if bFound then
   clip =  ST_MakeValid( ( select st_collect((o.r).geom) from ( select ST_Dump(  st_intersection( geo, g ) ) as r ) o where ST_GeometryType((o.r).geom ) in ( 'ST_Polygon', 'ST_MultiPolygon' ) ) );
   -- clip =  ST_MakeValid( st_buffer( st_intersection( geo, g ),0) );
   if length( qk ) >= mlevel then
    return next;
   else
    return query
     select r.qk, r.clip from qk_inhabited_g(qk, mlevel, clip ) r;
   end if;
  end if;
 end loop;

end
$$;


CREATE OR REPLACE FUNCTION htile_bbox(qk text) 
returns geometry AS 
$body$
select htile_bbox(rowy, colx, lev) from htile( qk,true )
$body$
language sql immutable strict;


CREATE OR REPLACE FUNCTION htile_s_inhabited(iqk text, mlevel integer, _tbl regclass) RETURNS TABLE(qk text)
    LANGUAGE plpgsql STABLE
    AS $_$
declare
 bFound boolean := false;
begin

 for lastDigit in 0..3 loop
  qk = iqk || lastDigit;
  
  execute format( 'select exists ( select 1 from %1$s r where st_intersects(r.geo, htile_bbox( %2$L )))', _tbl, qk ) into bFound;

  if bFound then
   if length( qk ) >= mlevel then
    return next;
   else
    return query
     select r.qk from htile_s_inhabited(qk, mlevel, _tbl ) r;
   end if;
  end if;
 end loop;

end
$_$;

CREATE OR REPLACE FUNCTION qk_s_inhabited(iqk text, mlevel integer, _tbl regclass) RETURNS TABLE(qk text)
    LANGUAGE plpgsql STABLE
    AS $_$
declare
 bFound boolean := false;
begin

 for lastDigit in 0..3 loop
  qk = iqk || lastDigit;
  
  execute format( 'select exists ( select 1 from %1$s r where st_intersects(r.geo, xyz_qk_qk2bbox( %2$L )))', _tbl, qk ) into bFound;

  if bFound then
   if length( qk ) >= mlevel then
    return next;
   else
    return query
     select r.qk from qk_s_inhabited(qk, mlevel, _tbl ) r;
   end if;
  end if;
 end loop;

end
$_$;

CREATE OR REPLACE FUNCTION qk_s_get_fc_of_tiles_v1(here_tile_qk boolean, tile_list text[], _tbl regclass, base64enc boolean, clipped boolean ) RETURNS TABLE(tile_id text, tile_content text)
    LANGUAGE plpgsql
    AS $_$
declare
 result record;
 tile text;
 fkt_qk2box text := 'xyz_qk_qk2bbox';
 plainjs text    := 'jsonb_build_object(''type'',''FeatureCollection'',''features'', jsonb_agg(feature) )::text';
 plaingeo text   := 'geo';
 feature_count bigint := 0;
begin

  if here_tile_qk then
   fkt_qk2box = 'htile_bbox';
  end if;
	
	if clipped then
	 plaingeo = 'ST_Intersection(ST_MakeValid( geo ),' || fkt_qk2box || '((%2$L)))';
	end if;
	
	if base64enc then
	 plainjs = 'regexp_replace( encode( replace(' || plainjs || ',''\'',''\\'')::bytea, ''base64'' ),''\n'','''',''g'')';
	end if;

	foreach tile in array tile_list
	loop
		begin
		 execute format(
				'SELECT %2$L,' || plainjs || ', count(1) as cnt'
				||' from( '
				||' select jsonb_set(jsondata,''{geometry}'',ST_AsGeojson(' || plaingeo ||')::jsonb) as feature'
				||'	 from %1$s '
				||'  where ST_Intersects(geo, ' || fkt_qk2box || '(%2$L))' 			
				||' ) A', _tbl, tile)
				INTO tile_id, tile_content, feature_count;
				
 		 if feature_count > 0 then
		  return next;
		 end if; 
			
		end;
	end loop;
end
$_$;

CREATE OR REPLACE FUNCTION qk_s_get_fc_of_tiles_v2(here_tile_qk boolean, tile_list text[], _tbl regclass, base64enc boolean, clipped boolean ) RETURNS TABLE(tile_id text, tile_content text)
    LANGUAGE plpgsql
    AS $_$
declare
 fkt_qk2box text := 'xyz_qk_qk2bbox';
 plainjs text    := 'jsonb_build_object(''type'',''FeatureCollection'',''features'', features )::text';
 plaingeo text   := 'geo';
begin

  if here_tile_qk then
   fkt_qk2box = 'htile_bbox';
  end if;
	
	if clipped then
	 plaingeo = 'ST_Intersection(ST_MakeValid(d.geo), bbox) as geo';
	end if;
	
	if base64enc then
	 plainjs = 'regexp_replace( encode( replace(' || plainjs || ',''\'',''\\'')::bytea, ''base64'' ),''\n'','''',''g'')';
	end if;
	
	return query execute
	 format(
          ' select qk,' || plainjs
       || ' from'
       || ' ( select qk, jsonb_agg( jsonb_set(jsondata,''{geometry}'',ST_AsGeojson( geo )::jsonb ) ) as features'
       || '   from'
       || '   ( select qk, d.jsondata,' || plaingeo
       || '     from'
       || '      unnest( %2$L::text[] ) qk,'
       || '      lateral ' || fkt_qk2box || '( qk ) bbox,'
       || '      lateral ( select jsondata, geo from %1$s d where st_intersects( d.geo, bbox ) ) d'
       || '   ) o'
       || '   group by qk'
       || ' ) oo' , _tbl, tile_list );
end
$_$;

CREATE OR REPLACE FUNCTION qk_s_get_fc_of_tiles(here_tile_qk boolean, tile_list text[], _tbl regclass, base64enc boolean, clipped boolean ) RETURNS TABLE(tile_id text, tile_content text)
LANGUAGE sql
AS $_$
 select tile_id, tile_content from qk_s_get_fc_of_tiles_v1( here_tile_qk, tile_list, _tbl, base64enc, clipped ) 
$_$;

create or replace function exp_build_sql_inhabited(htile boolean, iqk text, mlevel integer, _tbl regclass) returns table(qk text, s3sql text)
language plpgsql stable
as $_$
begin
 if not htile then
  return query 
	 with 
    indata as ( select exp_build_sql_inhabited.iqk as iqk, exp_build_sql_inhabited.mlevel as mlevel, exp_build_sql_inhabited._tbl as _tbl ),
    qks as ( select r.qk, i._tbl from indata i, qk_s_inhabited(i.iqk, i.mlevel, i._tbl ) r )
   select o.qk, format('select %1L, jsondata,geo from %2$s where st_intersects(geo, xyz_qk_qk2bbox( %1$L))',o.qk,o._tbl) as s3sql from qks o;
 else
  return query 
	 with 
    indata as ( select exp_build_sql_inhabited.iqk as iqk, exp_build_sql_inhabited.mlevel as mlevel, exp_build_sql_inhabited._tbl as _tbl ),
    qks as ( select r.qk, i._tbl from indata i, htile_s_inhabited(i.iqk, i.mlevel, i._tbl ) r )
   select o.qk, format('select %1L, jsondata,geo from %2$s where st_intersects(geo, htile_bbox( %1$L))',o.qk,o._tbl) as s3sql from qks o;
 end if;
end
$_$;

create or replace function exp_build_sql_inhabited(htile boolean, iqk text, mlevel integer, _tbl regclass, max_tiles integer) 
 returns table(qk text, mlev integer, _tble regclass, max_tls integer, bucket integer, nrbuckets integer, nrsubtiles integer, tiles_total integer, tile_list text[], s3sql text)
language plpgsql stable
as $_$
begin
 if not htile then
  return query 
	 with 
	  indata   as ( select exp_build_sql_inhabited.iqk as iqk, exp_build_sql_inhabited.mlevel as mlevel, exp_build_sql_inhabited._tbl as _tbl, exp_build_sql_inhabited.max_tiles as max_tiles ),
    --indata   as ( select '1202011320'::text as iqk, 14::integer as mlevel, 'public."56bd42b1a81b8f2753e8f2f6a235d2a8"'::regclass as _tbl, 3::integer as max_tiles ),
    ibuckets as 
    ( select rr.bucket::integer, (count(1) over ())::integer as nrbuckets, count(1)::integer as nrsubtiles, rr.tiles_total::integer , array_agg(rr.qk) as tlist
      from ( select count(1) over () tiles_total, ((row_number() over ()) - 1) / i.max_tiles as bucket, r.qk from indata i, qk_s_inhabited(i.iqk, i.mlevel, i._tbl ) r ) rr
	    group by rr.bucket, rr.tiles_total
    )
    select r.iqk as qk, r.mlevel, r._tbl, r.max_tiles, l.*, format('select * from qk_s_get_fc_of_tiles(false,%1$L::text[],''%2$s'',true,true)',l.tlist,r._tbl) as s3sql
    from ibuckets l, indata r
    order by bucket;
 else
  return query 
	 with 
	  indata   as ( select exp_build_sql_inhabited.iqk as iqk, exp_build_sql_inhabited.mlevel as mlevel, exp_build_sql_inhabited._tbl as _tbl, exp_build_sql_inhabited.max_tiles as max_tiles ),
    ibuckets as 
    ( select rr.bucket::integer, (count(1) over ())::integer as nrbuckets, count(1)::integer as nrsubtiles, rr.tiles_total::integer , array_agg(rr.qk) as tlist
      from ( select count(1) over () tiles_total, ((row_number() over ()) - 1) / i.max_tiles as bucket, r.qk from indata i, htile_s_inhabited(i.iqk, i.mlevel, i._tbl ) r ) rr
	    group by rr.bucket, rr.tiles_total
    )
    select r.iqk as qk, r.mlevel, r._tbl, r.max_tiles, l.*, format('select * from qk_s_get_fc_of_tiles(true,%1$L::text[],''%2$s'',true,true)',l.tlist,r._tbl) as s3sql
    from ibuckets l, indata r
    order by bucket;
 end if;
end
$_$;

/*
select * from exp_build_sql_inhabited(false, '1200232', 15, 'public."56bd42b1a81b8f2753e8f2f6a235d2a8"'::regclass ) -- mercator tiles 
select * from exp_build_sql_inhabited(true, '12201201011111', 15, 'public."56bd42b1a81b8f2753e8f2f6a235d2a8"'::regclass ) -- here tiles

select * from exp_build_sql_inhabited(false, '1202011320', 14, 'public."56bd42b1a81b8f2753e8f2f6a235d2a8"'::regclass, 3) 
select * from exp_build_sql_inhabited(true, '12201201011111', 15, 'public."56bd42b1a81b8f2753e8f2f6a235d2a8"'::regclass, 3) 

sample usage:

select 
 (aws_s3.query_export_to_s3( o.s3sql , 'iml-http-connector-sit-s3bucket-9bnjg7jo97un', format('test-mnah-499/space/%s/%s/%s-%s', replace(o._tble::text,'"','') ,o.qk,o.bucket,o.nrbuckets) ,'eu-west-1','format text')).* ,
 o.*
from 
 exp_build_sql_inhabited(false, '1202011320', 14, 'public."56bd42b1a81b8f2753e8f2f6a235d2a8"'::regclass, 13) o


*/

CREATE OR REPLACE FUNCTION qk_s_inhabited_txt(iqk text, mlevel integer, sql_with_geo text) RETURNS TABLE(qk text)
 LANGUAGE plpgsql STABLE
    AS $_$
declare
 bFound boolean := false;
begin

 for lastDigit in 0..3 loop
  qk = iqk || lastDigit;
	
  execute 
	   format( 'select exists ( select 1 from ( %1$s ) r where st_intersects(r.geo, xyz_qk_qk2bbox( %2$L )))', sql_with_geo, qk )
	     into bFound;

  if bFound then
   if length( qk ) >= mlevel then
    return next;
   else
    return query
     select r.qk from qk_s_inhabited_txt(qk, mlevel, sql_with_geo ) r;
   end if;
  end if;
 end loop;

end
$_$;

CREATE OR REPLACE FUNCTION htile_s_inhabited_txt(iqk text, mlevel integer, sql_with_geo text) RETURNS TABLE(qk text)
    LANGUAGE plpgsql STABLE
    AS $_$
declare
 bFound boolean := false;
begin

 for lastDigit in 0..3 loop
  qk = iqk || lastDigit;
  
  execute 
	   format( 'select exists ( select 1 from ( %1$s ) r where st_intersects(r.geo, htile_bbox( %2$L )))', sql_with_geo, qk )
	     into bFound;

  if bFound then
   if length( qk ) >= mlevel then
    return next;
   else
    return query
     select r.qk from htile_s_inhabited_txt(qk, mlevel, sql_with_geo ) r;
   end if;
  end if;
 end loop;

end
$_$;

CREATE OR REPLACE FUNCTION qk_s_get_fc_of_tiles_txt_v1(here_tile_qk boolean, sql_with_jsondata_geo text, base64enc boolean, clipped boolean ) RETURNS TABLE(tile_id text, tile_content text)
    LANGUAGE plpgsql
    AS $_$
declare
 result record;
 tile text;
 fkt_qk2box text := 'xyz_qk_qk2bbox';
 plainjs text    := 'jsonb_build_object(''type'',''FeatureCollection'',''features'', jsonb_agg(feature) )::text';
 plaingeo text   := 'geo';
 feature_count bigint := 0;
begin

  if here_tile_qk then
   fkt_qk2box = 'htile_bbox';
  end if;
	
	if clipped then
	 plaingeo = 'ST_Intersection(ST_MakeValid( geo ),' || fkt_qk2box || '((%2$L)))';
	end if;
	
	if base64enc then
	 plainjs = 'regexp_replace( encode( replace(' || plainjs || ',''\'',''\\'')::bytea, ''base64'' ),''\n'','''',''g'')';
	end if;

	foreach tile in array tile_list
	loop
		begin
		 execute format(
				'SELECT %2$L,' || plainjs || ', count(1) as cnt'
				||' from( '
				||' select jsonb_set(jsondata,''{geometry}'',ST_AsGeojson(' || plaingeo ||')::jsonb) as feature'
				||'	 from ( %1$s ) o '
				||'  where ST_Intersects(geo, ' || fkt_qk2box || '(%2$L))' 			
				||' ) oo', sql_with_jsondata_geo, tile)
				INTO tile_id, tile_content, feature_count;
				
 		 if feature_count > 0 then
		  return next;
		 end if; 
			
		end;
	end loop;
end
$_$;

CREATE OR REPLACE FUNCTION qk_s_get_fc_of_tiles_txt_v2(here_tile_qk boolean, tile_list text[], sql_with_jsondata_geo text, base64enc boolean, clipped boolean ) RETURNS TABLE(tile_id text, tile_content text)
    LANGUAGE plpgsql
    AS $_$
declare
 fkt_qk2box text := 'xyz_qk_qk2bbox';
 plainjs text    := 'jsonb_build_object(''type'',''FeatureCollection'',''features'', features )::text';
 plaingeo text   := 'geo';
begin

  if here_tile_qk then
   fkt_qk2box = 'htile_bbox';
  end if;
	
	if clipped then
	 plaingeo = 'ST_Intersection(ST_MakeValid(d.geo), bbox) as geo';
	end if;
	
	if base64enc then
	 plainjs = 'regexp_replace( encode( replace(' || plainjs || ',''\'',''\\'')::bytea, ''base64'' ),''\n'','''',''g'')';
	end if;
	
	return query execute
	 format(
          ' select qk,' || plainjs
       || ' from'
       || ' ( select qk, jsonb_agg( jsonb_set(jsondata,''{geometry}'',ST_AsGeojson( geo )::jsonb ) ) as features'
       || '   from'
       || '   ( select qk, d.jsondata,' || plaingeo
       || '     from'
       || '      unnest( %2$L::text[] ) qk,'
       || '      lateral ' || fkt_qk2box || '( qk ) bbox,'
       || '      lateral ( select jsondata, geo from ( %1$s ) i where st_intersects( d.geo, bbox ) ) d'
       || '   ) o'
       || '   group by qk'
       || ' ) oo' , sql_with_jsondata_geo, tile_list );
end
$_$;

CREATE OR REPLACE FUNCTION qk_s_get_fc_of_tiles_txt(here_tile_qk boolean, tile_list text[], sql_with_jsondata_geo text, base64enc boolean, clipped boolean ) RETURNS TABLE(tile_id text, tile_content text)
LANGUAGE sql
AS $_$
 select tile_id, tile_content from qk_s_get_fc_of_tiles_txt_v1( here_tile_qk, tile_list, sql_with_jsondata_geo, base64enc, clipped ) 
$_$;

-- drop function exp_build_sql_inhabited_txt(htile boolean, iqk text, mlevel integer, sql_with_jsondata_geo text, sql_for_tiles_with_geo text)
create or replace function exp_build_sql_inhabited_txt(htile boolean, iqk text, mlevel integer, sql_with_jsondata_geo text, sql_qk_tileqry_with_geo text) returns table(qk text, s3sql text)
language plpgsql stable
as $_$
begin
 if not htile then
  return query 
	 with 
    indata as ( select exp_build_sql_inhabited_txt.iqk as iqk, 
					   exp_build_sql_inhabited_txt.mlevel as mlevel, 
					   exp_build_sql_inhabited_txt.sql_with_jsondata_geo as sql_export_data, 
					   coalesce( exp_build_sql_inhabited_txt.sql_qk_tileqry_with_geo, exp_build_sql_inhabited_txt.sql_with_jsondata_geo) as sql_qks 
		 	  ),
    qks as ( select r.qk, i.sql_export_data from indata i, qk_s_inhabited_txt(i.iqk, i.mlevel, i.sql_qks ) r )
   select o.qk, format('select %1L, jsondata,geo from ( %2$s ) i where st_intersects(geo, xyz_qk_qk2bbox( %1$L))',o.qk,o.sql_export_data) as s3sql from qks o;
 else
  return query 
	 with 
    indata as ( select exp_build_sql_inhabited_txt.iqk as iqk, 
					   exp_build_sql_inhabited_txt.mlevel as mlevel, 
					   exp_build_sql_inhabited_txt.sql_with_jsondata_geo as sql_export_data, 
					   coalesce( exp_build_sql_inhabited_txt.sql_qk_tileqry_with_geo, exp_build_sql_inhabited_txt.sql_with_jsondata_geo) as sql_qks 
		 	  ),
    qks as ( select r.qk, i.sql_export_data from indata i, htile_s_inhabited_txt(i.iqk, i.mlevel, i.sql_qks ) r )
   select o.qk, format('select %1L, jsondata,geo from ( %2$s ) i where st_intersects(geo, htile_bbox( %1$L))',o.qk,o.sql_export_data) as s3sql from qks o;
 end if;
end
$_$;

create or replace function exp_build_sql_inhabited_txt(htile boolean, iqk text, mlevel integer, sql_with_jsondata_geo text) returns table(qk text, s3sql text)
language sql stable
as $_$
 select qk, s3sql from exp_build_sql_inhabited_txt(htile,iqk,mlevel,sql_with_jsondata_geo, null::text)
$_$;

-- drop function exp_build_sql_inhabited_txt(htile boolean, iqk text, mlevel integer, sql_with_jsondata_geo text, max_tiles integer) 

create or replace function exp_build_sql_inhabited_txt(htile boolean, iqk text, mlevel integer, sql_with_jsondata_geo text, sql_qk_tileqry_with_geo text, max_tiles integer) 
 returns table(qk text, mlev integer, sql_with_jsdata_geo text, max_tls integer, bucket integer, nrbuckets integer, nrsubtiles integer, tiles_total integer, tile_list text[], s3sql text)
language plpgsql stable
as $_$
begin
 if not htile then
  return query 
	 with 
	  indata   as ( select exp_build_sql_inhabited_txt.iqk as iqk, 
								         exp_build_sql_inhabited_txt.mlevel as mlevel, 
							           exp_build_sql_inhabited_txt.sql_with_jsondata_geo as sql_export_data, 
							           coalesce( exp_build_sql_inhabited_txt.sql_qk_tileqry_with_geo, exp_build_sql_inhabited_txt.sql_with_jsondata_geo) as sql_qks,
								         exp_build_sql_inhabited_txt.max_tiles as max_tiles 
								),
    ibuckets as 
    ( select rr.bucket::integer, (count(1) over ())::integer as nrbuckets, count(1)::integer as nrsubtiles, rr.tiles_total::integer , array_agg(rr.qk) as tlist
      from ( select count(1) over () tiles_total, ((row_number() over ()) - 1) / i.max_tiles as bucket, r.qk from indata i, qk_s_inhabited_txt(i.iqk, i.mlevel, i.sql_qks ) r ) rr
	    group by rr.bucket, rr.tiles_total
    )
    select r.iqk as qk, r.mlevel, r.sql_export_data, r.max_tiles, l.*, format('select * from qk_s_get_fc_of_tiles_txt(false,%1$L::text[],%2$L,true,true)',l.tlist,r.sql_export_data) as s3sql
    from ibuckets l, indata r
    order by bucket;
 else
  return query 
	 with 
	  indata   as ( select exp_build_sql_inhabited_txt.iqk as iqk, 
								         exp_build_sql_inhabited_txt.mlevel as mlevel, 
							           exp_build_sql_inhabited_txt.sql_with_jsondata_geo as sql_export_data, 
							           coalesce( exp_build_sql_inhabited_txt.sql_qk_tileqry_with_geo, exp_build_sql_inhabited_txt.sql_with_jsondata_geo) as sql_qks,
								         exp_build_sql_inhabited_txt.max_tiles as max_tiles 
								),
    ibuckets as 
    ( select rr.bucket::integer, (count(1) over ())::integer as nrbuckets, count(1)::integer as nrsubtiles, rr.tiles_total::integer , array_agg(rr.qk) as tlist
      from ( select count(1) over () tiles_total, ((row_number() over ()) - 1) / i.max_tiles as bucket, r.qk from indata i, htile_s_inhabited_txt(i.iqk, i.mlevel, i.sql_qks ) r ) rr
	    group by rr.bucket, rr.tiles_total
    )
    select r.iqk as qk, r.mlevel, r.sql_export_data, r.max_tiles, l.*, format('select * from qk_s_get_fc_of_tiles_txt(true,%1$L::text[],%2$L,true,true)',l.tlist,r.sql_export_data) as s3sql
    from ibuckets l, indata r
    order by bucket;
 end if;
end
$_$;

create or replace function exp_build_sql_inhabited_txt(htile boolean, iqk text, mlevel integer, sql_with_jsondata_geo text, max_tiles integer) 
 returns table(qk text, mlev integer, sql_with_jsdata_geo text, max_tls integer, bucket integer, nrbuckets integer, nrsubtiles integer, tiles_total integer, tile_list text[], s3sql text)
language sql stable
as $_$
 select qk, mlev, sql_with_jsdata_geo, max_tls, bucket, nrbuckets, nrsubtiles, tiles_total, tile_list, s3sql from exp_build_sql_inhabited_txt(htile, iqk, mlevel, sql_with_jsondata_geo, null::text, max_tiles) 
$_$;


/*
Sample:

select 
 (aws_s3.query_export_to_s3( o.s3sql, 'iml-http-connector-sit-s3bucket-9bnjg7jo97un', format('test-mnah-499/space/%s/%s/%s-%s', 'testdyn2' ,o.qk,o.bucket,o.nrbuckets) ,'eu-west-1','format csv')).* ,
 o.*
from exp_build_sql_inhabited_txt(true, '013200030201', 12,
'		
 select jsondata, geo from public."011420244443dfc61dfeaeb154c06b39"
 where 1 = 1 
   and jsondata->''properties''->''irand'' = to_jsonb( 4 )
'
, 7) o


*/

