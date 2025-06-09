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

/****************************************************************************************************************
*********************************************** MERCATOR_QUAD ***************************************************
*****************************************************************************************************************/
-- TODO: Remove all quad related functions in ext.sql and switch all implementation to the new geo.sql functions
-- select mercator_quad(2151,1387,12);
-- select mercator_quad(9.10899,50.09701, 12);
-- select mercator_quad_crl(9.10899,50.09701, 12);
-- select mercator_quad_crl(mercator_quad(2151,1387,12));
-- select ST_AsGeojson(mercator_quad_to_bbox(2151,1387,12))
-- select ST_AsGeojson(mercator_quad_to_bbox(mercator_quad(2151,1387,12)));
-- select mercator_quad_zoom_level_from_bbox(ST_GeomFromText('LINESTRING(9.102719236998638 49.22677286363091,
-- 	7.011126411595683 51.32741493065305)',4326));
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mercator_quad_crl(lonX DOUBLE PRECISION, latY DOUBLE PRECISION, in_level INTEGER)
    RETURNS TABLE(colX INTEGER, rowY INTEGER, level INTEGER) AS $$
DECLARE
    sinY numeric;
    numRowsCols constant INTEGER := 1 << in_level;
BEGIN

    sinY = sin( latY * pi() / 180.0 );

    colX := floor(((lonX + 180.0) / 360.0) * numRowsCols);
    rowY := floor((0.5 - ln((1 + sinY) / (1 - sinY)) / (4 * pi())) * numRowsCols );
    level := in_level;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mercator_quad_crl(qid TEXT)
    RETURNS TABLE(colX INTEGER, rowY INTEGER, level INTEGER) AS $$
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
                END case;
        END loop;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mercator_quad_to_bbox(colX INTEGER, rowY INTEGER, level INTEGER)
    RETURNS GEOMETRY AS $$
SELECT ST_Transform(ST_TileEnvelope(level, colX, rowY), 4326)
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mercator_quad_to_bbox(qid TEXT )
    RETURNS GEOMETRY AS $$
SELECT mercator_quad_to_bbox(colX,rowY,level)
FROM(
    SELECT colX,rowY,level FROM mercator_quad_crl( qid )
)A;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mercator_quad_zoom_level_from_bbox(geo GEOMETRY )
    RETURNS INTEGER AS $$
SELECT round( ( 5.88610403145016 - ln( st_xmax(i.env) - st_xmin(i.env) )  )/ 0.693147180559945 )::INTEGER as zm
FROM (
    select st_envelope( geo ) as env
) i
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mercator_quad(colX INTEGER, rowY INTEGER, level INTEGER )
    RETURNS TEXT AS $$
DECLARE
    qk TEXT := '';
    digit INTEGER;
    digits CHAR(1)[] = ARRAY[ '0', '1', '2', '3' ];
    mask BIT(32);
BEGIN

    for i in reverse level .. 1 LOOP
            digit = 1;
            mask = 1::BIT(32) << ( i - 1 );

            if (colX::BIT(32) & mask) <> 0::BIT(32) then
                digit = digit + 1;
            END if;

            if (rowY::BIT(32) & mask) <> 0::BIT(32) then
                digit = digit + 2;
            END if;

            qk = qk || digits[ digit ];

        END loop;

    RETURN qk;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mercator_quad(lonX DOUBLE PRECISION, latY DOUBLE PRECISION, level INTEGER)
    RETURNS TEXT AS $$
SELECT mercator_quad( colX,rowY, A.level)
FROM mercator_quad_crl(lonX, latY, level) A;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
/****************************************************************************************************************
*********************************************** HERE QUAD *******************************************************
*****************************************************************************************************************/
--------------------------------------------------
--Base4		12201203120220
--Base10	377894440
--------------------------------------------------
--- Base10
-- select here_quad(8800,6486,14)
-- select here_quad(13.4050::FLOAT, 52.5200::FLOAT, 14);
-- select here_quad_crl('377894440')
-- select here_quad_crl(13.4050::FLOAT, 52.5200::FLOAT, 14)
-- select ST_AsGeojson(here_quad_to_bbox(8800,6486,14))
-- select ST_AsGeojson(here_quad_to_bbox('377894440'))
-- --- Base4
-- select here_quad_crl('12201203120220',true);
-- select here_quad_base4_to_base10('12201203120220');
-- select ST_AsGeojson(here_quad_to_bbox('12201203120220',true));
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad(lonX DOUBLE PRECISION, latY DOUBLE PRECISION, level INT)
    RETURNS BIGINT AS $$
DECLARE
    x INT;
    y INT;
    angular_width DOUBLE PRECISION;
    angular_height DOUBLE PRECISION;
    col DOUBLE PRECISION;
    row DOUBLE PRECISION;
BEGIN
    -- Compute X coordinate
    x := 0;
    IF ABS(lonX) <> 180.0 THEN
        angular_width := get_quad_angular_width(level);
        col := (lonX + 180.0) / angular_width;

        IF (col - FLOOR(col)) = 0.0 AND lonX < (angular_width * col - 180.0) THEN
            x := FLOOR(col) - 1;
        ELSE
            x := FLOOR(col);
        END IF;

        x := LEAST(get_x_max(level), x);
    END IF;

    -- Compute Y coordinate
    y := 0;
    angular_height := get_quad_angular_height(level);
    row := (latY + 90.0) / angular_height;

    IF (row - FLOOR(row)) = 0.0 AND latY < (angular_height * row - 90.0) THEN
        y := FLOOR(row) - 1;
    ELSE
        y := FLOOR(row);
    END IF;

    y := LEAST(get_y_max(level), y);

    -- Return final long key
    RETURN here_quad(x, y, level);
END;
$$ LANGUAGE plpgsql;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad_crl(qk TEXT, isbase4encoded boolean DEFAULT false)
    RETURNS TABLE(colX INTEGER, rowY INTEGER, level INTEGER, hkey BIGINT) AS $$
BEGIN
    IF NOT isBase4Encoded THEN
        qk = number_to_base(qk::BIGINT, 4);
    END IF;

    level = length(qk);
    hkey = here_quad_base4_to_base10(qk);

    /** Remove first level BIT and convert to x */
    colX = here_quad_utils_modify_bits(hkey & ((1::BIGINT << level * 2) - 1),'extract');
    /** Remove first level BIT and convert to y */
    rowY = here_quad_utils_modify_bits(hkey & ((1::BIGINT << level * 2) - 1) >> 1, 'extract');
    RETURN NEXT;
END
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad_crl(lonX DOUBLE PRECISION, latY DOUBLE PRECISION, in_level INT)
    RETURNS TABLE(colX INTEGER, rowY INTEGER, level INTEGER) AS $$
BEGIN
    RETURN QUERY SELECT A.colX, A.rowY, A.level
        FROM here_quad_crl((
            SELECT here_quad(lonX, latY, in_level)::TEXT),false) A;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad_to_bbox(colX INTEGER, rowY INTEGER, level INTEGER)
    RETURNS GEOMETRY AS $$
DECLARE
    height float;
    width float;
    minX float;
    minY float;
    maxX float;
    maxY float;
BEGIN
    IF level = 0 THEN
        height = 180;
    ELSE
        height = 360.0 / (1 << level);
    END IF;

    width =  360.0 / (1 << level);
    minX = width * colX - 180;
    minY = height * rowY - 90;
    maxX = width * (colX + 1) - 180;
    maxY = height * (rowY + 1) - 90;

    RETURN st_setsrid(ST_MakeEnvelope( minX, minY, maxX, maxY ),4326);
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad_to_bbox(qk TEXT, isbase4encoded boolean DEFAULT false)
    RETURNS GEOMETRY AS $$
SELECT here_quad_to_bbox(colX,rowY, level) FROM here_quad_crl( qk, isbase4encoded );
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad_base4_to_base10(qk TEXT)
    RETURNS BIGINT AS $$
DECLARE
    level INTEGER;
    k INTEGER;
    hkey BIGINT;
BEGIN
    level = length(qk);
    hkey = 0;
    k = level -1;

    FOR i IN 1 .. level
        LOOP
            hkey = hkey+ (substring(qk,level-(i-1),1)::BIGINT << (i-1) * 2);
        END LOOP;
    RETURN hkey | (1::BIGINT << (level * 2));
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad_base4_to_base10(colX INTEGER, rowY INTEGER)
    RETURNS BIGINT AS $$
BEGIN
    colX = here_quad_utils_modify_bits(colX,'interleave');
    rowY = here_quad_utils_modify_bits(rowY,'interleave');
    RETURN colX | (rowY << 1);
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad(colX INTEGER, rowY INTEGER, level INTEGER)
    RETURNS BIGINT AS $$
SELECT here_quad_base4_to_base10(colX, rowY) | (1::BIGINT << (level * 2));
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION here_quad_utils_modify_bits(num BIGINT, bmode TEXT)
    RETURNS BIGINT AS $$
DECLARE
    magicNumbers BIGINT[] := ARRAY[
        6148914691236517205,	--110000101001000100100010100011010010001001000110110010100010111001000000101
        3689348814741910323,	--11001100110011001100110011001100110011001100110011001100110011
        1085102592571150095,	--111100001111000011110000111100001111000011110000111100001111
        71777214294589695,		--11111111000000001111111100000000111111110000000011111111
        281470681808895,		--111111111111111100000000000000001111111111111111
        4294967295				--11111111111111111111111111111111
        ];
BEGIN
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
    ELSE
        RAISE EXCEPTION 'Invalid mode - choose "interleave" or "extract"';
    END if;

    RETURN num;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION number_from_base(num TEXT, base INTEGER)
    RETURNS numeric AS $$
SELECT sum(exp * cn)
FROM (
         SELECT base::NUMERIC ^ (row_number() OVER () - 1) exp,
                CASE
                    WHEN ch BETWEEN '0' AND '9' THEN ascii(ch) - ascii('0')
                    WHEN ch BETWEEN 'a' AND 'z' THEN 10 + ascii(ch) - ascii('a')
                    END cn
         FROM regexp_split_to_table(reverse(lower(num)), '') ch(ch)
     ) sub
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION number_to_base(num BIGINT, base INTEGER)
    RETURNS TEXT AS $$
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
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
/****************************************************************************************************************
********************************** GENERIC QUAD FUNCTIONS *******************************************************
*****************************************************************************************************************/
CREATE OR REPLACE FUNCTION for_coordinate(lonX FLOAT, latY FLOAT, level INT, quad_type TEXT = 'MERCATOR_QUAD')
    RETURNS TABLE(x INT, y INT) AS $$
BEGIN
    IF quad_type = 'MERCATOR_QUAD' THEN
        RETURN QUERY select colX, rowY
                     from mercator_quad_crl( lonX, latY, level );
    ELSE
        RETURN QUERY select colX, rowY
                     from here_quad_crl(here_quad(lonX, latY, level)::TEXT, false);
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION tile_to_bbox(colX INT, rowY INT, level INT, quad_type TEXT = 'MERCATOR_QUAD')
    RETURNS GEOMETRY AS $$
BEGIN
    IF quad_type = 'MERCATOR_QUAD' THEN
        RETURN mercator_quad_to_bbox(colX,rowY,level);
    ELSE
        -- only base10 here_quads are currently supported
        RETURN here_quad_to_bbox( colX, rowY, level);
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
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

/****************************************************************************************************************
********************************** TILE_CALCULATIONS ************************************************************
*****************************************************************************************************************/
-----------------------------------------------------------------------------------------------
DO $$
BEGIN
    -- Check if the type exists in the current schema
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typname = 'map_quad_range' AND n.nspname = current_schema()
    ) THEN
        EXECUTE format(
                'CREATE TYPE %I.map_quad_range AS (
                    level INT,
                    tiles_per_axis INT,
                    min_x INT,
                    max_x INT,
                    min_y INT,
                    max_y INT,
                    span_x INT,
                    span_y INT
                )', current_schema());
    END IF;
END $$;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_bounding_box_for_range(
    tile_x INTEGER,
    max_tile_x INTEGER,
    tile_y INTEGER,
    max_tile_y INTEGER,
    level INTEGER,
    quad_type TEXT = 'MERCATOR_QUAD'
) RETURNS GEOMETRY AS $$
DECLARE
    top_left_bbox GEOMETRY;
    bottom_right_bbox GEOMETRY;
    bottom_left_bbox GEOMETRY;
    top_right_bbox GEOMETRY;
BEGIN
    IF quad_type = 'MERCATOR_QUAD' THEN
        -- Get bounding box for top-left tile
        SELECT * INTO top_left_bbox FROM tile_to_bbox(tile_x, tile_y, level, quad_type);

        -- Get bounding box for bottom-right tile
        SELECT * INTO bottom_right_bbox FROM tile_to_bbox(max_tile_x, max_tile_y, level, quad_type);

        -- Create a bounding box geometry
        RETURN ST_MakeEnvelope(
                ST_XMin(top_left_bbox),
                ST_YMin(bottom_right_bbox),
                ST_XMax(bottom_right_bbox),
                ST_YMax(top_left_bbox),
                4326);
    ELSIF quad_type = 'HERE_QUAD' THEN
        -- Get bounding box for bottom-left tile
        SELECT * INTO bottom_left_bbox FROM tile_to_bbox( tile_x, tile_y,level, quad_type);

        -- Get bounding box for top-right tile
        SELECT * INTO top_right_bbox FROM tile_to_bbox(max_tile_x, max_tile_y, level, quad_type);

        -- Create a bounding box geometry
        RETURN ST_MakeEnvelope(
                ST_XMin(bottom_left_bbox),
                ST_YMin(bottom_left_bbox),
                ST_XMax(top_right_bbox),
                ST_YMax(top_right_bbox),
                4326);
    ELSE
        RAISE EXCEPTION 'Unsupported quad_type: %', quad_type;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_tiles_for_range(range map_quad_range)
    RETURNS TABLE (x INT, y INT, rlevel INT) AS $$
DECLARE
    i INT;
    j INT;
BEGIN
    FOR i IN 0..(range.span_y - 1) LOOP
            FOR j IN 0..(range.span_x - 1) LOOP
                    x := (range.min_x + j) % range.tiles_per_axis;
                    y := range.min_y + i;
                    rlevel := range.level;
                    RETURN NEXT;
                END LOOP;
        END LOOP;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_tile_range(geom GEOMETRY, level INT, quad_type TEXT = 'MERCATOR_QUAD')
    RETURNS map_quad_range AS $$
DECLARE
    envelope GEOMETRY := ST_Envelope(geom);
    minx FLOAT := ST_XMin(envelope);
    miny FLOAT := ST_YMin(envelope);
    maxx FLOAT := ST_XMax(envelope);
    maxy FLOAT := ST_YMax(envelope);
    t1 RECORD; t2 RECORD;
    tiles_per_axis INT := 2^level;
    span_x INT;
    span_y INT;
BEGIN
    SELECT x, y INTO t1 FROM for_coordinate(minx, miny, level, quad_type);
    SELECT x, y INTO t2 FROM for_coordinate(maxx, maxy, level, quad_type);

    span_x := CASE WHEN t2.x >= t1.x THEN t2.x - t1.x + 1
                   ELSE tiles_per_axis - t1.x + t2.x + 1 END;
    span_y := GREATEST(t1.y, t2.y) - LEAST(t1.y, t2.y) + 1;

    RETURN (level, tiles_per_axis, t1.x, t2.x, LEAST(t1.y, t2.y), GREATEST(t1.y, t2.y), span_x, span_y);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION for_geometry(geom geometry, in_level integer, quad_type TEXT = 'MERCATOR_QUAD')
    RETURNS TABLE(colX integer, rowY integer, level integer)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    step_size CONSTANT INTEGER := 4;
    geom_type TEXT;
    range map_quad_range;
    tile_x INT;
    tile_y INT;
    max_tile_x INT;
    max_tile_y INT;
    i INT;
    bbox GEOMETRY;
BEGIN
    geom_type := ST_GeometryType(geom);

    -- Handle Point geometry
    IF geom_type = 'ST_Point' THEN
        RETURN QUERY
            SELECT a.x, a.y, in_level FROM for_coordinate(ST_X(geom), ST_Y(geom), in_level, quad_type) a;
        RETURN;
    END IF;

    -- Handle MultiPoint geometry
    IF geom_type = 'ST_MultiPoint' THEN
        RETURN QUERY
            WITH points AS (
                SELECT (ST_DumpPoints(geom)).geom AS point
            )
            SELECT DISTINCT fc.x, fc.y, in_level
            FROM points, LATERAL for_coordinate(ST_X(point), ST_Y(point), in_level, quad_type) AS fc;
        RETURN;
    END IF;

    -- Compute the tile range
    SELECT * INTO range FROM get_tile_range(geom, in_level, quad_type);

    -- Handle LineString or Polygon when the range is limited (spanX = 1 or spanY = 1)
    IF (geom_type IN ('ST_LineString', 'ST_Polygon')) AND (range.span_x = 1 OR range.span_y = 1) THEN
        RETURN QUERY SELECT * FROM get_tiles_for_range(range::map_quad_range);
        RETURN;
    END IF;

    -- Process the range in steps
    FOR tile_y IN range.min_y..range.max_y BY step_size LOOP
            max_tile_y := LEAST(tile_y + step_size - 1, range.max_y);

            i := 0;
            WHILE i < range.span_x LOOP
                    tile_x := (range.min_x + i) % (range.tiles_per_axis - 1);
                    max_tile_x := (LEAST(range.min_x + i + step_size, range.min_x + range.span_x) - 1) % (range.tiles_per_axis - 1);
                    i := i + step_size;

                    IF tile_x > max_tile_x THEN
                        i := i - (max_tile_x + 1);
                        max_tile_x := range.tiles_per_axis - 1;
                    END IF;

                    -- Get bounding box for this range
                    bbox := get_bounding_box_for_range(tile_x, max_tile_x, tile_y, max_tile_y, in_level, quad_type);

                    -- If it's a small step or intersects the geometry
                    IF (range.span_x <= step_size AND range.span_y <= step_size)
                        OR ST_Intersects(geom, bbox) THEN

                        FOR rowYY IN tile_y..max_tile_y LOOP
                                FOR j IN 0..(max_tile_x - tile_x) LOOP
                                        colX := (tile_x + j) % range.tiles_per_axis;
                                        rowY := rowYY; -- Ensure y is explicitly assigned
                                        level := in_level;

                                        -- Verify intersection at final step
                                        IF EXISTS (
                                            SELECT 1 FROM tile_to_bbox(colX, rowYY, in_level, quad_type) AS tile_bbox
                                            WHERE ST_Intersects(geom, tile_bbox)
                                        ) THEN
                                            RETURN NEXT;
                                        END IF;
                                    END LOOP;
                            END LOOP;
                    END IF;
                END LOOP;
        END LOOP;
END;
$BODY$;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_quad_angular_width(level INT)
    RETURNS DOUBLE PRECISION AS $$
SELECT 360.0 / (1 << level);
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_quad_angular_height(level INT)
    RETURNS DOUBLE PRECISION AS $$
SELECT CASE
    WHEN level = 0 THEN 180.0
    ELSE 360.0 / (1 << level)
END;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_x_max(level INT)
    RETURNS INT AS $$
SELECT (1 << level) - 1;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_y_max(level INT)
    RETURNS INT AS $$
SELECT ((1 << level) - 1) / 2;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
