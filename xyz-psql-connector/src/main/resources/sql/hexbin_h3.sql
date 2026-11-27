/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
 * Helper functions for the hexbin clustering and the h3Index filter, based on the extensions h3 and h3_postgis.
 * common.sql installs the extensions into the schema h3_ext. The objects of the extensions are referenced schema-qualified here.
 * The SQL functions of h3_postgis call further functions unqualified, so h3_ext must also be on the search path of the session.
 */
DO $checkH3Extension$
BEGIN
    IF to_regtype('h3_ext.h3index') IS NULL
        OR to_regprocedure('h3_ext.h3_cell_to_boundary_geometry(h3_ext.h3index)') IS NULL THEN
        RAISE EXCEPTION 'The extensions h3 and h3_postgis are not installed in schema h3_ext, the H3 functions can not be installed.'
            USING HINT = 'common.sql creates them if h3, h3_postgis and postgis_raster are available.';
    END IF;
END
$checkH3Extension$;

/**
 * Returns the distinct H3 cells of the specified resolution covering the geometry.
 * Points: the cell containing the point.
 * Polygons: every cell the polygon overlaps (mode "overlapping"), so a polygon smaller than a cell still gets its cell.
 * Lines: the line gets segmentized by the average edge length of the resolution, then the cell of each vertex.
 * Multi-geometries and geometry collections: the union of the cells of their parts.
 */
CREATE OR REPLACE FUNCTION xyz_h3_cover(geo GEOMETRY, resolution INTEGER) RETURNS SETOF h3_ext.h3index AS
$BODY$
    WITH parts AS (SELECT (ST_Dump(ST_Force2D(geo))).geom AS part)
    SELECT DISTINCT cell FROM (
        SELECT part OPERATOR(h3_ext.@) resolution FROM parts WHERE ST_GeometryType(part) = 'ST_Point'
        UNION ALL
        SELECT h3_ext.h3_polygon_to_cells_experimental(part, resolution, 'overlapping') FROM parts
        WHERE ST_GeometryType(part) = 'ST_Polygon'
        UNION ALL
        SELECT dp.geom OPERATOR(h3_ext.@) resolution
        FROM parts, LATERAL ST_DumpPoints(ST_Segmentize(part::geography,
            h3_ext.h3_get_hexagon_edge_length_avg(resolution, 'm'))::geometry) dp
        WHERE ST_GeometryType(part) = 'ST_LineString'
    ) cells(cell)
$BODY$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

/**
 * Converts the textual H3 index (15 or 16 hexadecimal digits) into a cell.
 * Raises an error with SQLSTATE XYZ40 (illegal argument) if the value is malformed or not a valid H3 cell.
 */
CREATE OR REPLACE FUNCTION xyz_h3_cell(h3_index TEXT) RETURNS h3_ext.h3index AS
$BODY$
BEGIN
    --The checks are nested, because the cast of a malformed value would fail with an unspecific error
    IF h3_index !~ '^[0-9a-fA-F]{15,16}$' THEN
        RAISE EXCEPTION 'Invalid h3Index %: not a valid H3 cell', h3_index USING ERRCODE = 'XYZ40';
    END IF;
    IF NOT h3_ext.h3_is_valid_cell(h3_index::h3_ext.h3index) THEN
        RAISE EXCEPTION 'Invalid h3Index %: not a valid H3 cell', h3_index USING ERRCODE = 'XYZ40';
    END IF;
    RETURN h3_index::h3_ext.h3index;
END
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

/**
 * Returns the number of consecutive vertices of the rings and lines of the geometry whose longitudes differ by more than 180 degrees.
 * A buffer computed on the geography and cast back to geometry has such a jump wherever its boundary crosses the antimeridian.
 */
CREATE OR REPLACE FUNCTION xyz_h3_antimeridian_jumps(geo GEOMETRY) RETURNS BIGINT AS
$BODY$
    SELECT count(*) FROM (
        SELECT abs(ST_X(geom) - lag(ST_X(geom)) OVER (PARTITION BY path[1:array_length(path, 1) - 1]
            ORDER BY path[array_length(path, 1)])) AS dx
        FROM ST_DumpPoints(geo)
    ) vertices WHERE dx > 180
$BODY$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

/**
 * Returns the bounding box buffered by the distance in meters, which hexbin uses to read the features and to select the cells.
 * Usually the envelope of the buffer. If the buffer crosses the antimeridian, a MultiPolygon of two envelopes instead, one on each
 * side of +-180, so that it does not wrap around into an envelope over almost all longitudes. Both parts reach one degree beyond
 * +-180, so that the parts of a cell split at the antimeridian, which end on +-180, lie in their interior.
 * The envelope of the buffer is kept for a bbox with west > east or spanning 180 degrees or more, and for a buffer around a pole.
 */
CREATE OR REPLACE FUNCTION xyz_h3_buffered_bbox(west FLOAT8, south FLOAT8, east FLOAT8, north FLOAT8, distance FLOAT8)
    RETURNS GEOMETRY AS
$BODY$
DECLARE
    buffered GEOMETRY := ST_Buffer(ST_MakeEnvelope(west, south, east, north, 4326)::geography, distance)::geometry;
    jumps BIGINT;
    shifted GEOMETRY;
BEGIN
    IF west > east OR east - west >= 180 THEN
        RETURN ST_Envelope(buffered);
    END IF;
    jumps := xyz_h3_antimeridian_jumps(buffered);
    --No jump: no crossing. An odd number: the buffer surrounds a pole, its envelope covers all longitudes already.
    IF jumps = 0 OR jumps % 2 = 1 THEN
        RETURN ST_Envelope(buffered);
    END IF;
    --Longitudes from 0 to 360, so that the buffer is continuous across the antimeridian
    shifted := ST_ShiftLongitude(buffered);
    --A buffer that crosses the prime meridian as well is wider than 180 degrees, keep its envelope
    IF xyz_h3_antimeridian_jumps(shifted) > 0 OR ST_XMin(shifted) >= 180 OR ST_XMax(shifted) <= 180 THEN
        RETURN ST_Envelope(buffered);
    END IF;
    RETURN ST_Collect(
        ST_MakeEnvelope(ST_XMin(shifted), ST_YMin(buffered), 181, ST_YMax(buffered), 4326),
        ST_MakeEnvelope(-181, ST_YMin(buffered), ST_XMax(shifted) - 360, ST_YMax(buffered), 4326));
END
$BODY$
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
