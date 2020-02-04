/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
package com.here.xyz.psql;

import com.here.xyz.events.*;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

public class SQLQueryBuilder {
    private static final long EQUATOR_LENGTH = 40_075_016;
    private static final long TILE_SIZE = 256;
    private static final String SQL_STATISTIC_FUNCTION = "xyz_statistic_space";
    private static final String IDX_STATUS_TABLE = "xyz_config.xyz_idxs_status";

    public static SQLQuery buildGetStatisticsQuery(GetStatisticsEvent event, PSQLConfig config) throws Exception {
        final String schema = config.schema();
        final String table = config.table(event);

        return new SQLQuery("SELECT * from " + schema + "."+SQL_STATISTIC_FUNCTION+"('" + schema + "','" + table + "')");
    }

    public static SQLQuery buildGetFeaturesByIdQuery(GetFeaturesByIdEvent event, PSQLConfig config, DataSource dataSource)
        throws SQLException{
        SQLQuery query = new SQLQuery("SELECT");
        query.append(SQLQuery.selectJson(event.getSelection(),dataSource));
        query.append(", geojson FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
                SQLQuery.createSQLArray(event.getIds().toArray(new String[event.getIds().size()]), "text",dataSource));
       return query;
    }

    public static SQLQuery buildGetFeaturesByGeometryQuery(GetFeaturesByGeometryEvent event, DataSource dataSource)
        throws SQLException{
        final int radius = event.getRadius();
        final Geometry geometry = event.getGeometry();

        final SQLQuery query;
        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        final SQLQuery geoQuery = radius != 0 ? new SQLQuery("ST_Intersects(geo, ST_Buffer(ST_GeomFromText('"
                + WKTHelper.geometryToWKB(geometry) + "')::geography, ? )::geometry)", radius) : new SQLQuery("ST_Intersects(geo, ST_GeomFromText('"
                + WKTHelper.geometryToWKB(geometry) + "',4326))");

        if (searchQuery == null) {
            query = new SQLQuery("SELECT");
            query.append(SQLQuery.selectJson(event.getSelection(), dataSource));
            /** No clipping or simplification needed*/
            query.append(",geojson");
            query.append("FROM ${schema}.${table} WHERE");
            query.append(geoQuery);
            query.append("LIMIT ?", event.getLimit());
        } else {
            query = generateCombinedQuery(event, geoQuery, searchQuery, dataSource);
        }
        return query;
    }

    public static SQLQuery buildGetFeaturesByBBoxQuery(final GetFeaturesByBBoxEvent event, boolean isBigQuery,
                                                          DataSource dataSource)
        throws SQLException{
        final BBox bbox = event.getBbox();

        final SQLQuery searchQuery = generateSearchQuery(event,dataSource);
        final SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ST_MakeEnvelope(?, ?, ?, ?, 4326))",
                bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());

        final SQLQuery query;
        if (searchQuery == null) {
            query = new SQLQuery("SELECT");
            query.append(SQLQuery.selectJson(event.getSelection(), dataSource));
            query.append(",");
            query.append(geometrySelectorForEvent(event));
            query.append("FROM ${schema}.${table} WHERE");
            query.append(geoQuery);
            query.append("LIMIT ?", event.getLimit());

        } else if (isBigQuery) {
            query = generateCombinedQuery(event, searchQuery, geoQuery,dataSource);
        } else {
            query = generateCombinedQuery(event, geoQuery, searchQuery,dataSource);
        }

        return query;
    }

    protected static SQLQuery buildCountFeaturesQuery(CountFeaturesEvent event, DataSource dataSource, String schema, String table)
        throws SQLException{

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);
        final String schemaTable = SQLQuery.sqlQuote(schema) + "." + SQLQuery.sqlQuote(table);
        final SQLQuery query;

        if (searchQuery != null) {
            query = new SQLQuery("SELECT count(*) FROM ${schema}.${table} WHERE");
            query.append(searchQuery);
        } else {
            query = new SQLQuery("SELECT CASE WHEN reltuples < 10000");
            query.append("THEN (SELECT count(*) FROM ${schema}.${table})");
            query.append("ELSE reltuples END AS count");
            query.append("FROM pg_class WHERE oid =?::regclass", schemaTable);
        }
        return query;
    }

    /***************************************** CLUSTERING ******************************************************/
    public static SQLQuery buildHexbinClusteringQuery(
            GetFeaturesByBBoxEvent event, BBox bbox,
            Map<String, Object> clusteringParams, DataSource dataSource) throws Exception {

        int zLevel = (event instanceof GetFeaturesByTileEvent ? (int) ((GetFeaturesByTileEvent) event).getLevel() : H3SQL.bbox2zoom(bbox)),
                maxResForLevel = H3SQL.zoom2resolution(zLevel),
                h3res = (clusteringParams != null && clusteringParams.get(H3SQL.HEXBIN_RESOLUTION) != null
                        ? Math.min((Integer) clusteringParams.get(H3SQL.HEXBIN_RESOLUTION), maxResForLevel)
                        : maxResForLevel);

        String statisticalProperty = (String) clusteringParams.get(H3SQL.HEXBIN_PROPERTY);
        boolean statisticalPropertyProvided = (statisticalProperty != null && statisticalProperty.length() > 0),
                h3cflip = (clusteringParams.get(H3SQL.HEXBIN_POINTMODE) == Boolean.TRUE);

        final String expBboxSql = String
                .format("st_envelope( st_buffer( ST_MakeEnvelope(%f,%f,%f,%f, 4326)::geography, ( 2.5 * edgeLengthM( %d )) )::geometry )",
                        bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), h3res);

        /*clippedGeo - passed bbox is extended by "margin" on service level */
        String clippedGeo = (!event.getClip() ? "geo" : String
                .format("ST_Intersection(geo,ST_MakeEnvelope(%f,%f,%f,%f,4326) )", bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat())),
                fid = (!event.getClip() ? "h3" : String.format("h3 || %f || %f", bbox.minLon(), bbox.minLat())),
                filterEmptyGeo = (!event.getClip() ? "" : String.format(" and not st_isempty( %s ) ", clippedGeo));

        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        String aggField = (statisticalPropertyProvided ? "jsonb_set('{}'::jsonb, ? , agg::jsonb)::json" : "agg");

        final SQLQuery query = new SQLQuery(String.format(H3SQL.h3sqlBegin, h3res,
                !h3cflip ? "st_centroid(geo)" : "geo",
                h3cflip ? "st_centroid(geo)" : clippedGeo,
                statisticalPropertyProvided ? ", min, max, sum, avg, median" : "",
                zLevel,
                !h3cflip ? "centroid" : "hexagon",
                aggField,
                fid));

        if (statisticalPropertyProvided) {
            ArrayList<String> jpath = new ArrayList<>();
            jpath.add(statisticalProperty);
            query.addParameter(SQLQuery.createSQLArray(jpath.toArray(new String[]{}), "text", dataSource));
        }

        query.append(expBboxSql);

        if (!statisticalPropertyProvided) {
            query.append(new SQLQuery(String.format(H3SQL.h3sqlMid, h3res, "(0.0)::numeric", zLevel, H3SQL.pxSize)));
        } else {
            ArrayList<String> jpath = new ArrayList<>();
            jpath.add("properties");
            jpath.addAll(Arrays.asList(statisticalProperty.split("\\.")));

            query.append(new SQLQuery(String.format(H3SQL.h3sqlMid, h3res, "(jsondata#>> ?)::numeric", zLevel, H3SQL.pxSize)));
            query.addParameter(SQLQuery.createSQLArray(jpath.toArray(new String[]{}), "text", dataSource));
        }

        //query.append(" case st_geometrytype(geo) when 'ST_Point' then geo else st_intersection( geo ," ); query.append( expBboxSql ); query.append(" ) end as geo ");
        query.append(" case st_geometrytype(geo) when 'ST_Point' then geo else st_closestpoint( geo, geo ) end as refpt ");
        query.append(" from ${schema}.${table} v where 1 = 1 and geo && ");
        query.append(expBboxSql);
        query.append(" and st_intersects( geo ,");
        query.append(expBboxSql);
        query.append(" ) ");

        if (searchQuery != null) {
            query.append(" and ");
            query.append(searchQuery);
        }

        query.append(String.format(H3SQL.h3sqlEnd, filterEmptyGeo));
        query.append("LIMIT ?", event.getLimit());

        return query;
    }

    public static SQLQuery buildQuadbinClusteringQuery(GetFeaturesByBBoxEvent event,
                                                          BBox bbox, int resolution, String quadMode,
                                                          PSQLConfig config) {
        /** Quadkey calc */
        final int lev = WebMercatorTile.getZoomFromBBOX(bbox);
        double lon2 = bbox.minLon() + ((bbox.maxLon() - bbox.minLon()) / 2);
        double lat2 = bbox.minLat() + ((bbox.maxLat() - bbox.minLat()) / 2);

        final WebMercatorTile tile = WebMercatorTile.getTileFromLatLonLev(lat2, lon2, lev);

        SQLQuery propQuery = null;
        String propQuerySQL = null;
        final PropertiesQuery propertiesQuery = event.getPropertiesQuery();


        if (propertiesQuery != null) {
            propQuery = generatePropertiesQuery(propertiesQuery);

            if (propQuery != null) {
                propQuerySQL = propQuery.text();
                for (Object param : propQuery.parameters()) {
                    propQuerySQL = propQuerySQL.replaceFirst("\\?", "'" + param + "'");
                }
            }
        }
        return QuadbinSQL.generateQuadbinClusteringSQL(config.schema(), config.table(event), resolution, quadMode, propQuerySQL, tile);
    }
    /***************************************** CLUSTERING END **************************************************/

    public static SQLQuery buildFeaturesQuery(final SearchForFeaturesEvent event, final boolean isIterate, final boolean hasHandle,
                                                 final boolean hasSearch, final long start, DataSource dataSource)
            throws Exception {

        final SQLQuery query = new SQLQuery("SELECT");
        query.append(SQLQuery.selectJson(event.getSelection(), dataSource));
        query.append(", geojson, i FROM ${schema}.${table}");
        final SQLQuery searchQuery = generateSearchQuery(event, dataSource);

        if (hasSearch || hasHandle) {
            query.append("WHERE");
        }

        if (hasSearch) {
            query.append(searchQuery);
        }

        if (hasHandle) {
            if (hasSearch) {
                query.append("OFFSET ?", start);
            } else {
                query.append("i > ?", start);
            }
        }

        if (isIterate && !hasSearch) {
            query.append("ORDER BY i");
        }

        query.append("LIMIT ?", event.getLimit());
        return query;
    }

    public static SQLQuery buildDeleteFeaturesByTagQuery(DeleteFeaturesByTagEvent event, boolean includeOldStates,
                                                            SQLQuery searchQuery, DataSource dataSource)
        throws SQLException{

        final SQLQuery query;

        if (searchQuery != null) {
            query = new SQLQuery("DELETE FROM ${schema}.${table} WHERE");
            query.append(searchQuery);
        } else {
            query = new SQLQuery("TRUNCATE ${schema}.${table}");
        }

        if (searchQuery != null && includeOldStates)
            query.append(" RETURNING jsondata->'id' as id, geojson as geometry");

        return query;
    }

    public static SQLQuery buildLoadFeaturesQuery(Event event, final Map<String, String> idMap, DataSource dataSource)
            throws SQLException{

        final ArrayList<String> ids = new ArrayList<>(idMap.size());
        ids.addAll(idMap.keySet());

        return new SQLQuery("SELECT jsondata, geojson FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
                SQLQuery.createSQLArray(ids.toArray(new String[ids.size()]), "text" ,dataSource));
    }

    public static SQLQuery buildSearchablePropertiesUpsertQuery(Map<String, Boolean> searchableProperties, ModifySpaceEvent.Operation operation,
                                                                   String schema, String table) throws SQLException {
        String searchablePropertiesJson = "";
        final SQLQuery query = new SQLQuery("");

        if (searchableProperties == null) {
            /** Received an empty map */
            searchableProperties = new HashMap<String, Boolean>();
        } else {
            for (String property : searchableProperties.keySet()) {
                searchablePropertiesJson += "\"" + property + "\":" + searchableProperties.get(property) + ",";
            }
            /** remove last comma */
            searchablePropertiesJson = searchablePropertiesJson.substring(0, searchablePropertiesJson.length() - 1);
        }

        /** update xyz_idx_status table with searchabelProperties information */
        query.append("INSERT INTO xyz_config.xyz_idxs_status as x_s (spaceid,schem,idx_creation_finished,idx_manual) "
                        + "		VALUES ('" + table + "', '" + schema + "', false, '{" + searchablePropertiesJson
                        + "}'::jsonb) "
                        + "ON CONFLICT (spaceid) DO "
                        + "		UPDATE SET schem='" + schema + "', "
                        + "    			idx_manual = '{" + searchablePropertiesJson + "}'::jsonb, "
                        + "				idx_creation_finished = false "
                        + "		WHERE x_s.spaceid = '" + table + "'");
        return query;
    }

/** ###################################################################################### */
    private static SQLQuery generatePropertiesQuery(PropertiesQuery properties) {
        if (properties == null || properties.size() == 0) {
            return null;
        }
        // TODO: This is only a hot-fix for the connector. The issue is caused in the service and the code below will be removed after the next XYZ Hub deployment
        if (properties.get(0).size() == 0 || properties.get(0).size() == 1 && properties.get(0).get(0) == null) {
            return null;
        }
        // List with the outer OR combined statements
        List<SQLQuery> disjunctionQueries = new ArrayList<>();
        properties.forEach(conjunctions -> {

            // List with the AND combined statements
            final List<SQLQuery> conjunctionQueries = new ArrayList<>();
            conjunctions.forEach(propertyQuery -> {

                // List with OR combined statements for one property key
                final List<SQLQuery> keyDisjunctionQueries = new ArrayList<>();
                propertyQuery.getValues().forEach(v -> {

                    // The ID is indexed as text
                    if (propertyQuery.getKey().equals("id")) {
                        keyDisjunctionQueries.add(new SQLQuery("jsondata->>'id'" + SQLQuery.getOperation(propertyQuery.getOperation()) + "?::text", v));
                    }
                    // The rest are indexed as jsonb
                    else {
                        SQLQuery q = SQLQuery.createKey(propertyQuery.getKey());

                        /** BigDecimal is needed to avoid a 8Byte DOUBLE PRECISION Cast */
                        if(v instanceof Double)
                            q.append(new SQLQuery(SQLQuery.getOperation(propertyQuery.getOperation()) + SQLQuery.getValue(v), new BigDecimal(v.toString())));
                        else
                            q.append(new SQLQuery(SQLQuery.getOperation(propertyQuery.getOperation()) + SQLQuery.getValue(v), v));
                        keyDisjunctionQueries.add(q);
                    }
                });
                conjunctionQueries.add(SQLQuery.join(keyDisjunctionQueries, "OR", true));
            });
            disjunctionQueries.add(SQLQuery.join(conjunctionQueries, "AND", false));
        });
        return SQLQuery.join(disjunctionQueries, "OR", false);
    }

    private static SQLQuery generateTagsQuery(TagsQuery tags, DataSource dataSource)
            throws SQLException {
        if (tags == null || tags.size() == 0) {
            return null;
        }

        SQLQuery query;
        StringBuilder andQuery = new StringBuilder("jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& ?");
        boolean hasAnd = tags.get(0).size() > 1;

        for (int i = 1; i < tags.size(); i++) {
            if (tags.get(i).size() > 1) {
                hasAnd = true;
            }
            andQuery.append(" OR jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& ?");
        }

        if (!hasAnd) {
            String[] orList = new String[tags.size()];
            for (int i = 0; i < tags.size(); i++) {
                orList[i] = tags.get(i).get(0);
            }

            query = new SQLQuery(" (jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??| ?)", SQLQuery.createSQLArray(orList, "text",dataSource));
        } else {
            query = new SQLQuery("(" + andQuery + ")");
            for (TagList tag : tags) {
                query.addParameter(SQLQuery.createSQLArray(tag.toArray(new String[tag.size()]), "text",dataSource));
            }
        }

        return query;
    }

    private static SQLQuery generateCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery,
                                                    SQLQuery secondaryQuery, DataSource dataSource)
            throws SQLException {
        final SQLQuery query = new SQLQuery();
        query.append("WITH features(jsondata, geojson, geo) AS (");
        query.append("SELECT jsondata, geojson, geo FROM ${schema}.${table} WHERE");
        query.append(indexedQuery);
        query.append(")");
        query.append("SELECT");
        query.append(SQLQuery.selectJson(event.getSelection(),dataSource));

        if (event instanceof GetFeaturesByBBoxEvent) {
            query.append(",");
            query.append(geometrySelectorForEvent((GetFeaturesByBBoxEvent) event));
        } else {
            query.append(",geojson");
        }

        query.append("FROM features WHERE");
        query.append(secondaryQuery);
        query.append("LIMIT ?", event.getLimit());
        return query;
    }

    /**
     * Returns the query, which will contains the geometry object.
     */
    private static SQLQuery geometrySelectorForEvent(final GetFeaturesByBBoxEvent event) {
        final long simplificationLevel = Optional.ofNullable(event.getSimplificationLevel()).orElse(0L);
        final double pixelSize = (double) EQUATOR_LENGTH / (TILE_SIZE << simplificationLevel);

        if (!event.getClip()) {
            if (simplificationLevel <= 0) {
                return new SQLQuery("geojson");

            }
            return new SQLQuery("ST_AsGeoJson(ST_Transform(ST_MakeValid(ST_SnapToGrid(ST_Force2D(ST_Transform(geo,3857)),?)),4326))", pixelSize);
        }

        final BBox bbox = event.getBbox();
        if (simplificationLevel <= 0) {
            return new SQLQuery("ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo),ST_MakeEnvelope(?,?,?,?,4326)))",
                    bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
        }

        return new SQLQuery(
                "ST_AsGeoJson(ST_Intersection(ST_Transform(ST_MakeValid(ST_SnapToGrid(ST_Force2D(ST_Transform(geo,3857)),?)),4326),ST_MakeEnvelope(?,?,?,?,4326)))",
                pixelSize, bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
    }

    protected static SQLQuery generateSearchQuery(final QueryEvent event, final DataSource dataSource)
            throws SQLException {
        final SQLQuery propertiesQuery = generatePropertiesQuery(event.getPropertiesQuery());
        final SQLQuery tagsQuery = generateTagsQuery(event.getTags(),dataSource);

        return SQLQuery.join("AND", propertiesQuery, tagsQuery);
    }

    protected static SQLQuery generateIDXStatusQuery(final String space){
        return new SQLQuery("SELECT idx_available FROM "+IDX_STATUS_TABLE+" WHERE spaceid=?", space);
    }
}