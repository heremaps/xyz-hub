package com.here.xyz.psql;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.responses.XyzError;

public class QuadClustering {
    public static final String QUAD = "quad";
    public static final String QUADMODE_REAL = "real";
    public static final String QUADMODE_ESTIMATED = "estimated";
    public static final String QUADMODE_MIXED = "mixed";

    protected static void checkQuadInput(String quadMode, int resolution, GetFeaturesByBBoxEvent event, String streamId,
                                         PSQLXyzConnector connector) throws
            ErrorResponseException{
        if(quadMode != null && (!quadMode.equalsIgnoreCase(QuadClustering.QUADMODE_REAL) && !quadMode.equalsIgnoreCase(QuadClustering.QUADMODE_ESTIMATED) && !quadMode.equalsIgnoreCase(QuadClustering.QUADMODE_MIXED)) )
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Unknown clustering.quadmode="+quadMode+". Available are: ["+ QuadClustering.QUADMODE_REAL +","+ QuadClustering.QUADMODE_ESTIMATED+","+ QuadClustering.QUADMODE_MIXED+"]!");
        if(resolution > 5)
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. clustering.resolution="+resolution+" to high. 5 is maximum!");

        if(event.getPropertiesQuery() != null && event.getPropertiesQuery() .get(0).size() != 1)
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Only one Property is allowed");

        if (!Capabilities.canSearchFor(event.getSpace(), event.getPropertiesQuery(), connector)) {
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Invalid request parameters. Search for the provided properties is not supported for this space.");
        }
    }

    public static SQLQuery generateQuadClusteringSQL(String schema, String space, int resolution, String quadMode, String propQuery, WebMercatorTile tile) {
        SQLQuery query = new SQLQuery("");

        String realCountCondition = "";
        String pureEstimation = "";
        String estClac = "cond_est_cnt";

        if(quadMode == null)
            quadMode = QuadClustering.QUADMODE_MIXED;

        switch (quadMode) {
            case QuadClustering.QUADMODE_REAL:
                realCountCondition = "TRUE";
                pureEstimation = "_postgis_selectivity( '"+schema+".\""+space+"\"'::regclass, 'geo',qkbbox)";
                break;
            case QuadClustering.QUADMODE_ESTIMATED:
            case QuadClustering.QUADMODE_MIXED:

                if(quadMode.equalsIgnoreCase(QuadClustering.QUADMODE_MIXED)) {
                    if (propQuery != null) {
                        realCountCondition = "cond_est_cnt < 100 AND est_cnt < 2000000";
                    } else {
                        realCountCondition = "cond_est_cnt < (10000 / est_cnt) AND est_cnt < 2000000";
                    }
                }else
                    realCountCondition = "FALSE";

                if(propQuery != null){
                    pureEstimation =
                            "              (SELECT xyz_count_estimation(concat(" +
                                    "                 'select 1 from "+ schema+".\""+space+"\""+
                                    "                  WHERE ST_Intersects(geo, xyz_qk_qk2bbox(''',qk,''')) "+
                                    " AND "+
                                    propQuery.replaceAll("'","''")+
                                    "')))";
                }else{
                    pureEstimation = "_postgis_selectivity( '"+schema+".\""+space+"\"'::regclass, 'geo',qkbbox)";
                    estClac += " * est_cnt ";
                }
                break;
        }

        query.append(
                "WITH stats AS("+
                        "    SELECT reltuples as est_cnt FROM pg_class WHERE oid = '"+schema+".\""+space+"\"'::regclass"+
                        ")"+
                        "SELECT * from ("+
                        "SELECT  (SELECT concat('{\"id\": \"',ceil(random()*10000000),'\", \"type\": \"Feature\""+
                        "       ,\"properties\": {\"count\": ',cnt_bbox_est,',\"qk\":\"',qk,'\""+
                        "       ,\"xyz\":\"',qkxyz,'\" ,\"estimated\":',to_jsonb(NOT("+realCountCondition+")),',\"total_count\":',est_cnt::bigint,',\"equipartition_count\":',"+
                        "          (floor((est_cnt/POW(2,"+(tile.level+1)+")/POW(4,"+resolution+")))),'}}')::jsonb) as properties,"+
                        "    (CASE WHEN cnt_bbox_est != 0"+
                        "        THEN"+
                        "            (SELECT ST_AsGeojson( ST_Buffer(qkbbox,-0.01/"+tile.level+")) ::jsonb)"+
                        "        ELSE"+
                        "            NULL::jsonb"+
                        "        END "+
                        "    ) as geojson"+
                        "    FROM "+
                        "    (SELECT cond_est_cnt,qk,qkbbox,qkxyz,"+
                        "        ("+
                        "        CASE WHEN "+realCountCondition+" THEN "+
                        "            (select count(1) from "+ schema+".\""+space+"\""+
                        "                WHERE ST_Intersects(geo, qkbbox)");
        if(propQuery != null) {
            query.append(" AND ");
            query.append(propQuery);
        }
        query.append(")"+
                "         ELSE "+
                "          "+estClac+""+
                "        END)::bigint as cnt_bbox_est"+
                "    FROM stats,"+
                "        (SELECT *,"+
                "              ("+pureEstimation+") as cond_est_cnt "+
                "            from("+
                "            SELECT qk, xyz_qk_qk2bbox( qk ) as qkbbox, xyz_qk_qk2lrc(qk) as qkxyz from ("+
                "                SELECT unnest(xyz_qk_child_calculation('"+tile.asQuadkey()+"',"+resolution+",null)) as qk"+
                "            )a"+
                "        ) b"+
                "    )c"+
                ")x, stats ) d WHERE geojson IS NOT null ");
        return query;
    }
}
