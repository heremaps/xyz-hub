/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.tools.DhString;

public class SQLQueryBuilder {

  private static final Integer BIG_SPACE_THRESHOLD = 10000;

    public static SQLQuery buildGetStatisticsQuery(Event event, PSQLConfig config, boolean historyMode) {
        String function;
        if(event instanceof GetHistoryStatisticsEvent)
            function = "xyz_statistic_history";
        else
            function = "xyz_statistic_space";
        final String schema = config.getDatabaseSettings().getSchema();
        final String table = config.readTableFromEvent(event) + (!historyMode ? "" : "_hst");

        return new SQLQuery("SELECT * from " + schema + "."+function+"('" + schema + "','" + table + "')");
    }

    public static SQLQuery buildGetNextVersionQuery(String table) {
        return new SQLQuery("SELECT nextval('${schema}.\"" + table.replaceAll("-","_") + "_hst_seq\"')");
    }

  /***************************************** CLUSTERING END **************************************************/


  public static SQLQuery buildMvtEncapsuledQuery( String spaceId, SQLQuery dataQry, WebMercatorTile mvtTile, HQuad hereTile, BBox eventBbox, int mvtMargin, boolean bFlattend )
    {
      //TODO: The following is a workaround for backwards-compatibility and can be removed after completion of refactoring
      dataQry.replaceUnnamedParameters();
      dataQry.replaceFragments();
      dataQry.replaceNamedParameters();
      int extend = 4096, buffer = (extend / WebMercatorTile.TileSizeInPixel) * mvtMargin;
      BBox b = ( mvtTile != null ? mvtTile.getBBox(false) : ( hereTile != null ? hereTile.getBoundingBox() : eventBbox) ); // pg ST_AsMVTGeom expects tiles bbox without buffer.

      SQLQuery r = new SQLQuery( DhString.format( hereTile == null ? TweaksSQL.mvtBeginSql : TweaksSQL.hrtBeginSql ,
                                   DhString.format( TweaksSQL.requestedTileBoundsSql , b.minLon(), b.minLat(), b.maxLon(), b.maxLat() ),
                                   (!bFlattend) ? TweaksSQL.mvtPropertiesSql : TweaksSQL.mvtPropertiesFlattenSql,
                                   extend,
                                   buffer )
                               );
      r.append(dataQry);
      r.append( DhString.format( TweaksSQL.mvtEndSql, spaceId ));
      return r;
    }

    /***************************************** TWEAKS END **************************************************/

  public static SQLQuery buildDeleteFeaturesByTagQuery(boolean includeOldStates, SQLQuery searchQuery) {

        final SQLQuery query;

        if (searchQuery != null) {
            query = new SQLQuery("DELETE FROM ${schema}.${table} WHERE");
            query.append(searchQuery);
        } else {
            query = new SQLQuery("TRUNCATE ${schema}.${table}");
        }

        if (searchQuery != null && includeOldStates)
            query.append(" RETURNING jsondata->'id' as id, replace(ST_AsGeojson(ST_Force3D(geo),"+ GetFeaturesByBBox.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') as geometry");

        return query;
    }

    public static SQLQuery buildHistoryQuery(IterateHistoryEvent event) {
        SQLQuery query = new SQLQuery(
                "SELECT vid," +
                "   jsondata->>'id' as id," +
                "   jsondata || jsonb_strip_nulls(jsonb_build_object('geometry',ST_AsGeoJSON(geo)::jsonb)) As feature,"+
                "   ( CASE " +
                "      WHEN (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS true) THEN 'DELETED'" +
                "      WHEN (jsondata->'properties'->'@ns:com:here:xyz'->'puuid' IS NULL" +
                "          AND" +
                "          (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS NOT true)" +
                "      ) THEN 'INSERTED'" +
                "      ELSE 'UPDATED' " +
                "   END) as operation," +
                "   jsondata->'properties'->'@ns:com:here:xyz'->>'version' as version" +
                "       FROM ${schema}.${hsttable}" +
                "           WHERE 1=1");

        if (event.getPageToken() != null) {
            query.append(
               "   AND vid > ?",event.getPageToken());
        }

        if(event.getStartVersion() != 0) {
            query.append("  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' >= to_jsonb(?::numeric)",event.getStartVersion());
        }

        if(event.getEndVersion() != 0)
            query.append("  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' <= to_jsonb(?::numeric)", event.getEndVersion());

        query.append(" ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'version' , " +
                "jsondata->>'id'");

        if(event.getLimit() != 0)
            query.append("LIMIT ?", event.getLimit());

        return query;
    }

    public static SQLQuery buildSquashHistoryQuery(IterateHistoryEvent event){
        SQLQuery query = new SQLQuery(
                "SELECT distinct ON (jsondata->>'id') jsondata->>'id'," +
                "   jsondata->>'id' as id," +
                "   jsondata || jsonb_strip_nulls(jsonb_build_object('geometry',ST_AsGeoJSON(geo)::jsonb)) As feature,"+
                "   ( CASE " +
                "      WHEN (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS true) THEN 'DELETED'" +
                "      WHEN (jsondata->'properties'->'@ns:com:here:xyz'->'puuid' IS NULL" +
                "          AND" +
                "          (COALESCE((jsondata->'properties'->'@ns:com:here:xyz'->>'deleted')::boolean, false) IS NOT true)" +
                "      ) THEN 'INSERTED'" +
                "      ELSE 'UPDATED' " +
                "   END) as operation, " +
                "   jsondata->'properties'->'@ns:com:here:xyz'->>'version' as version" +
                "       FROM ${schema}.${hsttable}" +
                "           WHERE 1=1");

        if (event.getPageToken() != null) {
            query.append(
                    "   AND jsondata->>'id' > ?",event.getPageToken());
        }

        if(event.getStartVersion() != 0) {
            query.append(
                "  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' >= to_jsonb(?::numeric)",event.getStartVersion());
        }

        if(event.getEndVersion() != 0)
            query.append(
                "  AND jsondata->'properties'->'@ns:com:here:xyz'->'version' <= to_jsonb(?::numeric)", event.getEndVersion());

        query.append(
                "   order by jsondata->>'id'," +
                "jsondata->'properties'->'@ns:com:here:xyz'->'version' DESC");

        if(event.getLimit() != 0)
            query.append("LIMIT ?", event.getLimit());

        return query;
    }

    public static SQLQuery buildLatestHistoryQuery(IterateFeaturesEvent event) {
        SQLQuery query = new SQLQuery("select jsondata#-'{properties,@ns:com:here:xyz,lastVersion}' as jsondata, geo, id " +
                "FROM(" +
                "   select distinct ON (jsondata->>'id') jsondata->>'id' as id," +
                "   jsondata->'properties'->'@ns:com:here:xyz'->'deleted' as deleted," +
                "   jsondata," +
                "   replace(ST_AsGeojson(ST_Force3D(geo),"+ GetFeaturesByBBox.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') as geo"+
                "       FROM ${schema}.${hsttable}" +
                "   WHERE 1=1" );

        if (event.getHandle() != null) {
            query.append(
                    "   AND jsondata->>'id' > ?",event.getHandle());
        }

        query.append(
                "   AND((" +
                "       jsondata->'properties'->'@ns:com:here:xyz'->'version' <= to_jsonb(?::numeric)", event.getV());
        query.append(
                "       AND " +
                "       jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb "+
                "   )"+
                "OR( "+
                "       jsondata->'properties'->'@ns:com:here:xyz'->'lastVersion' <= to_jsonb(?::numeric)", event.getV());
        query.append(
                "       AND " +
                "       jsondata->'properties'->'@ns:com:here:xyz'->'version' = '0'::jsonb " +
                "))");
        query.append(
                "   order by jsondata->>'id'," +
                        "jsondata->'properties'->'@ns:com:here:xyz'->'version' DESC ");
        query.append(
                ")A WHERE deleted IS NULL  ");
        if(event.getLimit() != 0)
            query.append("LIMIT ?", event.getLimit());

        return query;
    }


  protected static SQLQuery generateLoadOldFeaturesQuery(ModifyFeaturesEvent event, final String[] idsToFetch) {
        return new SQLQuery("SELECT jsondata, replace(ST_AsGeojson(ST_Force3D(geo),"+ GetFeaturesByBBox.GEOMETRY_DECIMAL_DIGITS+"),'nan','0') FROM ${schema}.${table} WHERE " + buildIdFragment(event) + " = ANY(?)", (Object) idsToFetch);
    }

    private static String buildIdFragment(ContextAwareEvent event) {
      return DatabaseHandler.readVersionsToKeep(event) < 1 ? "jsondata->>'id'" : "id";
    }

    protected static SQLQuery generateIDXStatusQuery(final String space){
        return new SQLQuery("SELECT idx_available FROM "+ ModifySpace.IDX_STATUS_TABLE+" WHERE spaceid=? AND count >=?", space, BIG_SPACE_THRESHOLD);
    }

  protected static SQLQuery buildMultiModalInsertStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
      return new SQLQuery("SELECT xyz_write_versioned_modification_operation(#{id}, #{version}, #{operation}, #{jsondata}, #{geo}, "
          + "#{schema}, #{table}, #{concurrencyCheck})")
          .withNamedParameter("schema", dbHandler.config.getDatabaseSettings().getSchema())
          .withNamedParameter("table", dbHandler.config.readTableFromEvent(event))
          .withNamedParameter("concurrencyCheck", event.getEnableUUID());
  }

  protected static SQLQuery buildInsertStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
    return setWriteQueryComponents(new SQLQuery("${{geoWith}} INSERT INTO ${schema}.${table} (id, version, operation, jsondata, geo) "
        + "VALUES("
        + "#{id}, "
        + "#{version}, "
        + "#{operation}, "
        + "#{jsondata}::jsonb, "
        + "${{geo}}"
        + ")"), dbHandler, event);
  }

  protected static SQLQuery buildUpdateStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
      return setWriteQueryComponents(new SQLQuery("${{geoWith}} UPDATE ${schema}.${table} SET "
          + "version = #{version}, "
          + "operation = #{operation}, "
          + "jsondata = #{jsondata}::jsonb, "
          + "geo = (${{geo}}) "
          + "WHERE ${{idColumn}} = #{id} ${{uuidCheck}}"), dbHandler, event)
          .withQueryFragment("uuidCheck", buildUuidCheckFragment(event))
          .withQueryFragment("idColumn", buildIdFragment(event));
  }

  private static SQLQuery setWriteQueryComponents(SQLQuery writeQuery, DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
      return setTableVariables(writeQuery
          .withQueryFragment("geoWith", "WITH in_params AS (SELECT #{geo} as geo)")
          .withQueryFragment("geo", "CASE WHEN (SELECT geo FROM in_params)::geometry IS NULL THEN NULL ELSE "
              + "ST_Force3D(ST_GeomFromWKB((SELECT geo FROM in_params)::BYTEA, 4326)) END"), dbHandler, event);
  }

  private static SQLQuery setTableVariables(SQLQuery writeQuery, DatabaseHandler dbHandler, ModifyFeaturesEvent event) {
    return writeQuery
        .withVariable("schema", dbHandler.config.getDatabaseSettings().getSchema())
        .withVariable("table", dbHandler.config.readTableFromEvent(event));
  }

  protected static SQLQuery buildDeleteStmtQuery(DatabaseHandler dbHandler, ModifyFeaturesEvent event, long version) {
      //If versioning is enabled, perform an update instead of a deletion. The trigger will finally delete the row.
      //NOTE: The following is a temporary implementation for backwards compatibility for old table structures
      boolean oldTableStyle = DatabaseHandler.readVersionsToKeep(event) < 1;
      SQLQuery query =
          version == -1 || !oldTableStyle && !event.isEnableGlobalVersioning()
          ? new SQLQuery("DELETE FROM ${schema}.${table} WHERE ${{idColumn}} = #{id} ${{uuidCheck}}")
              .withQueryFragment("idColumn", buildIdFragment(event))
          //Use UPDATE instead of DELETE to inject a version and the deleted flag. The deletion gets performed afterwards by the trigger.
          : new SQLQuery("UPDATE ${schema}.${table} "
              + "SET jsondata = jsonb_set(jsondata, '{properties,@ns:com:here:xyz}', "
              + "((jsondata->'properties'->'@ns:com:here:xyz')::JSONB "
              + "|| format('{\"uuid\": \"%s_deleted\"}', jsondata->'properties'->'@ns:com:here:xyz'->>'uuid')::JSONB) "
              + "|| format('{\"version\": %s}', #{version})::JSONB "
              + "|| format('{\"updatedAt\": %s}', (extract(epoch from now()) * 1000)::BIGINT)::JSONB "
              + "|| '{\"deleted\": true}'::JSONB) "
              + "WHERE ${{idColumn}} = #{id} ${{uuidCheck}}")
              .withNamedParameter("version", version)
              .withQueryFragment("idColumn", buildIdFragment(event));

      return setTableVariables(
          query.withQueryFragment("uuidCheck", buildUuidCheckFragment(event)),
          dbHandler,
          event);
  }

  private static String buildUuidCheckFragment(ModifyFeaturesEvent event) {
    //NOTE: The following is a temporary implementation for backwards compatibility for old spaces
    return event.getEnableUUID() ? " AND (#{puuid}::TEXT IS NULL OR jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = #{puuid})" : "";
    //return event.getEnableUUID() ? (DatabaseHandler.readVersionsToKeep(event) > 0 ? " AND (CASE WHEN #{baseVersion}::BIGINT IS NULL THEN next_version = max_bigint() ELSE version = #{baseVersion} END)" : " AND (#{puuid}::TEXT IS NULL OR jsondata->'properties'->'@ns:com:here:xyz'->>'uuid' = #{puuid})") : "";
  }

  public static String deleteOldHistoryEntries(final String schema, final String table, long maxAllowedVersion){
        /** Delete rows which have a too old version - only used if maxVersionCount is set */

        String deleteOldHistoryEntriesSQL =
                "DELETE FROM ${schema}.${table} t " +
                "USING (" +
                "   SELECT vid " +
                "   FROM   ${schema}.${table} " +
                "   WHERE  1=1 " +
                "     AND jsondata->'properties'->'@ns:com:here:xyz'->'version' = '0'::jsonb " +
                "     AND jsondata->>'id' IN ( " +
                "     SELECT jsondata->>'id' FROM ${schema}.${table} " +
                "        WHERE 1=1    " +
                "        AND (jsondata->'properties'->'@ns:com:here:xyz'->'version' <= '"+maxAllowedVersion+"'::jsonb " +
                "        AND jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb)" +
                ")" +
                "   ORDER  BY vid" +
                "   FOR    UPDATE" +
                "   ) del " +
                "WHERE  t.vid = del.vid;";

        return SQLQuery.replaceVars(deleteOldHistoryEntriesSQL, schema, table);
    }

    public static String flagOutdatedHistoryEntries(final String schema, final String table, long maxAllowedVersion){
        /** Set version=0 for objects which are too old - only used if maxVersionCount is set */

        String flagOutdatedHistoryEntries =
                "UPDATE ${schema}.${table} " +
                "SET jsondata = jsonb_set(jsondata,'{properties,@ns:com:here:xyz}', ( " +
                "   (jsondata->'properties'->'@ns:com:here:xyz')::jsonb) " +
                "   || format('{\"lastVersion\" : %s }',(jsondata->'properties'->'@ns:com:here:xyz'->'version'))::jsonb"+
                "   || '{\"version\": 0}'::jsonb" +
                ") " +
                "    WHERE " +
                "1=1" +
                "AND jsondata->'properties'->'@ns:com:here:xyz'->'version' <= '"+maxAllowedVersion+"'::jsonb " +
                "AND jsondata->'properties'->'@ns:com:here:xyz'->'version' > '0'::jsonb;";

        return SQLQuery.replaceVars(flagOutdatedHistoryEntries, schema, table);
    }

    public static String deleteHistoryEntriesWithDeleteFlag(final String schema, final String table){
        /** Remove deleted objects with version 0 - only used if maxVersionCount is set */
        String deleteHistoryEntriesWithDeleteFlag =
                "DELETE FROM ${schema}.${table} " +
                "WHERE " +
                "   1=1" +
                "   AND jsondata->'properties'->'@ns:com:here:xyz'->'version' = '0'::jsonb " +
                "   AND jsondata->'properties'->'@ns:com:here:xyz'->'deleted' = 'true'::jsonb;";

        return SQLQuery.replaceVars(deleteHistoryEntriesWithDeleteFlag, schema, table);
    }

    protected static String[] deleteHistoryTriggerSQL(final String schema, final String table){
        String[] sqls= new String[2];
        /** Old naming */
        String oldDeleteHistoryTriggerSQL = "DROP TRIGGER IF EXISTS TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER ON  ${schema}.${table};";
        /** New naming */
        String deleteHistoryTriggerSQL = "DROP TRIGGER IF EXISTS \"TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER\" ON  ${schema}.${table};";

        sqls[0] = SQLQuery.replaceVars(oldDeleteHistoryTriggerSQL, schema, table);
        sqls[1] = SQLQuery.replaceVars(deleteHistoryTriggerSQL, schema, table);
        return sqls;
    }

    protected static String addHistoryTriggerSQL(final String schema, final String table, final Integer maxVersionCount, final boolean compactHistory, final boolean isEnableGlobalVersioning){
        String triggerSQL = "";
        String triggerFunction = "xyz_trigger_historywriter";
        String triggerActions = "UPDATE OR DELETE ON";
        String tiggerEvent = "BEFORE";

        if(isEnableGlobalVersioning == true){
            triggerFunction = "xyz_trigger_historywriter_versioned";
            tiggerEvent = "AFTER";
            triggerActions = "INSERT OR UPDATE ON";
        }else{
            if(compactHistory == false){
                triggerFunction = "xyz_trigger_historywriter_full";
                triggerActions = "INSERT OR UPDATE OR DELETE ON";
            }
        }

        triggerSQL = "CREATE TRIGGER \"TR_"+table.replaceAll("-","_")+"_HISTORY_WRITER\" " +
                tiggerEvent+" "+triggerActions+" ${schema}.${table} " +
                " FOR EACH ROW " +
                "EXECUTE PROCEDURE "+triggerFunction;
        triggerSQL+=
                maxVersionCount == null ? "()" : "('"+maxVersionCount+"')";

        return SQLQuery.replaceVars(triggerSQL, schema, table);
    }

  protected static SQLQuery buildAddSubscriptionQuery(String space, String schemaName, String tableName) {
        String theSql =
          "insert into xyz_config.space_meta  ( id, schem, h_id, meta ) values(?,?,?,'{\"subscriptions\":true}' )"
         +" on conflict (id,schem) do "
         +"  update set meta = xyz_config.space_meta.meta || excluded.meta ";

        return new SQLQuery(theSql, space,schemaName,tableName);
	}

	protected static SQLQuery buildSetReplicaIdentIfNeeded() {
        String theSql =
          "do "
         +"$body$ "
         +"declare "
         +" mrec record; "
         +"begin "
         +" for mrec in "
         +"  select l.schem,l.h_id --, l.meta, r.relreplident, r.oid  "
         +"  from xyz_config.space_meta l left join pg_class r on ( r.oid = to_regclass(schem || '.' || '\"' || h_id || '\"') ) "
         +"  where 1 = 1 "
         +"    and l.meta->'subscriptions' = to_jsonb( true ) "
         +"        and r.relreplident is not null "
         +"      and r.relreplident != 'f' "
         +"  loop "
         +"     execute format('alter table %I.%I replica identity full', mrec.schem, mrec.h_id); "
         +"  end loop; "
         +"end; "
         +"$body$  "
         +"language plpgsql ";

        return new SQLQuery(theSql);
	}

    protected static String getReplicaIdentity(final String schema, final String table)
    { return String.format("select relreplident from pg_class where oid = to_regclass( '\"%s\".\"%s\"' )",schema,table); }

    protected static String setReplicaIdentity(final String schema, final String table)
    { return String.format("alter table \"%s\".\"%s\" replica identity full",schema,table); }

	public static SQLQuery buildRemoveSubscriptionQuery(String space, String schemaName) {
        String theSql =
          "update xyz_config.space_meta "
         +" set meta = meta - 'subscriptions' "
         +"where ( id, schem ) = ( ?, ? ) ";

        return new SQLQuery(theSql, space,schemaName );
	}
}
