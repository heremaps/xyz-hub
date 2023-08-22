/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.psql.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.PSQLConfig.AESGCMHelper;
import com.here.xyz.psql.datasource.DataSourceProvider;
import com.here.xyz.psql.query.helpers.GetIndexList;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.XyzError;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IterateFeaturesSorted extends IterateFeatures {

  public IterateFeaturesSorted(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  public FeatureCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    IterateFeaturesEvent event = tmpEvent;

    boolean hasHandle = (event.getHandle() != null);
    String tableName = XyzEventBasedQueryRunner.readTableFromEvent(event);

    if( !hasHandle )  // decrypt handle and configure event
    {
      if( event.getPart() != null && event.getPart()[0] == -1 )
        return new GetIterationHandles(event).run(dbHandler.getDataSourceProvider());

      if (!canSortBy(tableName, event.getSort(), dbHandler)) {
        throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT,
            "Invalid request parameters. Sorting by for the provided properties is not supported for this space.");
      }

      event.setSort( translateSortSysValues( event.getSort() ));
    }
    else if( !event.getHandle().startsWith( HPREFIX ) )
      throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "Invalid request parameter. handle is corrupted");
    else
      try {
        setEventValuesFromHandle(event, decrypt(chrD(event.getHandle().substring(HPREFIX.length())), HANDLE_ENCRYPTION_PHRASE));
      }
      catch (GeneralSecurityException | IllegalArgumentException | JsonProcessingException e) {
        throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "Invalid request parameter. handle is corrupted");
      }

    return super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    return super.buildQuery(event);
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    return super.handle(rs);
  }







  private static boolean canSortBy(String tableName, List<String> sort, PSQLXyzConnector dbHandler)
  {
    if (sort == null || sort.isEmpty() ) return true;

    try
    {
     String normalizedSortProp = "o:" + IdxMaintenance.normalizedSortProperties(sort);

     switch( normalizedSortProp.toLowerCase() ) { case "o:f.id" : case "o:f.createdat" : case "o:f.updatedat" : return true; }

     List<String> indices = new GetIndexList(tableName).run(dbHandler.getDataSourceProvider());

     if (indices == null) return true; // The table is small and not indexed. It's not listed in the xyz_idxs_status table

     for( String idx : indices )
      if( idx.startsWith( normalizedSortProp) ) return true;

     return false;
    }
    catch (Exception e)
    { // In all cases, when something with the check went wrong, allow the sort
    }

    return true;
  }

  private static void setEventValuesFromHandle(IterateFeaturesEvent event, String handle) throws JsonProcessingException
  {
    ObjectMapper om = new ObjectMapper();
    JsonNode jn = om.readTree(handle);
    String ps = jn.get("p").toString();
    String ts = jn.get("t").toString();
    String ms = jn.get("m").toString();
    PropertiesQuery pq = om.readValue( ps, PropertiesQuery.class );
    TagsQuery tq = om.readValue( ts, TagsQuery.class );
    Integer[] part = om.readValue(ms,Integer[].class);

    event.setPart(part);
    event.setPropertiesQuery(pq);
    event.setTags(tq);
    event.setHandle(handle);
  }

  private static List<String> translateSortSysValues(List<String> sort)
  { if( sort == null ) return null;
    List<String> r = new ArrayList<String>();
    for( String f : sort )      // f. sysval replacements - f.sysval:desc -> sysval:desc
     if( f.toLowerCase().startsWith("f.createdat" ) || f.toLowerCase().startsWith("f.updatedat" ) )
      r.add( f.replaceFirst("^f\\.", "properties.@ns:com:here:xyz.") );
     else
      r.add( f.replaceFirst("^f\\.", "") );

    return r;
  }

  private static String chrD( String s ) { return s.replace('-','+').replace('_','/').replace('.','='); }

  @SuppressWarnings("unused")
  private static String decrypt(String encryptedtext, String phrase) throws GeneralSecurityException {
    return AESGCMHelper.getInstance(phrase).decrypt(encryptedtext);
  }

  private static class GetIterationHandles extends XyzQueryRunner<IterateFeaturesEvent, FeatureCollection> {

    //TODO: Remove after refactoring
    private IterateFeaturesEvent tmpEvent;

    public GetIterationHandles(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
      super(event);
      setUseReadReplica(true);
      this.tmpEvent = event;
    }

    @Override
    protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
      //FIXME: Do not manipulate the incoming event
      event.setPart(null);
      event.setTags(null);

      return new SQLQuery( "with  "
          + "idata as "
          + "(select min(id) as id from ${schema}.${table} "
          + "  union  "
          + "   select id from ${schema}.${table} "
          + "    tablesample system((select least((100000 / greatest(reltuples, 1)), 100) from pg_catalog.pg_class where oid = '${schema}.${table}'::regclass))" //-- repeatable ( 0.7 )
          + "), "
          + "iidata   as (select id, ntile(#{nrHandles}) over (order by id) as bucket from idata), "
          + "iiidata  as (select min(id) as id, bucket from iidata group by bucket), "
          + "iiiidata as (select bucket, id as i_from, lead(id, 1) over (order by id) as i_to from iiidata) "
          + "select jsonb_set('{\"type\":\"Feature\",\"properties\":{}}', '{properties, handles}', jsonb_agg(jsonb_build_array(bucket, i_from, i_to))), '{\"type\":\"Point\",\"coordinates\":[]}', null from iiiidata")
          .withVariable(SCHEMA, getSchema())
          .withVariable(TABLE, getDefaultTable(event))
          .withNamedParameter("nrHandles", event.getPart()[1]);
    }

    @Override
    public FeatureCollection handle(ResultSet rs) throws SQLException {
      //FIXME: Do not use FeatureCollection as response vehicle
      FeatureCollection cl = dbHandler.defaultFeatureResultSetHandler(rs);
      List<List<Object>> hdata;
      try {
        hdata = cl.getFeatures().get(0).getProperties().get("handles");
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      for( List<Object> entry : hdata )
      {
        tmpEvent.setPropertiesQuery(null);
        if( entry.get(2) != null )
        { PropertyQuery pqry = new PropertyQuery();
          pqry.setKey("id");
          pqry.setOperation(QueryOperation.LESS_THAN);
          pqry.setValues(Arrays.asList( entry.get(2)) );
          PropertiesQuery pqs = new PropertiesQuery();
          PropertyQueryList pql = new PropertyQueryList();
          pql.add( pqry );
          pqs.add( pql );

          tmpEvent.setPropertiesQuery( pqs );
        }
        try {
          entry.set(0, createHandle(tmpEvent, DhString.format("{\"h\":\"%s\",\"s\":[]}", entry.get(1).toString())));
        }
        catch (Exception e) {
          throw new RuntimeException("Error creating handle.", e);
        }
      }
      return cl;
    }
  }

  private static class IdxMaintenance // idx Maintenance
  {
  /*
    private static String crtSortIdxSql = "create index \"%1$s\" on ${schema}.${table} using btree ( %2$s (jsondata->>'id') %3$s ) ",
                          crtSortIdxCommentSql = "comment on index ${schema}.\"%1$s\" is '%2$s'";

    // create index u. comment sql statments.
    private static String[] buildCreateIndexSql(List<String> sortby,String spaceName)
    { if (sortby == null || sortby.size() == 0) return null;

      String btreeClause = "", idxComment = "", direction = "", idxPostFix = "";
      boolean dFlip = false;

      for( int i = 0; i < sortby.size(); i++ )
      {
       String s = sortby.get(i),
              pname = s.replaceAll(":(?i)(asc|desc)$", "");

       if( i == 0 && isDescending(s) ) dFlip = true;  //normalize idx, first is always ascending

       if( isDescending(s) != dFlip )
       { direction = "desc"; idxPostFix += "0"; }
       else
       { direction = ""; idxPostFix += "1"; }

       btreeClause += DhString.format("%s %s,", jpathFromSortProperty(s), direction);
       idxComment += DhString.format("%s%s%s%s", idxComment.length()> 0 ? ",":"", pname, direction.length()>0 ? ":" : "", direction);
      }

      String hash = DigestUtils.md5Hex(idxComment).substring(0, 7),
             idxName = DhString.format("idx_%s_%s_o%s",spaceName,hash,idxPostFix);
      return new String[] { DhString.format( crtSortIdxSql, idxName, btreeClause, direction ),
                            DhString.format( crtSortIdxCommentSql, idxName, idxComment ) };
     }
  */

     public static String normalizedSortProperties(List<String> sortby) // retuns feld1,feld2:ord2,feld3:ord3 where sortorder feld1 is always ":asc"
     { if (sortby == null || sortby.size() == 0) return null;

       String normalizedSortProp = "";
       boolean dFlip = false;

       for( int i = 0; i < sortby.size(); i++ )
       {
        String direction = "",
               s = sortby.get(i),
               pname = s.replaceAll(":(?i)(asc|desc)$", "").replaceAll("^properties\\.","");

        if( i == 0 && isDescending(s) ) dFlip = true;  //normalize idx, first is always ascending

        if( isDescending(s) != dFlip )
         direction = "desc";

         normalizedSortProp += DhString.format("%s%s%s%s", normalizedSortProp.length()> 0 ? ",":"", pname, direction.length() > 0 ? ":" : "", direction);
       }

       return normalizedSortProp;
    }

   }
}
