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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

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
import com.here.xyz.psql.query.helpers.GetIndexList;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.XyzError;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

public class IterateFeaturesSorted extends IterateFeatures {
  public static final String SORTED_HANDLE_PREFIX = "h07~";
  private static String pg_hint_plan = "/*+ Set(seq_page_cost 100.0) IndexOnlyScan( ht1 ) */";
  private static String PropertyDoesNotExistIndikator = "#zJfCzPCz#";
  protected IterateFeaturesEvent tmpEvent; //TODO: Remove after refactoring

  public IterateFeaturesSorted(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    tmpEvent = event;
  }

  @Override
  public FeatureCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    IterateFeaturesEvent event = tmpEvent;

    checkCanSearchFor(event, dataSourceProvider);

    boolean hasHandle = (event.getHandle() != null);
    String tableName = XyzEventBasedQueryRunner.readTableFromEvent(event);

    if (!hasHandle) { // decrypt handle and configure event
      if (event.getPart() != null && event.getPart()[0] == -1)
        return new GetIterationHandles(event).withDataSourceProvider(dataSourceProvider).run();

      if (!canSortBy(tableName, event.getSort()))
        throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT,
            "Invalid request parameters. Sorting by for the provided properties is not supported for this space.");

      event.setSort(translateSortSysValues(event.getSort()));
    }
    else if (!event.getHandle().startsWith(SORTED_HANDLE_PREFIX))
      throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "Invalid request parameter. handle is corrupted");
    else
      try {
        setEventValuesFromHandle(event, decryptHandle(event.getHandle().substring(SORTED_HANDLE_PREFIX.length())));
      }
      catch (GeneralSecurityException | IllegalArgumentException | JsonProcessingException e) {
        throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "Invalid request parameter. handle is corrupted");
      }

    return super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    if (isCompositeQuery(event))
      return super.buildQuery(event);
    else
      return buildQueryForOrderBy(event);
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    FeatureCollection fc = super.handle(rs);

    if (fc.getHandle() != null) {
      //Extend handle and encrypt
      final String handle;
      try {
        handle = createHandle(tmpEvent, fc.getHandle());
      }
      catch (Exception e) {
        throw new RuntimeException(e); //TODO: Use ErrorResponseException here after refactoring of the base class
      }
      fc.setHandle(handle);
      fc.setNextPageToken(handle);
    }

    return fc;
  }

  /**
   * @deprecated Kept for backwards compatibility. Will be removed after refactoring.
   */
  @Deprecated
  public static boolean isOrderByEvent(IterateFeaturesEvent event) {
    return event.getSort() != null || hasPropertyQuery(event) || event.getPart() != null || event.getHandle() != null
        && event.getHandle().startsWith(SORTED_HANDLE_PREFIX);
  }

  private static boolean hasPropertyQuery(IterateFeaturesEvent event) {
    return event.getPropertiesQuery() != null && !event.getPropertiesQuery().isEmpty()
        && event.getPropertiesQuery().stream().anyMatch(pql -> !pql.isEmpty());
  }

  private SQLQuery buildQueryForOrderBy(IterateFeaturesEvent event) {
    SQLQuery partialQuery = buildPartialSortIterateQuery(event);

    String nextHandleJson = buildNextHandleAttribute(event.getSort(), isPartOverI(event));
    SQLQuery innerQry = new SQLQuery("WITH dt AS ("
        + " ${{partialQuery}} "
        + " ${{orderings}} "
        + ") "
        + "SELECT jsondata, geo, ${{nextHandleJson}} AS nxthandle "
        + "    FROM dt s JOIN ${schema}.${table} d ON ( s.i = d.i ) "
        + "    ORDER BY s.ord1, s.ord2")
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event));

    innerQry.setQueryFragment("partialQuery", partialQuery);
    innerQry.setQueryFragment("orderings", event.getHandle() == null ? "" : "ORDER BY ord1, ord2 limit #{limit}");
    innerQry.setNamedParameter("limit", limit);
    innerQry.setQueryFragment("nextHandleJson", nextHandleJson);


    //---------------------------------------------------------------------------------------------


    SQLQuery query = new SQLQuery("${{pgHintPlan}} "
        + "SELECT ${{selection}}, ${{geo}}, nxthandle FROM ("
        + "    ${{innerQuery}}"
        + ") o");
    query.setQueryFragment("pgHintPlan", pg_hint_plan);
    query.setQueryFragment("selection", buildSelectionFragment(event));
    query.setQueryFragment("geo", buildGeoFragment(event));
    query.setQueryFragment("innerQuery", innerQry);

    return query;
  }

  private SQLQuery buildPartialSortIterateQuery(IterateFeaturesEvent event) {
    SQLQuery searchQuery = generateSearchQuery(event);
    SQLQuery filterWhereClause = new SQLQuery("${{searchQuery}} ${{partialIterationModules}}");
    if (searchQuery != null)
      filterWhereClause.setQueryFragment("searchQuery", searchQuery);
    else
      filterWhereClause.setQueryFragment("searchQuery", "");

    //Extend the filterWhereClause by the necessary parts for partial iteration using modules
    Integer[] part = event.getPart();
    if (part != null && part.length == 2 && part[1] > 1) {
      filterWhereClause.setQueryFragment("partialIterationModules",
          new SQLQuery((searchQuery != null ? "AND " : "") + "(( i %% #{total} ) = #{partition})")
              .withNamedParameter("total", part[1])
              .withNamedParameter("partition", part[0] - 1));
    }

    String orderByClause = buildOrderByClause(event);
    List<String> continuationWhereClauses = event.getHandle() == null ? Collections.singletonList("")
        : buildContinuationConditions(event);

    List<SQLQuery> unionQueries = new ArrayList<>();
    for (int i = 0; i < continuationWhereClauses.size(); i++)
      unionQueries.add(new SQLQuery(buildPartialSortIterateSQL(i))
          .withQueryFragment("continuation", continuationWhereClauses.get(i)));

    SQLQuery partialQuery = SQLQuery.join(unionQueries, " UNION ALL ")
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event))
        .withQueryFragment("orderBy", orderByClause)
        .withNamedParameter("limit", limit)
        .withQueryFragment("filterWhereClause", filterWhereClause);

    return partialQuery;
  }

  private static String buildPartialSortIterateSQL(int ord1) {
    return "("
        + "    SELECT " + ord1 + "::integer AS ord1, row_number() over () ord2, i FROM (" //TODO: Rename ord1_i / ord2 to meaningful names
        + "        SELECT i FROM ${schema}.${table} ht1 "
        + "        WHERE ${{filterWhereClause}} ${{continuation}} ${{orderBy}} LIMIT #{limit}"
        + "    ) inr"
        + ")";
  }

  protected boolean isPartOverI(IterateFeaturesEvent event) {
    if (event.getHandle() != null)
      return (new JSONObject(event.getHandle())).has("i");

    return event.getPart() != null && event.getSort() == null && !hasSearch;
  }

  private static String buildNextHandleAttribute(List<String> sortby, boolean partOver_i)
  { String nhandle = !partOver_i ? "jsonb_set('{}','{h0}', jsondata->'id')" : "jsonb_set('{}','{i}',to_jsonb(s.i))",
           svalue = "";
    int hDepth = 1;

    if (sortby != null && sortby.size() > 0)
      for (String s : sortby)
      {
        svalue += DhString.format("%s\"%s\"", (svalue.length() == 0 ? "" : ","), s);

        nhandle = DhString.format("jsonb_set(%s,'{h%d}',coalesce(%s,'\"%s\"'::jsonb))", nhandle, hDepth++, jpathFromSortProperty(s), PropertyDoesNotExistIndikator);
      }

    return DhString.format("jsonb_set(%s,'{s}','[%s]')", nhandle, svalue);
  }

  private static List<String> buildContinuationConditions(IterateFeaturesEvent event)
  {
    String handle = event.getHandle();
   List<String> ret = new ArrayList<>(),
                sortby = convHandle2sortbyList( handle );
   JSONObject h = new JSONObject(handle);
   boolean descendingLast = false, sortbyIdUseCase = false;
   String sqlWhereContinuation = "";
   int hdix = 1;

   if( h.has("i") ) // partOver_i
   { ret.add(DhString.format(" and i > %d", h.getBigInteger("i")));
     return ret;
   }

   if( h.has("h") ) // start handle partitioned by id
   { ret.add(DhString.format(" and id >= '%s'",h.get("h").toString()));
     return ret;
   }

   for (String s : sortby)
   { String hkey = "h" + hdix++;
     Object v = h.get(hkey);
     boolean bNull = PropertyDoesNotExistIndikator.equals(v.toString());
     JSONObject jo = new JSONObject();
     jo.put(hkey, v);


     descendingLast = isDescending(s);

     if(s.startsWith("id"))
     { sortbyIdUseCase = true; break; }
     else if( !bNull )
      sqlWhereContinuation += DhString.format(" and %s = ('%s'::jsonb)->'%s'", jpathFromSortProperty(s), jo.toString() ,hkey );
     else
      sqlWhereContinuation += DhString.format(" and %s is null", jpathFromSortProperty(s) );
    }

   sqlWhereContinuation += DhString.format(" and id %s '%s'", ( descendingLast ? "<" : ">" ) ,h.get("h0").toString());

   ret.add( sqlWhereContinuation );

   if( sortbyIdUseCase ) return ret;

   for(; !sortby.isEmpty(); sortby.remove(sortby.size()-1) )
   {
     sqlWhereContinuation = "";
     hdix = 1;
     boolean bNullLastEnd = false;

     for (String s : sortby)
     { String op   = (( hdix < sortby.size() ) ? "=" : (isDescending(s) ? "<" : ">" ) ),
              hkey = "h" + hdix++;

       Object v = h.get(hkey);
       boolean bNull = PropertyDoesNotExistIndikator.equals(v.toString());

       JSONObject jo = new JSONObject();
       jo.put(hkey, v);

       descendingLast = isDescending(s);

       if(!bNull)
        switch( op )
        { case "=" :
          case "<" : sqlWhereContinuation += DhString.format(" and %s %s ('%s'::jsonb)->'%s'", jpathFromSortProperty(s), op ,jo.toString() ,hkey ); break;
          case ">" : ret.add( sqlWhereContinuation + DhString.format(" and ( %1$s > ('%2$s'::jsonb)->'%3$s' )", jpathFromSortProperty(s), jo.toString() ,hkey ) );
                     sqlWhereContinuation += DhString.format(" and ( %1$s is null )", jpathFromSortProperty(s) ); break;
        }
       else
        switch( op )
        { case "=" : sqlWhereContinuation += DhString.format(" and %s is null", jpathFromSortProperty(s) ); break;
          case ">" : bNullLastEnd = true; break; // nothing greater than null. this is due to default "NULLS LAST" on ascending dbindex
          case "<" : sqlWhereContinuation += DhString.format(" and %s is not null", jpathFromSortProperty(s) ); break;
        }
     }

     if(!bNullLastEnd)
      ret.add( sqlWhereContinuation );
   }

   return ret;

  }

  protected static boolean isDescending(String sortproperty) {
    return sortproperty.toLowerCase().endsWith(":desc");
  }

  private static String jpathFromSortProperty(String sortproperty)
  { String jformat = "(%s->'%s')", jpth = "jsondata";
    sortproperty = sortproperty.replaceAll(":(?i)(asc|desc)$", "");

    for (String p : sortproperty.split("\\."))
      jpth = DhString.format(jformat, jpth, p);

    return jpth;
  }

  private static List<String> convHandle2sortbyList( String handle )
  { JSONObject jo = new JSONObject(handle);
    JSONArray jarr = jo.getJSONArray("s");
    List<String> sortby = new ArrayList<String>();
    for(int i = 0; i < jarr.length(); i++)
     sortby.add(jarr.getString(i));

    return(sortby);
  }

  private boolean canSortBy(String tableName, List<String> sort) {
    if (sort == null || sort.isEmpty() ) return true;

    try {
     String normalizedSortProp = "o:" + IdxMaintenance.normalizedSortProperties(sort);

     switch( normalizedSortProp.toLowerCase() ) { case "o:f.id" : case "o:f.createdat" : case "o:f.updatedat" : return true; }

     List<String> indices = ((GetIndexList) new GetIndexList(tableName).withUseReadReplica(isUseReadReplica()))
         .run(getDataSourceProvider());

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

  private String buildOrderByClause(IterateFeaturesEvent event) {
    List<String> sortby = event.getHandle() != null ? convHandle2sortbyList(event.getHandle()) : event.getSort();

    if (sortby == null || sortby.size() == 0)
     return isPartOverI(event) ? "ORDER BY i" : "ORDER BY id"; // in case no sort is specified

    if (sortby.size() == 1 && sortby.get(0).toLowerCase().startsWith("id")) // usecase order by id
      return "ORDER BY id" + (sortby.get(0).equalsIgnoreCase("id:desc") ? " DESC" : "");

    String orderByClause = "", direction = "";

    for (String s : sortby)
    {
     direction = (isDescending(s) ? "DESC" : "");

     orderByClause += DhString.format("%s %s %s", (orderByClause.length() == 0 ? "" : ","), jpathFromSortProperty(s), direction);
    }

    return DhString.format("ORDER BY %s, id %s", orderByClause, direction); // id is always last sort crit with sort direction as last (most inner) index
  }

  private class GetIterationHandles extends IterateFeatures {
    //TODO: Remove after refactoring
    private IterateFeaturesEvent tmpEvent;

    public GetIterationHandles(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
      super(event);
      setUseReadReplica(true);
      this.tmpEvent = event;
    }

    @Override
    protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
      //TODO: Do not manipulate the incoming event
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
      //TODO: Do not use FeatureCollection as response vehicle
      FeatureCollection cl = super.handle(rs);
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

  private static String eventValuesToHandle(IterateFeaturesEvent event, String dbHandle)  throws JsonProcessingException {
    ObjectMapper om = new ObjectMapper();
    String pQry = DhString.format( ",\"p\":%s", event.getPropertiesQuery() != null ? om.writeValueAsString(event.getPropertiesQuery()) : "[]" ),
        tQry = DhString.format( ",\"t\":%s", event.getTags() != null ? om.writeValueAsString(event.getTags()) : "[]" ),
        mQry = DhString.format( ",\"m\":%s", event.getPart() != null ? om.writeValueAsString(event.getPart()) : "[]" ),
        hndl = DhString.format("%s%s%s%s}", dbHandle.substring(0, dbHandle.lastIndexOf("}")), pQry, tQry, mQry );
    return hndl;
  }

  protected static String createHandle(IterateFeaturesEvent event, String jsonData) throws Exception {
    return SORTED_HANDLE_PREFIX + encryptHandle(eventValuesToHandle(event, jsonData));
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
