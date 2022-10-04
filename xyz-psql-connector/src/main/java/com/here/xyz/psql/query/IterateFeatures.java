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

package com.here.xyz.psql.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.Capabilities;
import com.here.xyz.psql.Capabilities.IndexList;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.config.PSQLConfig.AESGCMHelper;
import com.here.xyz.responses.XyzError;
import com.here.xyz.psql.tools.DhString;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;

public class IterateFeatures extends SearchForFeatures<IterateFeaturesEvent> {

  public static final String HPREFIX = "h07~";
  private static final String HANDLE_ENCRYPTION_PHRASE = "findFeaturesSort";
  private static String pg_hint_plan = "/*+ Set(seq_page_cost 100.0) IndexOnlyScan( ht1 ) */";
  private static String PropertyDoesNotExistIndikator = "#zJfCzPCz#";
  private long limit;
  private long start;

  private boolean isOrderByEvent;

  private IterateFeaturesEvent tmpEvent; //TODO: Remove after refactoring

  public IterateFeatures(IterateFeaturesEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
    limit = event.getLimit();
    isOrderByEvent = PSQLXyzConnector.isOrderByEvent(event);
    tmpEvent = event;
  }

  @Override
  protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException {
    if (isExtendedSpace(event) && event.getContext() == SpaceContext.DEFAULT) {

      SQLQuery extensionQuery = buildQuery(event, "TRUE"); //TODO: Do not support search on iterate for now
      extensionQuery.setQueryFragment("iColumn", ", CONCAT('', i) AS i");

      if (is2LevelExtendedSpace(event)) {
        extensionQuery.setQueryFragment("iColumnIntermediate", ", CONCAT('e1_', i) AS i");
        extensionQuery.setQueryFragment("iColumnExtension", ", CONCAT('e2_', i) AS i");
      }
      else
        extensionQuery.setQueryFragment("iColumnExtension", ", CONCAT('e', i) AS i");

      SQLQuery offsetQuery = new SQLQuery(event.getHandle() == null? "TRUE" : "i::text > #{startOffset}", Collections.singletonMap("startOffset", event.getHandle()));

      SQLQuery query = new SQLQuery(
          "SELECT * FROM (${{extensionQuery}}) orderQuery WHERE ${{offsetQuery}} ORDER BY i ${{limit}}");
      query.setQueryFragment("extensionQuery", extensionQuery);
      query.setQueryFragment("offsetQuery", offsetQuery);
      query.setQueryFragment("limit", buildLimitFragment(event.getLimit()));

      return query;

    }

    if (isOrderByEvent) //TODO: Combine execution paths for ordered / non-orderd events
      return buildQueryForOrderBy(event);

    SQLQuery query = super.buildQuery(event);

    boolean hasHandle = event.getHandle() != null;
    start = hasHandle ? Long.parseLong(event.getHandle()) : 0L;

    if (hasSearch) {
      if (hasHandle)
        query.setQueryFragment("offset", "OFFSET #{startOffset}");
    }
    else {
      if (hasHandle)
        query.setQueryFragment("filterWhereClause", query.getQueryFragment("filterWhereClause") + " AND i > #{startOffset}");

      query.setQueryFragment("orderBy", "ORDER BY i");
    }

    if (hasHandle)
      query.setNamedParameter("startOffset", start);

    return query;
  }

  private SQLQuery buildQueryForOrderBy(IterateFeaturesEvent event) throws SQLException {
    SQLQuery filterWhereClause = generateSearchQuery(event);
    if (filterWhereClause == null)
      filterWhereClause = new SQLQuery("TRUE");

    //---------------------------------------------------------------------------------------------

    SQLQuery partialQuery = buildPartialSortIterateQuery(event, filterWhereClause);

    String nextHandleJson = buildNextHandleAttribute(event.getSort(), isPartOverI(event));
    SQLQuery innerQry = new SQLQuery("WITH dt AS ("
        + " ${{partialQuery}} "
        + " ${{orderings}} "
        + ") "
        + "SELECT jsondata, geo, ${{nextHandleJson}} AS nxthandle "
        + "    FROM dt s JOIN ${schema}.${table} d ON ( s.i = d.i ) "
        + "    ORDER BY s.ord1, s.ord2");

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
    query.setQueryFragment("selection", SQLQuery.selectJson(event));
    query.setQueryFragment("geo", buildGeoFragment(event));
    query.setQueryFragment("innerQuery", innerQry);

    return query;
  }

  private SQLQuery buildPartialSortIterateQuery(IterateFeaturesEvent event, SQLQuery filterWhereClause) {
    //Extend the filterWhereClause by the necessary parts for partial iteration using modules
    Integer[] part = event.getPart();
    if (part != null && part.length == 2 && part[1] > 1) {
      filterWhereClause.append(" AND (( i %% #{total} ) = #{partition})");
      filterWhereClause.setNamedParameter("total", part[1]);
      filterWhereClause.setNamedParameter("partition", part[0] - 1);
    }

    String orderByClause = buildOrderByClause(event);
    List<String> continuationWhereClauses = event.getHandle() == null ? Collections.singletonList("")
        : buildContinuationConditions(event.getHandle());

    SQLQuery partialQuery = new SQLQuery("");

    for (int i = 0; i < continuationWhereClauses.size(); i++) {
      partialQuery.append((i > 0 ? " UNION ALL " : "") + buildPartialSortIterateSQL(i));
      partialQuery.setQueryFragment("continuation", continuationWhereClauses.get(i));
    }
    partialQuery.setQueryFragment("orderBy", orderByClause);
    partialQuery.setNamedParameter("limit", limit);

    partialQuery.setQueryFragment("filterWhereClause", filterWhereClause);

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

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    FeatureCollection fc = super.handle(rs);
    if (isOrderByEvent) {
      if (fc.getHandle() != null) {
        //Extend handle and encrypt
        final String handle;
        try {
          handle = createHandle(tmpEvent, fc.getHandle() );
        }
        catch (Exception e) {
          throw new RuntimeException(e); //TODO: Use ErrorResponseException here after refactoring of the base class
        }
        fc.setHandle(handle);
        fc.setNextPageToken(handle);
      }
    }
    else {
      if (hasSearch && fc.getHandle() != null) {
        fc.setHandle("" + (start + limit)); //Kept for backwards compatibility for now
        fc.setNextPageToken("" + (start + limit));
      }
    }
    return fc;
  }

  private static SQLQuery buildGetIterateHandlesQuery( int nrHandles )
    { return getIterateHandles(nrHandles);  }

  private static boolean canSortBy(String space, List<String> sort, DatabaseHandler dbHandler)
  {
    if (sort == null || sort.isEmpty() ) return true;

    try
    {
     String normalizedSortProp = "o:" + IdxMaintenance.normalizedSortProperies(sort);

     switch( normalizedSortProp.toLowerCase() ) { case "o:f.id" : case "o:f.createdat" : case "o:f.updatedat" : return true; }

     List<String> indices = IndexList.getIndexList(space, dbHandler);

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

  private static boolean isDescending(String sortproperty) { return sortproperty.toLowerCase().endsWith(":desc"); }

  private static String jpathFromSortProperty(String sortproperty)
  { String jformat = "(%s->'%s')", jpth = "jsondata";
    sortproperty = sortproperty.replaceAll(":(?i)(asc|desc)$", "");

    for (String p : sortproperty.split("\\."))
      jpth = DhString.format(jformat, jpth, p);

    return jpth;
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

  private  String buildOrderByClause(IterateFeaturesEvent event) {
    List<String> sortby = event.getHandle() != null ? convHandle2sortbyList(event.getHandle()) : event.getSort();

    if (sortby == null || sortby.size() == 0)
     return isPartOverI(event) ? "ORDER BY i" : "ORDER BY (jsondata->>'id')"; // in case no sort is specified

    if( sortby.size() == 1 && sortby.get(0).toLowerCase().startsWith("id") ) // usecase order by id
     if( sortby.get(0).equalsIgnoreCase( "id:desc" ) )
      return "ORDER BY (jsondata->>'id') DESC";
     else
      return "ORDER BY (jsondata->>'id')";

    String orderByClause = "", direction = "";

    for (String s : sortby)
    {
     direction = (isDescending(s) ? "DESC" : "");

     orderByClause += DhString.format("%s %s %s", (orderByClause.length() == 0 ? "" : ","), jpathFromSortProperty(s), direction);
    }

    return DhString.format("ORDER BY %s, (jsondata->>'id') %s", orderByClause, direction); // id is always last sort crit with sort direction as last (most inner) index
  }

  private static List<String> convHandle2sortbyList( String handle )
  { JSONObject jo = new JSONObject(handle);
    JSONArray jarr = jo.getJSONArray("s");
    List<String> sortby = new ArrayList<String>();
    for(int i = 0; i < jarr.length(); i++)
     sortby.add(jarr.getString(i));

    return(sortby);
  }

  private boolean isPartOverI(IterateFeaturesEvent event) {
    if (event.getHandle() != null)
      return (new JSONObject(event.getHandle())).has("i");

    return event.getPart() != null && event.getSort() == null && !hasSearch;
  }

  private static List<String> buildContinuationConditions( String handle )
  {
   List<String> ret = new ArrayList<String>(),
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
   { ret.add(DhString.format(" and (jsondata->>'id') >= '%s'",h.get("h").toString()));
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

   sqlWhereContinuation += DhString.format(" and (jsondata->>'id') %s '%s'", ( descendingLast ? "<" : ">" ) ,h.get("h0").toString());

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

  private static String bucketOfIdsSql =
      "with  "
    + "idata as "
    + "(  select min( jsondata->>'id' ) as id from ${schema}.${table} "
    + "  union  "
    + "   select jsondata->>'id' as id from ${schema}.${table} "
    + "    tablesample system( (select least((100000/greatest(reltuples,1)),100) from pg_catalog.pg_class where oid = format('%%s.%%s','${schema}', '${table}' )::regclass) ) " //-- repeatable ( 0.7 )
    + "), "
    + "iidata   as ( select id, ntile( %1$d ) over ( order by id ) as bucket from idata ), "
    + "iiidata  as ( select min(id) as id, bucket from iidata group by bucket ), "
    + "iiiidata as ( select bucket, id as i_from, lead( id, 1) over ( order by id ) as i_to from iiidata ) "
    + "select  jsonb_set('{\"type\":\"Feature\",\"properties\":{}}','{properties,handles}', jsonb_agg(jsonb_build_array(bucket, i_from, i_to ))),'{\"type\":\"Point\",\"coordinates\":[]}', null from iiiidata ";


  private static SQLQuery getIterateHandles(int nrHandles)
  { return new SQLQuery(DhString.format(bucketOfIdsSql, nrHandles)); }

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

  private static String addEventValuesToHandle(IterateFeaturesEvent event, String dbhandle)  throws JsonProcessingException
  {
   ObjectMapper om = new ObjectMapper();
   String pQry = DhString.format( ",\"p\":%s", event.getPropertiesQuery() != null ? om.writeValueAsString(event.getPropertiesQuery()) : "[]" ),
          tQry = DhString.format( ",\"t\":%s", event.getTags() != null ? om.writeValueAsString(event.getTags()) : "[]" ),
          mQry = DhString.format( ",\"m\":%s", event.getPart() != null ? om.writeValueAsString(event.getPart()) : "[]" ),
          hndl = DhString.format("%s%s%s%s}", dbhandle.substring(0, dbhandle.lastIndexOf("}")), pQry, tQry, mQry );
   return hndl;
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

  private static List<String> getSearchKeys(  PropertiesQuery p )
  { return p.stream()
             .flatMap(List::stream)
             .filter(k -> k.getKey() != null && k.getKey().length() > 0)
             .map(PropertyQuery::getKey)
             .collect(Collectors.toList());
  }

  private static List<String> getSortFromSearchKeys( List<String> searchKeys, String space, DatabaseHandler dbHandler ) throws Exception
  {
   List<String> indices = Capabilities.IndexList.getIndexList(space, dbHandler);
   if( indices == null ) return null;

   indices.sort((s1, s2) -> s1.length() - s2.length());

   for(String sk : searchKeys )
    switch( sk )
    { case "id" : return null; // none is always sorted by ID;
      case "properties.@ns:com:here:xyz.createdAt" : return Arrays.asList("f.createdAt");
      case "properties.@ns:com:here:xyz.updatedAt" : return Arrays.asList("f.updatedAt");
      default:
       if( !sk.startsWith("properties.") ) sk = "o:f." + sk;
       else sk = sk.replaceFirst("^properties\\.","o:");

       for(String idx : indices)
        if( idx.startsWith(sk) )
        { List<String> r = new ArrayList<String>();
          String[] sortIdx = idx.replaceFirst("^o:","").split(",");
          for( int i = 0; i < sortIdx.length; i++)
           r.add( sortIdx[i].startsWith("f.") ? sortIdx[i] : "properties." + sortIdx[i] );
          return r;
        }
      break;
    }

   return null;
  }

  private static String chrE( String s ) { return s.replace('+','-').replace('/','_').replace('=','.'); }

  private static String chrD( String s ) { return s.replace('-','+').replace('_','/').replace('.','='); }

  private static String createHandle(IterateFeaturesEvent event, String jsonData ) throws Exception {
    return HPREFIX + chrE(encrypt(addEventValuesToHandle(event, jsonData), HANDLE_ENCRYPTION_PHRASE));
  }

  private static FeatureCollection requestIterationHandles(IterateFeaturesEvent event, int nrHandles, DatabaseHandler dbHandler) throws Exception
  {
    event.setPart(null);
    event.setTags(null);

    FeatureCollection cl = dbHandler.executeQueryWithRetry( buildGetIterateHandlesQuery(nrHandles));
    List<List<Object>> hdata = cl.getFeatures().get(0).getProperties().get("handles");
    for( List<Object> entry : hdata )
    {
      event.setPropertiesQuery(null);
      if( entry.get(2) != null )
      { PropertyQuery pqry = new PropertyQuery();
        pqry.setKey("id");
        pqry.setOperation(QueryOperation.LESS_THAN);
        pqry.setValues(Arrays.asList( entry.get(2)) );
        PropertiesQuery pqs = new PropertiesQuery();
        PropertyQueryList pql = new PropertyQueryList();
        pql.add( pqry );
        pqs.add( pql );

        event.setPropertiesQuery( pqs );
      }
      entry.set(0, createHandle(event,DhString.format("{\"h\":\"%s\",\"s\":[]}",entry.get(1).toString())));
    }
    return cl;
  }

  public static FeatureCollection findFeaturesSort(IterateFeaturesEvent event, DatabaseHandler dbHandler) throws Exception
  {
    boolean hasHandle = (event.getHandle() != null);
    String space = dbHandler.getConfig().readTableFromEvent(event);

    if( !hasHandle )  // decrypt handle and configure event
    {
      if( event.getPart() != null && event.getPart()[0] == -1 )
       return requestIterationHandles(event, event.getPart()[1], dbHandler);

      if( event.getPropertiesQuery() != null && (event.getSort() == null || event.getSort().isEmpty()) )
      {
       event.setSort( getSortFromSearchKeys( getSearchKeys( event.getPropertiesQuery() ), space, dbHandler ) );
      }
      else if (!canSortBy(space, event.getSort(), dbHandler))
      {
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
     catch (GeneralSecurityException | IllegalArgumentException e) {
       throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "Invalid request parameter. handle is corrupted");
     }

     return new IterateFeatures(event, dbHandler).run();
  }

  @SuppressWarnings("unused")
  private static String encrypt(String plaintext, String phrase) throws Exception {
    return PSQLConfig.encryptECPS( plaintext, phrase );
  }

  @SuppressWarnings("unused")
  private static String decrypt(String encryptedtext, String phrase) throws Exception { return AESGCMHelper.getInstance(phrase).decrypt(encryptedtext); }

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

     public static String normalizedSortProperies(List<String> sortby) // retuns feld1,feld2:ord2,feld3:ord3 where sortorder feld1 is always ":asc"
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
