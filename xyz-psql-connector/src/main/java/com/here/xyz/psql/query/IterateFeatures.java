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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.tools.DhString;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

public class IterateFeatures extends SearchForFeatures<IterateFeaturesEvent, FeatureCollection> {

  public static final String HPREFIX = "h07~";
  protected static final String HANDLE_ENCRYPTION_PHRASE = "findFeaturesSort";
  private static String pg_hint_plan = "/*+ Set(seq_page_cost 100.0) IndexOnlyScan( ht1 ) */";
  private static String PropertyDoesNotExistIndikator = "#zJfCzPCz#";
  private long limit;
  private long start;

  private boolean isOrderByEvent;

  protected IterateFeaturesEvent tmpEvent; //TODO: Remove after refactoring

  public IterateFeatures(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    limit = event.getLimit();
    isOrderByEvent = PSQLXyzConnector.isOrderByEvent(event);
    tmpEvent = event;
  }

  @Override
  protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    if (isExtendedSpace(event) && event.getContext() == SpaceContext.DEFAULT) {

      SQLQuery extensionQuery = super.buildQuery(event);
      extensionQuery.setQueryFragment("filterWhereClause", "TRUE"); //TODO: Do not support search on iterate for now
      extensionQuery.setQueryFragment("iColumn", ", i, dataset");

      if (is2LevelExtendedSpace(event)) {
        int ds = 3;

        ds--;
        extensionQuery.setQueryFragment("iColumnBase", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetBase", buildIOffsetFragment(event, ds));

        ds--;
        extensionQuery.setQueryFragment("iColumnIntermediate", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetIntermediate", buildIOffsetFragment(event, ds));

        ds--;
        extensionQuery.setQueryFragment("iColumnExtension", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetExtension", buildIOffsetFragment(event, ds));
      }
      else {
        int ds = 2;

        ds--;
        extensionQuery.setQueryFragment("iColumnBase", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetBase", buildIOffsetFragment(event, ds));

        ds--;
        extensionQuery.setQueryFragment("iColumnExtension", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetExtension", buildIOffsetFragment(event, ds));
      }
      extensionQuery.setNamedParameter("currentDataset", getDatasetFromHandle(event));

      SQLQuery query = new SQLQuery(
          "SELECT * FROM (${{extensionQuery}}) orderQuery ORDER BY dataset, i ${{limit}}");
      query.setQueryFragment("extensionQuery", extensionQuery);
      query.setQueryFragment("limit", buildLimitFragment(event.getLimit()));

      return query;

    }

    if (isOrderByEvent) //TODO: Combine execution paths for ordered / non-orderd events
      return buildQueryForOrderBy(event);

    SQLQuery query = super.buildQuery(event);
    query.setQueryFragment("iColumn", ", i");

    boolean hasHandle = event.getHandle() != null;
    start = hasHandle ? Long.parseLong(event.getHandle()) : 0L;

    if (hasSearch) {
      if (hasHandle)
        query.setQueryFragment("offset", "OFFSET #{startOffset}");
    }
    else {
      if (hasHandle)
        query.setQueryFragment("filterWhereClause", "i > #{startOffset}");

      query.setQueryFragment("orderBy", "ORDER BY i");
    }

    if (hasHandle)
      query.setNamedParameter("startOffset", start);

    return query;
  }

  private static String buildIColumnFragment(int dataset) {
    return ", i, " + dataset + " as dataset";
  }

  private SQLQuery buildIOffsetFragment(IterateFeaturesEvent event, int dataset) {
    return new SQLQuery("AND " + dataset + " >= #{currentDataset} "
        + "AND (" + dataset + " > #{currentDataset} OR i > #{startOffset}) ORDER BY i")
        .withNamedParameter("startOffset", getIOffsetFromHandle(event));
  }

  private int getDatasetFromHandle(IterateFeaturesEvent event) {
    if (event.getHandle() == null)
      return -1;
    return Integer.parseInt(event.getHandle().split("_")[0]);
  }

  private int getIOffsetFromHandle(IterateFeaturesEvent event) {
    if (event.getHandle() == null)
      return 0;
    return Integer.parseInt(event.getHandle().split("_")[1]);
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

  private SQLQuery buildQueryForOrderBy(IterateFeaturesEvent event) {
    SQLQuery partialQuery = buildPartialSortIterateQuery(event);

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

  private static SQLQuery buildGetIterateHandlesQuery(int nrHandles) {
    return new SQLQuery(DhString.format(bucketOfIdsSql, nrHandles));
  }

  protected static boolean isDescending(String sortproperty) { return sortproperty.toLowerCase().endsWith(":desc"); }

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

  private static String bucketOfIdsSql =
      "with  "
    + "idata as "
    + "(  select min(id) as id from ${schema}.${table} "
    + "  union  "
    + "   select id from ${schema}.${table} "
    + "    tablesample system( (select least((100000/greatest(reltuples,1)),100) from pg_catalog.pg_class where oid = format('%%s.%%s','${schema}', '${table}' )::regclass) ) " //-- repeatable ( 0.7 )
    + "), "
    + "iidata   as ( select id, ntile( %1$d ) over ( order by id ) as bucket from idata ), "
    + "iiidata  as ( select min(id) as id, bucket from iidata group by bucket ), "
    + "iiiidata as ( select bucket, id as i_from, lead( id, 1) over ( order by id ) as i_to from iiidata ) "
    + "select  jsonb_set('{\"type\":\"Feature\",\"properties\":{}}','{properties,handles}', jsonb_agg(jsonb_build_array(bucket, i_from, i_to ))),'{\"type\":\"Point\",\"coordinates\":[]}', null from iiiidata ";


  private static String addEventValuesToHandle(IterateFeaturesEvent event, String dbhandle)  throws JsonProcessingException
  {
   ObjectMapper om = new ObjectMapper();
   String pQry = DhString.format( ",\"p\":%s", event.getPropertiesQuery() != null ? om.writeValueAsString(event.getPropertiesQuery()) : "[]" ),
          tQry = DhString.format( ",\"t\":%s", event.getTags() != null ? om.writeValueAsString(event.getTags()) : "[]" ),
          mQry = DhString.format( ",\"m\":%s", event.getPart() != null ? om.writeValueAsString(event.getPart()) : "[]" ),
          hndl = DhString.format("%s%s%s%s}", dbhandle.substring(0, dbhandle.lastIndexOf("}")), pQry, tQry, mQry );
   return hndl;
  }

  private static String chrE( String s ) { return s.replace('+','-').replace('/','_').replace('=','.'); }

  protected static String createHandle(IterateFeaturesEvent event, String jsonData ) throws Exception {
    return HPREFIX + chrE(encrypt(addEventValuesToHandle(event, jsonData), HANDLE_ENCRYPTION_PHRASE));
  }

  @SuppressWarnings("unused")
  private static String encrypt(String plaintext, String phrase) throws Exception {
    return PSQLConfig.encryptECPS( plaintext, phrase );
  }

}
