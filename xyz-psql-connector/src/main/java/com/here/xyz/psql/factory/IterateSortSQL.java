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

package com.here.xyz.psql.factory;

import java.util.ArrayList;
import java.util.List;

import com.here.xyz.psql.SQLQuery;

import org.json.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;

public class IterateSortSQL {

  private static boolean isDescending(String sortproperty) { return sortproperty.toLowerCase().endsWith(":desc"); }

  private static String jpathFromSortProperty(String sortproperty)
  { String jformat = "(%s->'%s')", jpth = "jsondata";
    sortproperty = sortproperty.replaceAll(":(?i)(asc|desc)$", "");

    for (String p : sortproperty.split("\\."))
      jpth = String.format(jformat, jpth, p);

    return jpth;  
  }

  private static String buildNextHandleAttribute(List<String> sortby) 
  { String nhandle = "jsonb_set('{}','{h0}', jsondata->'id')", svalue = "";
    int hDepth = 1;

    if (sortby != null && sortby.size() > 0)
      for (String s : sortby) 
      {
        svalue += String.format("%s\"%s\"", (svalue.length() == 0 ? "" : ","), s);

        nhandle = String.format("jsonb_set(%s,'{h%d}',%s)", nhandle, hDepth++, jpathFromSortProperty(s) );
      }

    return String.format("jsonb_set(%s,'{s}','[%s]')", nhandle, svalue);
  }

  private static String buildOrderByClause(List<String> sortby) 
  { if (sortby == null || sortby.size() == 0) return "";

    String orderByClause = "", direction = "";

    for (String s : sortby) 
    {
     direction = (isDescending(s) ? "desc" : "");

     orderByClause += String.format("%s %s %s", (orderByClause.length() == 0 ? "" : ","), jpathFromSortProperty(s), direction);
    }

    return String.format("order by %s, (jsondata->>'id') %s", orderByClause, direction); // id is always last sort crit with sort direction as last (most inner) index
  }

  private static List<String> convHandle2sortbyList( String handle )
  { JSONObject jo = new JSONObject(handle);
    JSONArray jarr = jo.getJSONArray("s");
    List<String> sortby = new ArrayList<String>();
    for(int i = 0; i < jarr.length(); i++) 
     sortby.add(jarr.getString(i));
     
    return(sortby);
  }

  private static List<String> buildContinuationConditions( String handle )
  {
   List<String> ret = new ArrayList<String>(),
                sortby = convHandle2sortbyList( handle );
   JSONObject h = new JSONObject(handle);
   boolean descendingLast = false;
   String sqlWhereContinuation = "";
   int hdix = 1;   

   for (String s : sortby) 
   { String hkey = "h" + hdix++;
     JSONObject jo = new JSONObject();
     jo.put(hkey, h.get(hkey));

     descendingLast = isDescending(s);
     
     sqlWhereContinuation += String.format(" and %s = ('%s'::jsonb)->'%s'", jpathFromSortProperty(s), jo.toString() ,hkey );
   }

   sqlWhereContinuation += String.format(" and (jsondata->>'id') %s '%s'", ( descendingLast ? "<" : ">" ) ,h.getString("h0"));

   ret.add( sqlWhereContinuation );
   
   for(; !sortby.isEmpty(); sortby.remove(sortby.size()-1) )
   { 
     sqlWhereContinuation = "";
     hdix = 1;

     for (String s : sortby) 
     { String op   = (( hdix < sortby.size() ) ? "=" : (isDescending(s) ? "<" : ">" ) ),
              hkey = "h" + hdix++;
              
       JSONObject jo = new JSONObject();
       jo.put(hkey, h.get(hkey));
  
       descendingLast = isDescending(s);
       
       sqlWhereContinuation += String.format(" and %s %s ('%s'::jsonb)->'%s'", jpathFromSortProperty(s), op ,jo.toString() ,hkey );
     }
       
     ret.add( sqlWhereContinuation );
   }
   
   return ret;

  }

  private static String sortedIterate = 
        "with dt as " 
      + "( "
      + " ##_INNER_SEARCH_QRY_## "
      + " %1$s " 
      + ") "
      + "select jsondata, geo, %2$s as nxthandle " 
      + "from dt s join ${schema}.${table} d on ( s.i = d.i ) "
      + "order by s.ord1, s.ord2 ";

  private static String partialSortedIterate = 
        " ("
      + "  select %1$d::integer as ord1, row_number() over () ord2, i " 
      + "  from " 
      + "  ( select i from ${schema}.${table} "
      + "    where 1 = 1 "
      + "    ##_SEARCHQRY_## "
      + "    %2$s "  // continuation sql
      + "    %3$s "  // orderby criteria
      + "    limit %4$d " // limt
      + "  ) inr " 
      + " ) "; 

  public static SQLQuery innerSortedQry(SQLQuery searchQuery, List<String> sortby, String handle, long limit) {
    boolean useHandle = (handle != null) ;

    if( useHandle ) 
     sortby = convHandle2sortbyList(handle);
    
   String orderByClause = buildOrderByClause(sortby),
          partialSQL = "";

   if(! useHandle )
    partialSQL = String.format( partialSortedIterate, 0, "", orderByClause, limit );
   else
   {
    List<String> continuationWhereClause = buildContinuationConditions( handle );
    for( int i = 0; i < continuationWhereClause.size(); i++ )
     partialSQL += String.format(" %s %s", (partialSQL.length() > 0 ? " union all " : "") , String.format( partialSortedIterate, i, continuationWhereClause.get(i), orderByClause, limit ) );
   } 

   String[] parts = partialSQL.split("##_SEARCHQRY_##");

   SQLQuery partialQry = new SQLQuery();

   if( searchQuery == null )
    partialQry.append( String.join("",parts) );
   else
    for( int i = 0; i < parts.length; i++ ) 
    { partialQry.append(parts[i]);
      if( i < parts.length - 1 ) 
      { partialQry.append( " and " );
        partialQry.append( searchQuery );
      }  
    }

   String nextHandleJson = buildNextHandleAttribute(sortby),
          outerSQL = String.format( sortedIterate, (!useHandle ? "" : String.format("order by ord1, ord2 limit %1$d",limit) ) , nextHandleJson );

   String[] outs = outerSQL.split("##_INNER_SEARCH_QRY_##");

   SQLQuery innerQry = new SQLQuery( outs[0] );
   innerQry.append( partialQry );
   innerQry.append( outs[1]);
          
   return innerQry;
  }


public static class IdxMaintenance // idx Maintenance
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

     btreeClause += String.format("%s %s,", jpathFromSortProperty(s), direction);
     idxComment += String.format("%s%s%s%s", idxComment.length()> 0 ? ",":"", pname, direction.length()>0 ? ":" : "", direction);
    }

    String hash = DigestUtils.md5Hex(idxComment).substring(0, 7),
           idxName = String.format("idx_%s_%s_o%s",spaceName,hash,idxPostFix);
    return new String[] { String.format( crtSortIdxSql, idxName, btreeClause, direction ), 
                          String.format( crtSortIdxCommentSql, idxName, idxComment ) };
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
 
       normalizedSortProp += String.format("%s%s%s%s", normalizedSortProp.length()> 0 ? ",":"", pname, direction.length() > 0 ? ":" : "", direction);
     }

     return normalizedSortProp;
  }

 }
}



