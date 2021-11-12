package com.here.xyz.psql.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.here.xyz.util.DhString;

public class PartitionedSpace 
{
  public static class PartitionDef
  { 
   public int size = 0;
   public boolean isNumeric = false;
   public String ekey;
   public ArrayList<Object> listArr;
   public ArrayList<ArrayList<Object>> rangeArr;
   public ArrayList<Object> tableArr;

   private static String jpathFromProperty(String pth)
   { 
    if(pth.startsWith("p.") ) 
     pth = pth.replaceFirst("^p\\.", "properties.");
    else if ( pth.startsWith("f.") )
     pth = pth.replaceFirst("^f\\.", ""); 

    String jpth = "jsondata";
 
    String[] ptharr = pth.split("\\.");
    for( int i = 0; i < ptharr.length; i++ )
     jpth = DhString.format("(%s->'%s')", jpth, ptharr[i]);

    return jpth;  
   }
 
   public String jkey() { return jpathFromProperty( ekey ); };

   public Map<String, Object> asMap()
   {
     HashMap<String,Object> h = new HashMap<String,Object>();

     h.put("key", ekey);

     switch( Type() )
     { case "hash"  : h.put( "buckets", new Integer(size) ); break;
       case "list"  : h.put("list", listArr); break;
       case "range" : h.put("range", rangeArr); break;
       default: break;
     }
     
     return h; 
   }

   public PartitionDef( Map<String, Object> prtns )
   { ekey = prtns.getOrDefault("key", "id").toString();
     
     if( prtns.containsKey("buckets") )
     { Object o = prtns.get("buckets");
       if( o != null && o instanceof Integer ) size = (Integer) o;
     } 
     else if ( prtns.containsKey("list") )
     { Object o = prtns.get("list");
       if( o != null && o instanceof ArrayList<?> )
       { listArr = (ArrayList<Object>) o;
         size = listArr.size();
         isNumeric = !( listArr.get(0) instanceof String );
       }
     }
     else if ( prtns.containsKey("range") )
     { Object o = prtns.get("range");
       if( o != null && o instanceof ArrayList<?> )
       { rangeArr = (ArrayList<ArrayList<Object>>) o;
         size = rangeArr.size();
         isNumeric = !( rangeArr.get(0).get(0) instanceof String );
       }
     }

     if( prtns.containsKey("tables") ) // additional db info returned by PartitionedSpaceSQL.getCurrentStateSql
     { Object o = prtns.get("tables");
       if( o != null && o instanceof ArrayList<?> )
        tableArr = (ArrayList<Object>) o;
     }
   }

   public boolean byHash()  { return ( listArr == null && rangeArr == null); }
   public boolean byList()  { return ( listArr != null ); }
   public boolean byRange() { return ( rangeArr != null ); }

   public String Type() { return byHash() ? "hash" : byList() ? "list" : byRange() ? "range" : "499"; }
   public boolean isCompatible( PartitionDef pd) { return ekey.equals(pd.ekey) && Type() == pd.Type(); }
   public boolean equals( PartitionDef pd )
   {
    return    size == pd.size 
           && isCompatible(pd) 
           && (byHash() || (byList() && listArr.containsAll( pd.listArr)) || (byRange() && rangeArr.containsAll( pd.rangeArr))) ;
   }

   public int lastUsedTableNr()
   { int rVal = -1;
     
     if( tableArr != null )
      for (Object o : tableArr ) 
      { String tableName = o.toString(),
               strNr = tableName.replaceFirst(".*_([\\d]+)$", "$1");
        int iNr = Integer.parseInt(strNr);
        if( iNr > rVal ) rVal = iNr;
      }

     return rVal;
   }

   
  }


 public static String getCurrentStateSql =
    "with "
    +"indata as ( select partrelid, partstrat, (partdefid != 0) as hasdefault from pg_partitioned_table where partrelid = format('%%I.%%I','%1$s', '%2$s' )::regclass ),"
    +"partdata as "
    +"( select rtable,ptable,strat,hasdefault, count(1) over () as nrpartitons,"
    +"    regexp_replace(regexp_replace(regexp_replace(pkey,'(LIST|RANGE|HASH)\\s+|\\)|\\(|''|::text|(\\s+->>\\s+0)|(jsondata\\s+->\\s)','','g'),'^properties\\s*->\\s*','p.'),'\\s*->\\s*','.','g') as pkey,"
    +"    case strat"
    +"     when 'r' then (regexp_replace(replace( pexpr ,'''','\"'),'.*\\(([^)]+)\\)\\s+TO\\s+\\(([^)]+)\\)','[\\1,\\2]'))::jsonb"
    +"     when 'l' then (regexp_replace(replace( pexpr ,'''','\"'),'.*\\(([^)]+)\\)','\\1'))::jsonb"
    +"     when 'h' then to_jsonb( regexp_replace( pexpr ,'.*remainder\\s+(\\d+)\\s*\\).*','\\1' )::numeric )"
    +"     else null::jsonb"
    +"    end as pval"
    +"  from"
    +"  ( select pc.relname as rtable,"
    +"           pt.relname as ptable,"
    +"           i1.partstrat as strat,"
    +"           i1.hasdefault,"
    +"           pg_get_partkeydef(pc.oid) as pkey,"
    +"           pg_get_expr(pt.relpartbound, pt.oid, true) as pexpr"
    +"    from indata i1, pg_class pc "
    +"    join pg_inherits i on i.inhparent = pc.oid "
    +"    join pg_class pt on pt.oid = i.inhrelid"
    +"    where pc.oid = i1.partrelid"
    +"  ) o"
    +"  where pexpr != 'DEFAULT'"
    +"),"
    +"partjs as "
    +"( select "
    +"   jsonb_set( "
    +"    case strat"
    +"     when 'h' then jsonb_set(jsonb_set('{}','{key}',to_jsonb( max(pkey) ) ),'{buckets}', to_jsonb(max(nrpartitons)))"
    +"     when 'r' then jsonb_set(jsonb_set('{}','{key}',to_jsonb( max(pkey) ) ),'{range}', jsonb_agg(pval) )"
    +"     when 'l' then jsonb_set(jsonb_set('{}','{key}',to_jsonb( max(pkey) ) ),'{list}', jsonb_agg(pval) )"
    +"    end, '{tables}',jsonb_agg(to_jsonb(ptable))"
    +"   ) as partitions_def"
    +"  from partdata group by strat"
    +") "
    +"select partitions_def from partjs";

}