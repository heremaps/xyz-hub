package com.here.xyz.psql.query;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.helpers.versioning.GetNextVersion;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;

public class EraseSpace extends XyzQueryRunner<ModifyFeaturesEvent, FeatureCollection> {

    ModifyFeaturesEvent mfe;

    public EraseSpace(ModifyFeaturesEvent event) throws SQLException, ErrorResponseException {
        super(event);
    }

    @Override
    protected SQLQuery buildQuery(ModifyFeaturesEvent input) throws SQLException, ErrorResponseException {
     return buildTruncateSpaceQuery(input, getSchema(), getDefaultTable(input));
    }

  @Override
  protected FeatureCollection run(DataSourceProvider dataSourceProvider) throws ErrorResponseException {
    try {

      FeatureCollection fc = super.run(dataSourceProvider);

      if( fc != null ) // s. handle(...)  -- with current used sql statements, fc will be always null 
       return fc; // just in case

      return new FeatureCollection();

    }
    catch (SQLException e) {
     throw new ErrorResponseException(EXCEPTION, e.getMessage(), e);
    }
  }


    @Override
    public FeatureCollection handle(ResultSet rs) throws SQLException {

      //handle function is not called when used with belows sql statements (non select) 
      //TODO: throw new ErrorResponseException(NOT_IMPLEMENTED, "Unexpected call of method 'handle'");
      
      throw new UnsupportedOperationException("Unexpected call of method 'handle'");
    }

    private static SQLQuery buildTruncateSpaceQueryKeepVersionSeq(ModifyFeaturesEvent event, String schema, String table) {
        String dropOtherPartitions = // build a list of partitions to be dropped
         String.format(
                "DO $b2$\n" +
                "DECLARE\n" +
                "    partition_list text;\n" +
                "BEGIN\n" +
                "    with\n" +
                "    indata as  ( select '%1$s' as in_schema, '%2$s' as in_table ),\n" +
                "    iindata as ( select i.in_schema, relname::text, replace( relname::text, i.in_table || '_p', '' )::integer as pid\n" +
                "                 from pg_class c, indata i  \n" +
                "                 where 1 = 1\n" +
                "                   and relname like i.in_table || '_p%%' \n" +
                "                   and relkind = 'r'\n" +
                "                   and relnamespace = ( select n.oid from pg_namespace n where n.nspname = i.in_schema )),\n" +
                "    iiindata as ( select ii.*, ( select max(pid) from iindata ) as max_pid from iindata ii )\n" +
                "    select string_agg( format('%%I.%%I', iii.in_schema, iii.relname ),',' ) into partition_list from iiindata iii\n" +
                "    where 1 = 1\n" +
                "      and pid != max_pid;\n" +
                "\n" +
                "    if partition_list is not null then\n" +
                "     execute format('DROP TABLE IF EXISTS %%s', partition_list);\n" +
                "    end if; \n" +
                "END $b2$\n", schema, table ); //TODO: use withNamedParameter when replacement issue is fixed.

//MMSUP-1092  tmp workaroung reduce timeouts on db9 - skip reading pg_class, drop old partitions on db9
//TODO: reinclude dropOtherPartitions, when timeouts on pg_class (db9) is fixed

         dropOtherPartitions = "";

//MMSUP-1092


        SQLQuery q = new SQLQuery("${{truncateTable}}; ${{dropOtherPartitions}}; ${{analyseTruncatedTable}}")
            .withQueryFragment("truncateTable", "TRUNCATE TABLE ${schema}.${table}")
            .withQueryFragment("dropOtherPartitions", dropOtherPartitions )
            .withQueryFragment("analyseTruncatedTable", "ANALYSE ${schema}.${table}" );

        return q
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table);
    }

    private static SQLQuery buildTruncateSpaceQueryResetVersionSeq(ModifyFeaturesEvent event, String schema, String table) {
        String dropOtherPartitions = // build a list of partitions to be dropped
         String.format(
                "DO $b2$\n" +
                "DECLARE\n" +
                "    partition_list text;\n" +
                "BEGIN\n" +
                "    with indata as ( select '%1$s' as in_schema, '%2$s' as in_table )\n" +
                "    select string_agg( format('%%I.%%I',i.in_schema,relname::text ),',' ) into partition_list from pg_class c, indata i  \n" +
                "    where 1 = 1\n" +
                "    and relname like i.in_table || '_p%%' \n" +
                "        and relname != i.in_table || '_p0' \n" +
                "        and relkind = 'r'\n" +
                "        and relnamespace = ( select n.oid from pg_namespace n where n.nspname = i.in_schema );\n" +
                "\n" +
                "    if partition_list is not null then\n" +
                "     execute format('DROP TABLE IF EXISTS %%s', partition_list);\n" +
                "    end if; \n" +
                "END $b2$\n", schema, table ); //TODO: use withNamedParameter when replacement issue is fixed.
        
        SQLQuery q = new SQLQuery("${{truncateTable}}; ${{resetVersionSequence}}; DO $b1$ BEGIN ${{recreateTableP0}}; END $b1$; ${{dropOtherPartitions}}; ${{analyseTruncatedTable}}")
            .withQueryFragment("truncateTable", "TRUNCATE TABLE ${schema}.${table} RESTART IDENTITY")
            .withQueryFragment("resetVersionSequence", "ALTER SEQUENCE ${schema}.${versionSequence} RESTART WITH 1")
            .withQueryFragment("recreateTableP0", XyzSpaceTableHelper.buildCreateHistoryPartitionQuery(schema, table, 0L,false))
            .withQueryFragment("dropOtherPartitions", dropOtherPartitions )
            .withQueryFragment("analyseTruncatedTable", "ANALYSE ${schema}.${table}" );

        return q
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table)
            .withVariable("versionSequence", table + GetNextVersion.VERSION_SEQUENCE_SUFFIX);
    }

    private static SQLQuery buildTruncateSpaceQuery(ModifyFeaturesEvent event, String schema, String table) {

      //return new SQLQuery("select count(1) from tmp.atest;");

      boolean resetVersionSequence = false;  // always keep current version, but might be subject of change.

      return resetVersionSequence ? buildTruncateSpaceQueryResetVersionSeq( event, schema, table ) 
                                  : buildTruncateSpaceQueryKeepVersionSeq( event, schema, table );

    }



}
