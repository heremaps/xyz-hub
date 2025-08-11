package com.here.xyz.psql.query;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.xbill.DNS.dnssec.R;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.helpers.versioning.GetNextVersion;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.SQLError;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.responses.XyzError.EXCEPTION;

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

      if( fc != null ) // s. handle(...)  -- current fc will be null 
       return fc;

      return new FeatureCollection();

    }
    catch (SQLException e) {
     throw new ErrorResponseException(EXCEPTION, e.getMessage(), e);
    }
  }


    @Override
    public FeatureCollection handle(ResultSet rs) throws SQLException {

      //TODO: handle function is not called when sql does not return results
      // return new FeatureCollection();
      throw new UnsupportedOperationException("Unimplemented method 'handle'");
    }

    private static SQLQuery buildTruncateSpaceQueryKeepVersionSeq(ModifyFeaturesEvent event, String schema, String table) {
        String dropOtherPartitions = // build a list of partitions to be dropped
         String.format(
                """
                    DO $b2$
                    DECLARE
                        partition_list text;
                    BEGIN
                        with
                        indata as  ( select '%1$s' as in_schema, '%2$s' as in_table ),
                        iindata as ( select i.in_schema, relname::text, replace( relname::text, i.in_table || '_p', '' )::integer as pid
                                     from pg_class c, indata i  
                                     where 1 = 1
                                       and relname like i.in_table || '_p%%' 
                                       and relkind = 'r'
                                       and relnamespace = ( select n.oid from pg_namespace n where n.nspname = i.in_schema )),
                        iiindata as ( select ii.*, ( select max(pid) from iindata ) as max_pid from iindata ii )
                        select string_agg( format('%%I.%%I', iii.in_schema, iii.relname ),',' ) into partition_list from iiindata iii
                        where 1 = 1
                          and pid != max_pid;

                        if partition_list is not null then
                         execute format('DROP TABLE IF EXISTS %%s', partition_list);
                        end if; 
                    END $b2$
                """, schema, table ); //TODO: use withNamedParameter when replacement issue is fixed.

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
                """
                    DO $b2$
                    DECLARE
                        partition_list text;
                    BEGIN
                        with indata as ( select '%1$s' as in_schema, '%2$s' as in_table )
                        select string_agg( format('%%I.%%I',i.in_schema,relname::text ),',' ) into partition_list from pg_class c, indata i  
                        where 1 = 1
                        and relname like i.in_table || '_p%%' 
                            and relname != i.in_table || '_p0' 
                            and relkind = 'r'
                            and relnamespace = ( select n.oid from pg_namespace n where n.nspname = i.in_schema );

                        if partition_list is not null then
                         execute format('DROP TABLE IF EXISTS %%s', partition_list);
                        end if; 
                    END $b2$
                """, schema, table ); //TODO: use withNamedParameter when replacement issue is fixed.
        

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
