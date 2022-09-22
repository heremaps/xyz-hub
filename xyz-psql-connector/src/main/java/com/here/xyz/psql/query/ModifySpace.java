package com.here.xyz.psql.query;

import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.psql.*;
import com.here.xyz.psql.config.PSQLConfig;

import java.sql.SQLException;
import java.util.Map;

public class ModifySpace extends ExtendedSpace<ModifySpaceEvent> {
    private String schema;
    private String table;
    private String spaceId;

    public ModifySpace(ModifySpaceEvent event, DatabaseHandler dbHandler) throws SQLException {
        super(event, dbHandler, true);
    }

    @Override
    protected SQLQuery buildQuery(ModifySpaceEvent event) throws SQLException {
        PSQLConfig config = dbHandler.getConfig();
        this.schema = config.getDatabaseSettings().getSchema();
        this.table = config.readTableFromEvent(event);
        this.spaceId = event.getSpaceDefinition() == null ? "" : event.getSpaceDefinition().getId();

        SQLQuery q = new SQLQuery();

        if ((ModifySpaceEvent.Operation.CREATE == event.getOperation()
                || ModifySpaceEvent.Operation.UPDATE == event.getOperation())) {

            Map<String, String> extendedTableNames = getExtendedTableNames(config);
            // Write idx related data
            q.append(SQLQueryBuilder.buildSearchablePropertiesUpsertQuery(
                    event.getSpaceDefinition(),
                    schema,
                    table,
                    extendedTableNames));

            // Write metadata
            q.append(SQLQueryBuilder.buildSpaceMetaUpsertQuery(
                    spaceId,
                    schema,
                    table,
                    extendedTableNames));

        }else if (ModifySpaceEvent.Operation.DELETE == event.getOperation()) {
            q.append(SQLQueryBuilder.buildCleanUpQuery(schema,table));
        }
        return q;
    }

    public void maintainSpace(){
        if(((ModifySpaceEvent)event).getOperation() != ModifySpaceEvent.Operation.DELETE)
            this.dbHandler.dbMaintainer.maintainSpace(this.dbHandler.traceItem, schema, table);
    }
}
