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


import com.here.mapcreator.ext.naksha.sql.SQLQuery;
import com.here.naksha.lib.core.models.geojson.implementation.FeatureCollection;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.events.FeatureEvent;
import com.here.naksha.lib.core.models.payload.events.feature.QueryEvent;
import com.here.xyz.psql.PsqlHandler;
import com.here.xyz.psql.SQLQueryBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public abstract class GetFeatures<E extends Event> extends ExtendedSpace<E, FeatureCollection> {

    public GetFeatures(@NotNull E event, final @NotNull PsqlHandler psqlConnector) throws SQLException {
        super(event, psqlConnector);
        setUseReadReplica(true);
    }

    @Override
    protected @NotNull SQLQuery buildQuery(@Nonnull E event) throws SQLException {
        boolean isExtended = isExtendedSpace(event);
        SQLQuery query;
        if (isExtended) {
            query = new SQLQuery("SELECT * FROM ("
                    + "    (SELECT ${{selection}}, ${{geo}}${{iColumnExtension}}"
                    + "        FROM ${schema}.${table}"
                    + "        WHERE ${{filterWhereClause}} AND deleted = false ${{iOffsetExtension}} ${{limit}})"
                    + "    UNION ALL "
                    + "        SELECT ${{selection}}, ${{geo}}${{iColumn}} FROM"
                    + "            ("
                    + "                ${{baseQuery}}"
                    + "            ) a WHERE NOT exists(SELECT 1 FROM ${schema}.${table} b WHERE jsondata->>'id' = a.jsondata->>'id')"
                    + ") limitQuery ${{limit}}");
        } else {
            query = new SQLQuery("SELECT ${{selection}}, ${{geo}}${{iColumn}}"
                    + "    FROM ${schema}.${table}"
                    + "    WHERE ${{filterWhereClause}} ${{orderBy}} ${{limit}} ${{offset}}");
        }

        if (event instanceof FeatureEvent)
            query.setQueryFragment("selection", buildSelectionFragment((FeatureEvent) event));
        else query.setQueryFragment("selection", "jsondata");

        query.setQueryFragment("geo", buildGeoFragment(event));
        query.setQueryFragment("iColumn", ""); // NOTE: This can be overridden by implementing subclasses
        query.setQueryFragment("limit", ""); // NOTE: This can be overridden by implementing subclasses

        query.setVariable(SCHEMA, processor.spaceSchema());
        query.setVariable(TABLE, processor.spaceTable());

        if (isExtended) {
            query.setQueryFragment("iColumnExtension", ""); // NOTE: This can be overridden by implementing subclasses
            query.setQueryFragment("iOffsetExtension", "");

            SQLQuery baseQuery = !is2LevelExtendedSpace(event)
                    ? build1LevelBaseQuery(getExtendedTable(event)) // 1-level extension
                    : build2LevelBaseQuery(getIntermediateTable(event), getExtendedTable(event)); // 2-level extension

            query.setQueryFragment("baseQuery", baseQuery);

            query.setQueryFragment("iColumnBase", ""); // NOTE: This can be overridden by implementing subclasses
            query.setQueryFragment("iOffsetBase", ""); // NOTE: This can be overridden by implementing subclasses
            query.setQueryFragment(
                    "iColumnIntermediate", ""); // NOTE: This can be overridden by implementing subclasses
            query.setQueryFragment(
                    "iOffsetIntermediate", ""); // NOTE: This can be overridden by implementing subclasses
        } else {
            query.setQueryFragment("orderBy", ""); // NOTE: This can be overridden by implementing subclasses
            query.setQueryFragment("offset", ""); // NOTE: This can be overridden by implementing subclasses
        }

        return query;
    }

    private SQLQuery build1LevelBaseQuery(String extendedTable) {
        SQLQuery query = new SQLQuery("SELECT jsondata, geo${{iColumnBase}}"
                + "    FROM ${schema}.${extendedTable} m"
                + "    WHERE ${{filterWhereClause}} ${{iOffsetBase}} ${{limit}}"); // in the base
        // table there is
        // no need to
        // check a
        // deleted flag;
        query.setVariable("extendedTable", extendedTable);
        return query;
    }

    private SQLQuery build2LevelBaseQuery(String intermediateTable, String extendedTable) {
        SQLQuery query = new SQLQuery(
                "(SELECT jsondata, geo${{iColumnIntermediate}}"
                        + "    FROM ${schema}.${intermediateExtensionTable}"
                        + "    WHERE ${{filterWhereClause}} AND deleted = false ${{iOffsetIntermediate}} ${{limit}})"
                        + "UNION ALL"
                        + "    SELECT jsondata, geo${{iColumn}} FROM"
                        + "        ("
                        + "            ${{innerBaseQuery}}"
                        + "        ) b WHERE NOT exists(SELECT 1 FROM ${schema}.${intermediateExtensionTable} WHERE jsondata->>'id' = b.jsondata->>'id')");
        query.setVariable("intermediateExtensionTable", intermediateTable);
        query.setQueryFragment("innerBaseQuery", build1LevelBaseQuery(extendedTable));
        return query;
    }

    @Nonnull
    @Override
    public FeatureCollection handle(@Nonnull ResultSet rs) throws SQLException {
        return processor.defaultFeatureResultSetHandler(rs);
    }

    protected static SQLQuery buildSelectionFragment(FeatureEvent event) {
        if (event.getSelection() == null || event.getSelection().size() == 0) return new SQLQuery("jsondata");

        List<String> selection = event.getSelection();
        if (!selection.contains("type")) {
            selection = new ArrayList<>(selection);
            selection.add("type");
        }

        return new SQLQuery("(SELECT "
                        + "CASE WHEN prj_build ?? 'properties' THEN prj_build "
                        + "ELSE jsonb_set(prj_build,'{properties}','{}'::jsonb) "
                        + "END "
                        + "FROM prj_build(#{selection}, jsondata)) AS jsondata")
                .withNamedParameter("selection", selection.toArray(new String[0]));
    }

    // TODO: Can be removed after completion of refactoring
    @Deprecated
    public static SQLQuery buildSelectionFragmentBWC(QueryEvent event) {
        SQLQuery selectionFragment = buildSelectionFragment(event);
        selectionFragment.replaceNamedParameters();
        return selectionFragment;
    }

    protected SQLQuery buildGeoFragment(E event) {
        return buildGeoFragment(event, true, null);
    }

    protected SQLQuery buildGeoFragment(Event event, SQLQuery geoOverride) {
        return buildGeoFragment(event, true, geoOverride);
    }

    protected static SQLQuery buildGeoFragment(Event event, boolean convertToGeoJson) {
        return buildGeoFragment(event, convertToGeoJson, null);
    }

    protected static SQLQuery buildGeoFragment(Event event, boolean convertToGeoJson, SQLQuery geoOverride) {
        boolean isForce2D = event instanceof FeatureEvent && ((FeatureEvent) event).isForce2D();
        String geo = geoOverride != null ? "${{geoOverride}}" : ((isForce2D ? "ST_Force2D" : "ST_Force3D") + "(geo)");

        if (convertToGeoJson)
            geo = "replace(ST_AsGeojson(" + geo + ", " + SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS + "), 'nan', '0')";

        SQLQuery geoFragment = new SQLQuery(geo + " as geo");
        if (geoOverride != null) geoFragment.setQueryFragment("geoOverride", geoOverride);

        return geoFragment;
    }
}
