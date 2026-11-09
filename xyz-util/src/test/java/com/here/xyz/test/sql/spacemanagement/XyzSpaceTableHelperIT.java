/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.test.sql.spacemanagement;

import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.featurewriter.sql.SQLSpaceWriter;
import com.here.xyz.test.sql.base.SQLITBase;
import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.IndexHelper;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.TableComment;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class XyzSpaceTableHelperIT extends SQLITBase {
    private final String SCHEMA = "public";
    private final String TABLE = getClass().getSimpleName() + "_" + randomAlpha(5);
    private final List<String> extraTables = new ArrayList<>();

    protected SpaceWriter spaceWriter = new SQLSpaceWriter(false, TABLE);

    @BeforeEach
    public void setup() throws Exception {
        spaceWriter.createSpaceResources();
    }

    @AfterEach
    public void tearDown() throws Exception {
        spaceWriter.cleanSpaceResources();
        try (DataSourceProvider dsp = getDataSourceProvider()) {
            for (String table : extraTables)
                SQLQuery.batchOf(XyzSpaceTableHelper.buildCleanUpQuery(SCHEMA, table, "_version_seq", OLD_LAYOUT)).writeBatch(dsp);
        }
    }

    @Test
    public void createTableComment() throws Exception {
      TableLayout v2 = TableLayout.NEW_LAYOUT;

      try (DataSourceProvider dsp = getDataSourceProvider()) {
          SQLQuery addTableComment = XyzSpaceTableHelper.buildAddTableCommentQuery(SCHEMA, TABLE, new TableComment(TABLE, v2));
          addTableComment.write(dsp);

          SQLQuery readTableComment = XyzSpaceTableHelper.buildReadTableCommentQuery(SCHEMA, TABLE);
          readTableComment.run(dsp, rs -> {
              if (rs.next()) {
                  TableComment comment;
                  try {
                      comment = XyzSerializable.deserialize(rs.getString("comment"), TableComment.class);
                      Assertions.assertEquals(TableLayout.NEW_LAYOUT, comment.tableLayout());
                      Assertions.assertEquals(TABLE, comment.spaceId());
                  } catch (JsonProcessingException e) {
                      throw new RuntimeException(e);
                  }
              } else {
                  Assertions.fail("Table comment not found for " + TABLE);
              }
              return null;
          });
      }
    }

    @Test
    public void fullSystemIndexListCreatesTheSameRelations() throws Exception {
        //All three names have the same length, so generated partition index names are truncated alike
        String defaultTable = newTable("d");
        String fullListTable = newTable("f");

        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery.batchOf(XyzSpaceTableHelper.buildCreateSpaceTableQueries(SCHEMA, defaultTable, List.of(), defaultTable, OLD_LAYOUT))
                .writeBatch(dsp);
            SQLQuery.batchOf(XyzSpaceTableHelper.buildCreateSpaceTableQueries(SCHEMA, fullListTable, List.of(), fullListTable, OLD_LAYOUT,
                IndexHelper.systemIndexes(OLD_LAYOUT))).writeBatch(dsp);

            Set<String> defaultRows = catalogRows(dsp, defaultTable);
            Assertions.assertFalse(defaultRows.isEmpty());
            Assertions.assertEquals(defaultRows, catalogRows(dsp, fullListTable));
            Assertions.assertEquals(systemIndexNames(List.of(SystemIndex.values())), rootSystemIndexNames(dsp, defaultTable));
        }
    }

    @Test
    public void systemIndexWhitelistCreatesOnlyThoseIndexes() throws Exception {
        String defaultTable = newTable("d");
        String whitelistTable = newTable("w");
        List<SystemIndex> whitelist = List.of(SystemIndex.VERSION_ID, SystemIndex.OPERATION);

        try (DataSourceProvider dsp = getDataSourceProvider()) {
            SQLQuery.batchOf(XyzSpaceTableHelper.buildCreateSpaceTableQueries(SCHEMA, defaultTable, List.of(), defaultTable, OLD_LAYOUT))
                .writeBatch(dsp);
            SQLQuery.batchOf(XyzSpaceTableHelper.buildCreateSpaceTableQueries(SCHEMA, whitelistTable, List.of(), whitelistTable,
                OLD_LAYOUT, whitelist)).writeBatch(dsp);

            Assertions.assertEquals(systemIndexNames(whitelist), rootSystemIndexNames(dsp, whitelistTable));

            //The relations other than indexes are the same as those of a default create
            Assertions.assertEquals(withoutIndexes(catalogRows(dsp, defaultTable)), withoutIndexes(catalogRows(dsp, whitelistTable)));
            //The constraint indexes stay
            Set<String> constraintIndexes = Set.of("<t>_primKey", "<t>_unique");
            Assertions.assertTrue(catalogRows(dsp, whitelistTable).stream()
                .filter(row -> row.startsWith("index:"))
                .map(row -> row.split(":")[1])
                .collect(Collectors.toSet())
                .containsAll(constraintIndexes));
        }
    }

    private String newTable(String suffix) {
        String table = TABLE + "_" + suffix;
        extraTables.add(table);
        return table;
    }

    private static Set<String> withoutIndexes(Set<String> rows) {
        return rows.stream()
            .filter(row -> !row.startsWith("index:") && !row.endsWith(":i") && !row.endsWith(":I"))
            .collect(Collectors.toCollection(TreeSet::new));
    }

    private static Set<String> systemIndexNames(List<SystemIndex> indexes) {
        return indexes.stream().map(index -> index.getIndexName("<t>")).collect(Collectors.toCollection(TreeSet::new));
    }

    private Set<String> rootSystemIndexNames(DataSourceProvider dsp, String table) throws SQLException {
        return new SQLQuery("""
            SELECT replace(ic.relname, #{table}, '<t>') AS name FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              JOIN pg_class c ON c.oid = i.indrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = #{schema} AND c.relname = #{table} AND starts_with(ic.relname, 'idx_')
            """)
            .withNamedParameter("schema", SCHEMA)
            .withNamedParameter("table", table)
            .run(dsp, rs -> {
                Set<String> names = new TreeSet<>();
                while (rs.next())
                    names.add(rs.getString("name"));
                return names;
            });
    }

    /**
     * Returns one row per relation (name and kind) and one per index (name, table and definition) of the given table, its
     * partitions and sequences, with the table name replaced by "<t>".
     */
    private Set<String> catalogRows(DataSourceProvider dsp, String table) throws SQLException {
        return new SQLQuery("""
            SELECT replace(c.relname, #{table}, '<t>') || ':' || c.relkind::TEXT AS row FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = #{schema} AND starts_with(c.relname, #{table})
            UNION ALL
            SELECT 'index:' || replace(ic.relname, #{table}, '<t>') || ':' || replace(c.relname, #{table}, '<t>') || ':'
                || replace(pg_get_indexdef(i.indexrelid), #{table}, '<t>') FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              JOIN pg_class c ON c.oid = i.indrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = #{schema} AND starts_with(c.relname, #{table})
            """)
            .withNamedParameter("schema", SCHEMA)
            .withNamedParameter("table", table)
            .run(dsp, rs -> {
                Set<String> rows = new TreeSet<>();
                while (rs.next())
                    rows.add(rs.getString("row"));
                return rows;
            });
    }
}
