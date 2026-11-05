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

package com.here.xyz.psql;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.query.GetChangesetStatistics;
import com.here.xyz.psql.query.GetStatistics;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

class PSQLStatisticsIT extends PSQLAbstractIT {
  private final String table = "statistics_" + UUID.randomUUID().toString().replace("-", "");
  private final Map<String, Object> params = Map.of(TABLE_NAME, table);

  @BeforeEach
  void createStatisticsSpace() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, new Space().withId(table), params);
  }

  @AfterEach
  void deleteStatisticsSpace() throws Exception {
    invokeDeleteTestSpace(defaultTestConnectorParams, table, params);
  }

  @Test
  void emptySpaceHasZeroVersionBounds() throws Exception {
    StatisticsResponse response = statistics(new ArrayList<>());

    assertEquals(0L, response.getMinVersion().getValue());
    assertEquals(0L, response.getMaxVersion().getValue());
    assertFalse(response.getMinVersion().getEstimated());
    assertFalse(response.getMaxVersion().getEstimated());
  }

  @Test
  void statisticsReadOnlyTheMaximumVersionFromThePhysicalTable() throws Exception {
    insertVersions();
    List<String> queries = new ArrayList<>();

    StatisticsResponse response = statistics(queries);

    assertEquals(0L, response.getMinVersion().getValue());
    assertEquals(11L, response.getMaxVersion().getValue());
    assertEquals(2, queries.size());
    assertTrue(queries.get(0).contains("SELECT max(version) FROM \"public\".\"" + table + "\""));
    assertTrue(queries.stream().noneMatch(sql -> sql.toLowerCase(Locale.ROOT).contains("min(version)")));
  }

  @Test
  void changesetStatisticsStillReadTheActualMinimumVersion() throws Exception {
    insertVersions();
    GetChangesetStatistics query = new GetChangesetStatistics(new GetChangesetStatisticsEvent()
        .withSpace("logical-space")
        .withParams(params)
        .withConnectorParams(defaultTestConnectorParams));
    query.setDataSourceProvider(LAMBDA.dataSourceProvider);

    ChangesetsStatisticsResponse response = query.run();

    assertEquals(7L, response.getMinVersion());
    assertEquals(11L, response.getMaxVersion());
  }

  private void insertVersions() throws SQLException {
    new SQLQuery("""
        INSERT INTO ${schema}.${table} (id, version, next_version, operation, jsondata)
        VALUES ('first', 7, max_bigint(), 'I', '{}'::JSONB), ('second', 11, max_bigint(), 'I', '{}'::JSONB)
        """)
        .withVariable("schema", "public")
        .withVariable("table", table)
        .write(LAMBDA.dataSourceProvider);
  }

  private StatisticsResponse statistics(List<String> queries) throws Exception {
    GetStatistics query = new GetStatistics(new GetStatisticsEvent()
        .withSpace("logical-space")
        .withParams(params)
        .withConnectorParams(defaultTestConnectorParams)
        .withMinVersion(9));
    DataSourceProvider delegate = LAMBDA.dataSourceProvider;
    DataSource reader = new PGSimpleDataSource() {
      @Override
      public Connection getConnection() throws SQLException {
        Connection connection = delegate.getReader().getConnection();
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (method.getName().equals("prepareStatement") || method.getName().equals("prepareCall"))
                queries.add((String) args[0]);
              try {
                return method.invoke(connection, args);
              }
              catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
      }
    };
    query.setDataSourceProvider(new DataSourceProvider(null) {
      @Override
      public DatabaseSettings getDatabaseSettings() {
        return delegate.getDatabaseSettings();
      }

      @Override
      public DataSource getReader() {
        return reader;
      }

      @Override
      public DataSource getWriter() {
        throw new AssertionError("Statistics must use the read replica.");
      }

      @Override
      public void close() {}
    });
    return query.run();
  }

}
