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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.db.pg.LockHelper;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Concurrent first writes, a deletion racing a first write, and the timeout of a materialization.
 */
public class PSQLLazyConcurrencyIT extends LazyTablesITBase {
  private static final Set<String> SCHEMAS = ConcurrentHashMap.newKeySet();
  private static final int THREADS = 6;
  private static final org.apache.logging.log4j.core.Logger MATERIALIZATION_LOGGER =
      (org.apache.logging.log4j.core.Logger) LogManager.getLogger(LazySpaceTables.class);
  private static TestConnector connector;

  @Override
  protected Set<String> schemas() {
    return SCHEMAS;
  }

  @BeforeAll
  static void createConnector() throws Exception {
    //Parallel requests like those of several containers, each with a connection of its own
    connector = new PSQLLazyConcurrencyIT().newConnector().with("lazyTableCreation", true).with("dbMaxPoolSize", THREADS + 2);
    MATERIALIZATION_LOGGER.addAppender(APPENDER);
  }

  @AfterAll
  static void cleanUp() throws Exception {
    MATERIALIZATION_LOGGER.removeAppender(APPENDER);
    dropSchemas(SCHEMAS);
  }

  private static Set<String> readIds(String table, List<String> ids) throws Exception {
    FeatureCollection read = assertFeatureCollection(invoke(connector, new GetFeaturesByIdEvent().withSpace("s")
        .withParams(lazyParams(table)).withIds(ids)));
    Set<String> found = new TreeSet<>();
    for (Feature feature : read.getFeatures())
      found.add(feature.getId());
    return found;
  }

  @Test
  public void parallelFirstWritesCreateTheTableOnce() throws Exception {
    String table = newTable();
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    List<String> ids = new ArrayList<>();
    try {
      List<Future<XyzResponse>> results = new ArrayList<>();
      for (int i = 0; i < THREADS; i++) {
        String id = "P" + i;
        ids.add(id);
        boolean legacyWriter = i % 2 == 0;
        results.add(executor.submit(() -> {
          start.await();
          return invoke(connector, legacyWriter ? insertEvent("s", true, lazyWriteParams(table), id)
              : writeFeaturesEvent("s", lazyWriteParams(table), id));
        }));
      }
      start.countDown();
      for (Future<XyzResponse> result : results)
        assertEquals(Boolean.TRUE, assertFeatureCollection(result.get(60, TimeUnit.SECONDS)).getMaterialized());
    }
    finally {
      executor.shutdownNow();
    }
    assertEquals(1, logLines("INFO Materialized table " + table + " ").size(), "The table must be created once");
    assertEquals(new TreeSet<>(ids), readIds(table, ids), "All writes must be visible");
  }

  @Test
  public void deleteRacingFirstWrite() throws Exception {
    String eagerTable = newTable();
    createEager(connector, eagerTable);
    Set<String> fullSet = catalogRows(connector.schema(), eagerTable);

    for (int round = 0; round < 4; round++) {
      String table = newTable();
      String spaceId = newId();
      CountDownLatch start = new CountDownLatch(1);
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<XyzResponse> write = executor.submit(() -> {
          start.await();
          return invoke(connector, insertEvent(spaceId, true, lazyWriteParams(table), "R1"));
        });
        Future<XyzResponse> delete = executor.submit(() -> {
          start.await();
          return invoke(connector, modifySpaceEvent(Operation.DELETE, spaceId, lazyParams(table)));
        });
        start.countDown();
        assertOk(delete.get(60, TimeUnit.SECONDS));
        XyzResponse written = write.get(60, TimeUnit.SECONDS);

        //Either the table set is complete or it is gone, the lock serializes the creation and the drop
        Set<String> rows = catalogRows(connector.schema(), table);
        assertTrue(rows.isEmpty() || rows.equals(fullSet), "Partial table set: " + rows);
        if (written instanceof ErrorResponse error)
          assertEquals(XyzError.NOT_FOUND, error.getError(), error.serialize());
        else if (!rows.isEmpty())
          assertEquals(Set.of("R1"), readIds(table, List.of("R1")), "A reported write must be readable");
      }
      finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void materializationTimeoutIsTimeout() throws Exception {
    String table = newTable();
    TestConnector shortBudget = connector.with("lazyMaterializationMaxBudget", 3000);
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection lockConnection = dsp.getWriter().getConnection()) {
      //The session lock a space creation or deletion holds
      LockHelper.advisoryLock(table, lockConnection);
      long startMillis = System.currentTimeMillis();
      XyzResponse response = invoke(shortBudget, insertEvent(newId(), true, lazyWriteParams(table), "T1"));
      long duration = System.currentTimeMillis() - startMillis;
      LockHelper.advisoryUnlock(table, lockConnection);

      ErrorResponse error = assertError(XyzError.TIMEOUT, response);
      assertEquals("Materialization of the space table timed out, retry is safe", error.getErrorMessage());
      assertTrue(duration >= 2_500 && duration < 8_000, "The write must time out after the budget of 3 s, took " + duration + " ms");
    }
    assertFalse(relationExists(connector.schema(), table));
    assertEquals(1, logLines("WARN Table " + table + " of space ").size(), "The resolver logs the timeout");
  }
}
