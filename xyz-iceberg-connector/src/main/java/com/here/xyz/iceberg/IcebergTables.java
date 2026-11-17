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

package com.here.xyz.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.here.xyz.iceberg.IcebergConnectorConfig.CatalogKey;
import com.here.xyz.models.hub.IcebergLocation;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The catalog and the caches of one connector configuration. The Hub creates a new connector instance per request, so the instances
 * of this class are kept in a static cache. An instance which was not used for {@link #IDLE_TIMEOUT} is closed and removed, so a
 * changed configuration (or a rotated placeholder value) does not keep the catalog, the thread pool and the caches of the old one.
 */
final class IcebergTables {

  private static final Logger logger = LogManager.getLogger(IcebergTables.class);

  /**
   * The columns whose statistics are kept in the planned scans, used for pruning files, in addition to the filter columns.
   */
  static final List<String> BBOX_STATS_COLUMNS = List.of("bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax");
  private static final int TASK_WEIGHT_BYTES = 2048;
  private static final int MAX_MISSING_ENTRIES = 10_000;
  /**
   * Much longer than a request may take, so an instance is never closed while a request uses it.
   */
  static final Duration IDLE_TIMEOUT = Duration.ofMinutes(10);
  private static final Cache<CatalogKey, IcebergTables> INSTANCES = Caffeine.newBuilder()
      .expireAfterAccess(IDLE_TIMEOUT)
      .scheduler(Scheduler.systemScheduler())
      .<CatalogKey, IcebergTables>removalListener((key, instance, cause) -> {
        if (instance != null && cause != RemovalCause.REPLACED)
          instance.close();
      })
      .build();

  private final CatalogKey key;
  private final Catalog catalog;
  private final ExecutorService planningPool;
  private final Cache<TableIdentifier, Table> tables;
  private final Cache<ScanKey, List<FileScanTask>> scans;
  private final Cache<String, byte[]> footers;
  /**
   * Tables (by identifier) and refs (by identifier and ref name) which do not exist
   */
  private final Cache<String, Boolean> missing;

  private IcebergTables(CatalogKey key) {
    this.key = key;
    catalog = CatalogUtil.buildIcebergCatalog(key.catalogName(), key.catalogProperties(), new Configuration());
    planningPool = Executors.newFixedThreadPool(key.planningThreads(), daemonThreads("iceberg-planning-" + key.catalogName()));
    long maxMemory = Runtime.getRuntime().maxMemory();
    tables = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(key.tableCacheTtlSeconds()))
        .build();
    scans = Caffeine.newBuilder()
        .maximumWeight((long) (maxMemory * key.scanCacheHeapShare()))
        .<ScanKey, List<FileScanTask>>weigher((scanKey, tasks) -> (int) Math.min(Integer.MAX_VALUE,
            (long) Math.max(1, tasks.size()) * TASK_WEIGHT_BYTES))
        .build();
    footers = Caffeine.newBuilder()
        .maximumWeight((long) (maxMemory * key.footerCacheHeapShare()))
        .<String, byte[]>weigher((location, tail) -> tail.length)
        .build();
    missing = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(key.missingCacheTtlSeconds()))
        .maximumSize(MAX_MISSING_ENTRIES)
        .build();
  }

  static IcebergTables forConfig(IcebergConnectorConfig config) {
    return INSTANCES.get(config.catalogKey(), IcebergTables::new);
  }

  private void close() {
    logger.info("Closing the idle Iceberg catalog {}", key);
    planningPool.shutdown();
    tables.invalidateAll();
    scans.invalidateAll();
    footers.invalidateAll();
    missing.invalidateAll();
    if (catalog instanceof Closeable closeable) {
      try {
        closeable.close();
      }
      catch (IOException | RuntimeException e) {
        logger.warn("Unable to close the Iceberg catalog {}", key, e);
      }
    }
  }

  /**
   * Resolves the tag of the location to its snapshot and returns the data files of that snapshot.
   * Only tags are accepted: the Hub caches the responses of a version as immutable, and a branch can move.
   * A missing table or ref is remembered for {@code missingCacheTtlSeconds}.
   *
   * @throws NotFoundException if the ref does not exist in the table
   * @throws NoSuchTableException if the table does not exist
   * @throws IllegalArgumentException if the ref is a branch
   */
  SnapshotFiles resolve(IcebergLocation location) {
    TableIdentifier identifier = TableIdentifier.of(Namespace.of(location.namespace().split("\\.")), location.table());
    String tableKey = identifier.toString();
    String refKey = tableKey + "@" + location.ref();
    if (missing.getIfPresent(tableKey) != null)
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    if (missing.getIfPresent(refKey) != null)
      throw new NotFoundException("Ref %s does not exist in the Iceberg table %s", location.ref(), identifier);

    Table table = loadTable(identifier, tableKey);
    SnapshotRef ref = table.refs().get(location.ref());
    if (ref == null) {
      //The ref might have been created after the table was cached
      tables.invalidate(identifier);
      table = loadTable(identifier, tableKey);
      ref = table.refs().get(location.ref());
    }
    if (ref == null) {
      missing.put(refKey, Boolean.TRUE);
      throw new NotFoundException("Ref %s does not exist in the Iceberg table %s", location.ref(), identifier);
    }
    if (!ref.isTag())
      throw new IllegalArgumentException("The ref " + location.ref() + " of the Iceberg table " + identifier + " is not a tag");

    long snapshotId = ref.snapshotId();
    Snapshot snapshot = table.snapshot(snapshotId);
    Schema schema = snapshot != null && snapshot.schemaId() != null && table.schemas().containsKey(snapshot.schemaId())
        ? table.schemas().get(snapshot.schemaId())
        : table.schema();
    Table scanTable = table;
    List<FileScanTask> tasks = scans.get(new ScanKey(identifier, snapshotId), k -> planFiles(scanTable, schema, snapshotId));
    return new SnapshotFiles(table, schema, snapshotId, tasks);
  }

  private Table loadTable(TableIdentifier identifier, String tableKey) {
    try {
      return tables.get(identifier, catalog::loadTable);
    }
    catch (NoSuchTableException | NoSuchNamespaceException e) {
      missing.put(tableKey, Boolean.TRUE);
      throw e;
    }
  }

  /**
   * Opens a data file so that the bytes read count against the budget and the Parquet footer comes from the footer cache.
   */
  InputFile dataFile(InputFile file, ReadBudget budget) {
    return new TailCachingInputFile(new CountingInputFile(file, budget), footers);
  }

  private List<FileScanTask> planFiles(Table table, Schema schema, long snapshotId) {
    List<String> statsColumns = Stream.concat(BBOX_STATS_COLUMNS.stream(), key.statsColumns().stream())
        .filter(column -> schema.findField(column) != null).toList();
    List<FileScanTask> tasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> planned = table.newScan()
        .useSnapshot(snapshotId)
        .planWith(planningPool)
        .includeColumnStats(statsColumns)
        .planFiles()) {
      planned.forEach(tasks::add);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    tasks.sort(Comparator.<FileScanTask, String>comparing(task -> task.file().location()).thenComparingLong(FileScanTask::start));
    return List.copyOf(tasks);
  }

  private static java.util.concurrent.ThreadFactory daemonThreads(String prefix) {
    AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  /**
   * The data files of a snapshot, sorted by location. The position of a file in the list is stable for the snapshot, so it can be
   * used in page tokens.
   */
  record SnapshotFiles(Table table, Schema schema, long snapshotId, List<FileScanTask> tasks) {}

  private record ScanKey(TableIdentifier table, long snapshotId) {}
}
