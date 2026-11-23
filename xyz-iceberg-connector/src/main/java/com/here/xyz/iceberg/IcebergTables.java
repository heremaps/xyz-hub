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

import static com.here.xyz.responses.XyzError.TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
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
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;

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
  private static final int MAX_ROW_GROUP_SIZE_ENTRIES = 10_000;
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
  private final ExecutorService rangeReadPool;
  private final RangeReader rangeReader;
  private final Cache<TableIdentifier, Table> tables;
  private final Cache<ScanKey, List<FileScanTask>> scans;
  private final Cache<String, byte[]> footers;
  private final Cache<String, PageIndex> pageIndexes;
  /**
   * Tables (by identifier) and refs (by identifier and ref name) which do not exist
   */
  private final Cache<String, Boolean> missing;
  /**
   * The largest projected row group of a snapshot (see {@link FeatureScan#maxRowGroupBytes(long)})
   */
  private final Cache<RowGroupSizeKey, Long> maxRowGroupBytes;

  private IcebergTables(CatalogKey key) {
    this.key = key;
    catalog = CatalogUtil.buildIcebergCatalog(key.catalogName(), key.catalogProperties(), new Configuration());
    planningPool = Executors.newFixedThreadPool(key.planningThreads(), daemonThreads("iceberg-planning-" + key.catalogName()));
    rangeReadPool = Executors.newFixedThreadPool(key.rangeReadThreads(), daemonThreads("iceberg-range-read-" + key.catalogName()));
    rangeReader = new RangeReader(rangeReadPool);
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
    pageIndexes = Caffeine.newBuilder()
        .maximumWeight((long) (maxMemory * key.pageIndexCacheHeapShare()))
        .<String, PageIndex>weigher((location, index) -> index.bytes().length)
        .build();
    missing = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(key.missingCacheTtlSeconds()))
        .maximumSize(MAX_MISSING_ENTRIES)
        .build();
    maxRowGroupBytes = Caffeine.newBuilder()
        .maximumSize(MAX_ROW_GROUP_SIZE_ENTRIES)
        .build();
  }

  static IcebergTables forConfig(IcebergConnectorConfig config) {
    return INSTANCES.get(config.catalogKey(), IcebergTables::new);
  }

  private void close() {
    logger.info("Closing the idle Iceberg catalog {}", key);
    planningPool.shutdown();
    rangeReadPool.shutdown();
    tables.invalidateAll();
    scans.invalidateAll();
    footers.invalidateAll();
    pageIndexes.invalidateAll();
    missing.invalidateAll();
    maxRowGroupBytes.invalidateAll();
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

  /**
   * The parallel range reads of the two-step reader
   */
  RangeReader rangeReader() {
    return rangeReader;
  }

  /**
   * The cached tail of a data file (its Parquet footer, see {@link TailCachingInputFile}), or null.
   */
  byte[] cachedTail(String location) {
    return footers.getIfPresent(location);
  }

  /**
   * The cached page index of a data file, or null.
   */
  PageIndex cachedPageIndex(String location) {
    return pageIndexes.getIfPresent(location);
  }

  /**
   * Returns the page index of a data file: the bytes of all its column indexes and offset indexes, which Parquet writes as one
   * region between the last row group and the footer. They are read with one range read, which counts against the budget before
   * it starts, and cached by file location (data files are immutable).
   *
   * @param file The data file, not counting its reads (the bytes count against the budget before the read)
   * @return The page index, or null if the file has none
   */
  PageIndex pageIndex(InputFile file, ParquetMetadata footer, ReadBudget budget) {
    PageIndex cached = pageIndexes.getIfPresent(file.location());
    if (cached != null)
      return cached;
    long start = Long.MAX_VALUE, end = 0;
    for (BlockMetaData block : footer.getBlocks())
      for (ColumnChunkMetaData chunk : block.getColumns())
        for (IndexReference reference : new IndexReference[] {chunk.getColumnIndexReference(), chunk.getOffsetIndexReference()})
          if (reference != null) {
            start = Math.min(start, reference.getOffset());
            end = Math.max(end, reference.getOffset() + reference.getLength());
          }
    if (end <= start || end - start > Integer.MAX_VALUE)
      return null;
    int length = (int) (end - start);
    budget.reserve(length);
    PageIndex index = new PageIndex(start, rangeReader.read(file, start, length));
    pageIndexes.put(file.location(), index);
    return index;
  }

  /**
   * Returns the maximum of {@code fileMax} over the data files of the snapshot, cached per snapshot and projection. The files are
   * evaluated in parallel on the planning pool.
   *
   * @param columns The projected columns, part of the cache key
   * @param fileMax The value of one data file
   * @param deadlineMillis The time after which the files are no longer awaited
   * @throws ScanException with TIMEOUT if not all files were evaluated before the deadline; nothing is cached then
   */
  long maxRowGroupBytes(SnapshotFiles files, Set<String> columns, ToLongFunction<FileScanTask> fileMax, long deadlineMillis) {
    RowGroupSizeKey key = new RowGroupSizeKey(files.table().name(), files.snapshotId(), columns);
    Long cached = maxRowGroupBytes.getIfPresent(key);
    if (cached != null)
      return cached;

    List<Future<Long>> futures = new ArrayList<>();
    Set<String> locations = new HashSet<>();
    for (FileScanTask task : files.tasks())
      if (locations.add(task.file().location()))
        futures.add(planningPool.submit(() -> fileMax.applyAsLong(task)));
    long max = 0;
    try {
      for (Future<Long> future : futures) {
        long wait = deadlineMillis - System.currentTimeMillis();
        if (wait <= 0)
          throw new TimeoutException();
        max = Math.max(max, future.get(wait, MILLISECONDS));
      }
    }
    catch (TimeoutException e) {
      futures.forEach(future -> future.cancel(false));
      throw new ScanException(TIMEOUT, "The Iceberg read did not finish in time");
    }
    catch (InterruptedException e) {
      futures.forEach(future -> future.cancel(false));
      Thread.currentThread().interrupt();
      throw new ScanException(TIMEOUT, "The Iceberg read was interrupted");
    }
    catch (ExecutionException e) {
      futures.forEach(future -> future.cancel(false));
      if (e.getCause() instanceof RuntimeException runtimeException)
        throw runtimeException;
      throw new IllegalStateException(e.getCause());
    }
    maxRowGroupBytes.put(key, max);
    return max;
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

  /**
   * The column indexes and offset indexes of a Parquet file.
   *
   * @param offset The position of the first byte in the file
   */
  record PageIndex(long offset, byte[] bytes) {}

  private record ScanKey(TableIdentifier table, long snapshotId) {}

  private record RowGroupSizeKey(String table, long snapshotId, Set<String> columns) {}
}
