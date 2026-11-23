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

import static com.here.xyz.iceberg.FeatureScan.FEATURE_JSON;
import static com.here.xyz.iceberg.FeatureScan.GEOMETRY;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

import com.here.xyz.iceberg.FeatureScan.ColumnFilter;
import com.here.xyz.iceberg.FeatureScan.ProjectedColumns;
import com.here.xyz.iceberg.FeatureScan.RowGroup;
import com.here.xyz.iceberg.FeatureScan.Stats;
import com.here.xyz.iceberg.IcebergTables.PageIndex;
import com.here.xyz.iceberg.IcebergTables.SnapshotFiles;
import com.here.xyz.iceberg.RangeReader.FileRanges;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * The two-step page-index reader of a spatial read. It returns exactly the features of the row-group reader of
 * {@link FeatureScan}, in the same order (files in task order, rows in file order, the limit applied in that order), but reads
 * far fewer bytes for a small area:
 * <ol>
 *   <li>Round 0, only for files whose footer or page index is not cached yet: the footers, the page indexes (all column indexes
 *   and offset indexes of a file, one range read, see {@link IcebergTables#pageIndex}) and the position deletes, in parallel.</li>
 *   <li>Planning: the row groups of a task (Parquet's midpoint rule), pruned by their bbox statistics, and their row ranges from
 *   the column indexes of {@code bbox.*} and of the filter columns.</li>
 *   <li>Round 1, one parallel fetch for all files: the pages of {@code bbox.*} and of the filter columns which overlap the row
 *   ranges. The rows whose bbox overlaps the query envelope, whose filter columns match and which are not deleted are kept.</li>
 *   <li>Round 2: only the {@code geometry} and {@code feature_json} pages which hold the kept rows. Then the exact JTS check and
 *   the response limit, row by row in file order. If the read has a limit, round 2 is split into batches of a few times the
 *   missing features, so that a read which ends early does not fetch the pages of all candidates.</li>
 * </ol>
 * A read without a limit runs the rounds once for all file tasks. A read with a limit runs them on consecutive groups of tasks,
 * in task order, and stops when the limit is reached: each group holds an estimated twice the missing features (from the row
 * counts and bbox bounds of the files) and at least twice the tasks of the previous group. So a small read is one group, and a
 * read over a large area does not read the page indexes and bbox pages of all its files. If the round 1 of a group does not fit
 * into the rest of the budget, the row-group reader reads the remaining tasks in order, as it may reach the limit earlier.
 * <p>
 * All pages are read with bounded, parallel range reads, which count against the byte budget before they start. Each column is
 * decoded on its own with <em>page-aligned</em> row ranges, i.e. whole pages: parquet-java returns values of the wrong rows when
 * the row ranges of a filtered read skip rows inside a page of a column whose pages are not aligned with the other columns. The
 * column reader decompresses one page at a time.
 * <p>
 * A file task is read by the row-group reader instead if it has equality deletes, if one of the columns is missing in the file or
 * has an unexpected type, or if a column chunk has no offset index.
 */
final class TwoStepScan {

  private static final List<String> BBOX_COLUMNS = List.of("bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax");
  private static final ParquetReadOptions READ_OPTIONS = ParquetReadOptions.builder()
      .useStatsFilter(false)
      .useDictionaryFilter(false)
      .useBloomFilter(false)
      .useColumnIndexFilter(false)
      .build();
  private static final int CHECK_INTERVAL_VALUES = 1024;
  /**
   * The smallest batch of kept rows of round 2 if the read has a limit
   */
  private static final int MIN_BATCH_ROWS = 64;
  /**
   * A group of file tasks of a read with a limit holds an estimated this many times the missing features
   */
  private static final double GROUP_MATCHES_FACTOR = 2;
  private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

  private final FeatureScan scan;
  private final IcebergTables tables;
  private final SnapshotFiles files;
  private final Envelope envelope;
  private final PreparedGeometry geometry;
  private final List<List<ColumnFilter>> filters;
  private final ReadBudget budget;
  private final Stats stats;
  private final List<String> round1Columns;
  private final WKBReader wkbReader = new WKBReader();
  /**
   * The table fields of bbox.xmin, bbox.ymin, bbox.xmax and bbox.ymax (an element is null if the field is missing)
   */
  private final List<NestedField> bboxFields;
  private long checkCounter;
  /**
   * Set once the round 1 of a group of a read with a limit did not fit into the budget
   */
  private boolean rowGroupReader;
  private long twoStepTasks;
  private long rowGroupTasks;

  TwoStepScan(FeatureScan scan, IcebergTables tables, SnapshotFiles files, Envelope envelope, PreparedGeometry geometry,
      List<List<ColumnFilter>> filters, ReadBudget budget, Stats stats) {
    this.scan = scan;
    this.tables = tables;
    this.files = files;
    this.envelope = envelope;
    this.geometry = geometry;
    this.filters = filters;
    this.budget = budget;
    this.stats = stats;
    List<String> columns = new ArrayList<>(BBOX_COLUMNS);
    if (filters != null)
      filters.forEach(allOf -> allOf.forEach(filter -> columns.add(filter.column())));
    round1Columns = List.copyOf(new LinkedHashSet<>(columns));
    bboxFields = BBOX_COLUMNS.stream().map(name -> files.schema().findField(name)).toList();
  }

  /**
   * Reads the file tasks, which were already pruned by their column statistics, in their order.
   *
   * @return true if the limit was reached
   */
  boolean run(List<FileScanTask> tasks) throws IOException {
    List<FileTask> fileTasks = new ArrayList<>();
    for (FileScanTask task : tasks)
      fileTasks.add(new FileTask(task));

    try {
      if (!limited())
        return runGroup(fileTasks);
      int from = 0, minFiles = 1;
      while (from < fileTasks.size()) {
        int to = groupEnd(fileTasks, from, minFiles);
        if (runGroup(fileTasks.subList(from, to)))
          return true;
        minFiles = 2 * (to - from);
        from = to;
      }
      return false;
    }
    finally {
      stats.reader = rowGroupTasks == 0 ? "TS" : twoStepTasks == 0 ? "RG" : "TS+RG";
    }
  }

  /**
   * Reads consecutive file tasks with rounds 0 to 2.
   *
   * @return true if the limit was reached
   */
  private boolean runGroup(List<FileTask> fileTasks) throws IOException {
    if (rowGroupReader)
      fileTasks.forEach(fileTask -> fileTask.twoStep = false);
    else {
      round0(fileTasks);
      long cpu = cpuNanos();
      for (FileTask fileTask : fileTasks)
        if (fileTask.twoStep)
          plan(fileTask);
      round1(fileTasks);
      stats.round1Nanos += cpuNanos() - cpu;
    }
    for (FileTask fileTask : fileTasks)
      if (fileTask.twoStep)
        twoStepTasks++;
      else
        rowGroupTasks++;
    return round2(fileTasks);
  }

  /**
   * Whether the read has a limit, so that it may end before it reads all file tasks
   */
  private boolean limited() {
    return scan.missingFeatures() <= Integer.MAX_VALUE / 4;
  }

  /**
   * The end of the next group of file tasks of a read with a limit: at least {@code minFiles} tasks, and as many as are
   * estimated to hold {@link #GROUP_MATCHES_FACTOR} times the missing features. After the row-group reader took over, all
   * remaining tasks.
   */
  private int groupEnd(List<FileTask> fileTasks, int from, int minFiles) {
    if (rowGroupReader)
      return fileTasks.size();
    double target = GROUP_MATCHES_FACTOR * scan.missingFeatures(), estimated = 0;
    int to = from;
    while (to < fileTasks.size() && (to - from < minFiles || estimated < target))
      estimated += estimatedMatches(fileTasks.get(to++).task);
    return to;
  }

  /**
   * The rows of a file which overlap the query envelope, estimated from the file's row count and bbox bounds, as if the rows were
   * spread evenly over the extent of the file. If the bounds are missing, all rows of the file.
   */
  private double estimatedMatches(FileScanTask task) {
    DataFile file = task.file();
    Double minX = bound(file.lowerBounds(), bboxFields.get(0)), minY = bound(file.lowerBounds(), bboxFields.get(1)),
        maxX = bound(file.upperBounds(), bboxFields.get(2)), maxY = bound(file.upperBounds(), bboxFields.get(3));
    if (minX == null || minY == null || maxX == null || maxY == null)
      return file.recordCount();
    return file.recordCount() * overlap(minX, maxX, envelope.getMinX(), envelope.getMaxX())
        * overlap(minY, maxY, envelope.getMinY(), envelope.getMaxY());
  }

  private static Double bound(Map<Integer, ByteBuffer> bounds, NestedField field) {
    ByteBuffer bound = field == null || bounds == null ? null : bounds.get(field.fieldId());
    if (bound == null)
      return null;
    Object value = Conversions.fromByteBuffer(field.type(), bound);
    return value instanceof Double d && !d.isNaN() ? d : null;
  }

  /**
   * The share of the extent [min, max] of a file which [queryMin, queryMax] overlaps
   */
  private static double overlap(double min, double max, double queryMin, double queryMax) {
    if (max <= min)
      return min <= queryMax && max >= queryMin ? 1 : 0;
    return Math.max(0, Math.min(max, queryMax) - Math.max(min, queryMin)) / (max - min);
  }

  /**
   * Loads the footers, the page indexes and the position deletes of the tasks which can be read in two steps, in parallel.
   */
  private void round0(List<FileTask> fileTasks) {
    budget.checkDeadline();
    long bytesBefore = budget.bytesRead();
    List<Callable<Long>> jobs = new ArrayList<>();
    for (FileTask fileTask : fileTasks)
      if (fileTask.twoStep)
        jobs.add(fileTask::load);
    stats.pageIndexBytes += tables.rangeReader().invokeAll(jobs, budget.deadlineMillis()).stream().mapToLong(Long::longValue).sum();
    if (budget.bytesRead() > bytesBefore)
      stats.rounds++;
  }

  /**
   * Plans the row groups of a task and their row ranges, or switches the task to the row-group reader.
   */
  private void plan(FileTask fileTask) throws IOException {
    if (fileTask.pageIndex == null || !fileTask.resolveColumns()) {
      fileTask.twoStep = false;
      return;
    }
    FilterPredicate predicate = fileTask.predicate();
    Set<ColumnPath> allColumns = new LinkedHashSet<>();
    for (ColumnDescriptor column : fileTask.footer.getFileMetaData().getSchema().getColumns())
      allColumns.add(ColumnPath.get(column.getPath()));
    Set<ColumnPath> needed = new LinkedHashSet<>();
    fileTask.columns.values().forEach(column -> needed.add(ColumnPath.get(column.getPath())));

    long splitStart = fileTask.task.start(), splitEnd = fileTask.task.start() + fileTask.task.length();
    List<BlockMetaData> blocks = fileTask.footer.getBlocks();
    List<RowGroup> rowGroups = FeatureScan.rowGroups(fileTask.footer, new ProjectedColumns(Set.of(), Set.of()));
    try (ParquetFileReader reader = fileTask.reader(fileTask.columns.values())) {
      for (RowGroup rowGroup : rowGroups) {
        //Like Parquet's split filter: a row group belongs to the split which contains its midpoint
        long midpoint = rowGroup.start() + rowGroup.length() / 2;
        if (midpoint < splitStart || midpoint >= splitEnd || rowGroup.rowCount() == 0)
          continue;
        BlockMetaData block = blocks.get(rowGroup.index());
        Map<ColumnPath, ColumnChunkMetaData> chunks = new HashMap<>();
        for (ColumnChunkMetaData chunk : block.getColumns())
          if (needed.contains(chunk.getPath()))
            chunks.put(chunk.getPath(), chunk);
        for (ColumnPath path : needed)
          if (chunks.get(path) == null || chunks.get(path).getOffsetIndexReference() == null) {
            fileTask.twoStep = false;
            fileTask.rowGroups.clear();
            return;
          }
        if (!bboxMayOverlap(fileTask, chunks))
          continue;
        ColumnIndexStore indexes = reader.getColumnIndexStore(rowGroup.index());
        RowRanges ranges = ColumnIndexFilter.calculateRowRanges(FilterCompat.get(predicate), indexes, allColumns,
            rowGroup.rowCount());
        if (ranges.rowCount() == 0)
          continue;
        Map<String, Chunk> columnChunks = new HashMap<>();
        fileTask.columns.forEach((name, column) -> {
          ColumnPath path = ColumnPath.get(column.getPath());
          columnChunks.put(name, new Chunk(column, chunks.get(path), indexes.getOffsetIndex(path)));
        });
        fileTask.rowGroups.add(new RowGroupPlan(rowGroup.index(), rowGroup.firstRow(), rowGroup.rowCount(), ranges, columnChunks));
      }
    }
  }

  /**
   * Whether the bbox statistics of the row group allow an overlap with the query envelope.
   */
  private boolean bboxMayOverlap(FileTask fileTask, Map<ColumnPath, ColumnChunkMetaData> chunks) {
    Double xmin = statistic(chunks, fileTask.columns.get("bbox.xmin"), true),
        ymin = statistic(chunks, fileTask.columns.get("bbox.ymin"), true),
        xmax = statistic(chunks, fileTask.columns.get("bbox.xmax"), false),
        ymax = statistic(chunks, fileTask.columns.get("bbox.ymax"), false);
    return (xmax == null || xmax >= envelope.getMinX()) && (xmin == null || xmin <= envelope.getMaxX())
        && (ymax == null || ymax >= envelope.getMinY()) && (ymin == null || ymin <= envelope.getMaxY());
  }

  private static Double statistic(Map<ColumnPath, ColumnChunkMetaData> chunks, ColumnDescriptor column, boolean min) {
    Statistics<?> statistics = chunks.get(ColumnPath.get(column.getPath())).getStatistics();
    if (statistics == null || statistics.isEmpty() || !statistics.hasNonNullValue())
      return null;
    Object value = min ? statistics.genericGetMin() : statistics.genericGetMax();
    return value instanceof Double d && !d.isNaN() ? d : null;
  }

  /**
   * Fetches the bbox and filter pages of the planned row ranges of all tasks, then keeps the rows whose bbox overlaps, whose
   * filters match and which are not deleted.
   */
  private void round1(List<FileTask> fileTasks) throws IOException {
    budget.checkDeadline();
    long pagesPlanned = 0;
    Map<String, List<long[]>> pageRanges = new LinkedHashMap<>();
    Map<String, FileTask> byLocation = new LinkedHashMap<>();
    for (FileTask fileTask : fileTasks)
      for (RowGroupPlan rowGroup : fileTask.rowGroups) {
        RowSelection selection = rowGroup.ranges::isOverlapping;
        for (String column : round1Columns) {
          Chunk chunk = rowGroup.chunks.get(column);
          int[] pages = chunk.pages(selection, rowGroup.rowCount);
          rowGroup.round1Pages.put(column, pages);
          pagesPlanned += addRanges(pageRanges.computeIfAbsent(fileTask.location, k -> new ArrayList<>()), chunk, pages);
          byLocation.putIfAbsent(fileTask.location, fileTask);
        }
      }
    if (pageRanges.isEmpty())
      return;
    if (limited() && mergedBytes(pageRanges) > budget.remaining()) {
      //The row-group reader may reach the limit within the budget: it reads the remaining tasks in order
      rowGroupReader = true;
      for (FileTask fileTask : fileTasks) {
        fileTask.twoStep = false;
        fileTask.rowGroups.clear();
      }
      return;
    }
    stats.pagesRead += pagesPlanned;
    Map<String, List<Long>> fetched = fetch(pageRanges, byLocation);

    for (FileTask fileTask : fileTasks)
      for (RowGroupPlan rowGroup : fileTask.rowGroups) {
        stats.rowGroupsRead++;
        keepOverlappingRows(fileTask, rowGroup);
      }
    //The bbox pages are no longer needed
    fetched.forEach((location, offsets) -> byLocation.get(location).file.removeAll(offsets));
  }

  private void keepOverlappingRows(FileTask fileTask, RowGroupPlan rowGroup) throws IOException {
    Map<String, Values> values = new HashMap<>();
    for (String column : round1Columns)
      values.put(column, decode(fileTask, rowGroup, rowGroup.chunks.get(column), rowGroup.round1Pages.get(column)));
    Values xmin = values.get("bbox.xmin"), ymin = values.get("bbox.ymin"), xmax = values.get("bbox.xmax"),
        ymax = values.get("bbox.ymax");

    long[] kept = new long[(int) Math.min(rowGroup.ranges.rowCount(), Integer.MAX_VALUE)];
    int count = 0;
    PrimitiveIterator.OfLong rows = rowGroup.ranges.iterator();
    while (rows.hasNext()) {
      long row = rows.nextLong();
      checkDeadline();
      stats.rowsScanned++;
      stats.round1Rows++;
      int x0 = xmin.find(row), y0 = ymin.find(row), x1 = xmax.find(row), y1 = ymax.find(row);
      if (x0 < 0 || y0 < 0 || x1 < 0 || y1 < 0)
        continue;
      if (xmax.doubles[x1] < envelope.getMinX() || xmin.doubles[x0] > envelope.getMaxX() || ymax.doubles[y1] < envelope.getMinY()
          || ymin.doubles[y0] > envelope.getMaxY())
        continue;
      if (filters != null && !filtersMatch(values, row))
        continue;
      if (fileTask.deletes != null && fileTask.deletes.isDeleted(rowGroup.firstRow + row))
        continue;
      kept[count++] = row;
    }
    rowGroup.kept = Arrays.copyOf(kept, count);
  }

  private boolean filtersMatch(Map<String, Values> values, long row) {
    for (List<ColumnFilter> allOf : filters) {
      boolean all = true;
      for (ColumnFilter columnFilter : allOf) {
        Values column = values.get(columnFilter.column());
        int index = column.find(row);
        if (index < 0 || !columnFilter.values().contains(column.strings[index])) {
          all = false;
          break;
        }
      }
      if (all)
        return true;
    }
    return false;
  }

  /**
   * Reads the geometry and feature pages of the kept rows in batches, checks the geometries and adds the features, in task and row
   * order. The tasks which are read by the row-group reader are read at their place in that order.
   *
   * @return true if the limit was reached
   */
  private boolean round2(List<FileTask> fileTasks) throws IOException {
    int taskIndex = 0, rowGroupIndex = 0, rowIndex = 0;
    while (taskIndex < fileTasks.size()) {
      FileTask fileTask = fileTasks.get(taskIndex);
      if (!fileTask.twoStep) {
        if (scan.scanTask(fileTask.task))
          return true;
        taskIndex++;
        rowGroupIndex = rowIndex = 0;
        continue;
      }

      //A batch of kept rows: slices of the kept rows of consecutive row groups of consecutive two-step tasks
      long batchRows = batchRows(scan.missingFeatures());
      List<Slice> batch = new ArrayList<>();
      long rows = 0;
      while (taskIndex < fileTasks.size() && fileTasks.get(taskIndex).twoStep && rows < batchRows) {
        FileTask current = fileTasks.get(taskIndex);
        if (rowGroupIndex >= current.rowGroups.size()) {
          taskIndex++;
          rowGroupIndex = rowIndex = 0;
          continue;
        }
        RowGroupPlan rowGroup = current.rowGroups.get(rowGroupIndex);
        long remaining = batchRows - rows;
        int to = remaining >= rowGroup.kept.length - rowIndex ? rowGroup.kept.length : (int) (rowIndex + remaining);
        if (to > rowIndex) {
          batch.add(new Slice(current, rowGroup, Arrays.copyOfRange(rowGroup.kept, rowIndex, to)));
          rows += to - rowIndex;
        }
        if (to >= rowGroup.kept.length) {
          rowGroupIndex++;
          rowIndex = 0;
        }
        else
          rowIndex = to;
      }
      if (!batch.isEmpty() && readBatch(batch))
        return true;
    }
    return false;
  }

  private static long batchRows(long missingFeatures) {
    return missingFeatures > Integer.MAX_VALUE / 4 ? Long.MAX_VALUE : Math.max(MIN_BATCH_ROWS, 4 * missingFeatures);
  }

  /**
   * Fetches the geometry and feature pages of the rows of a batch, then checks and adds the features.
   *
   * @return true if the limit was reached
   */
  private boolean readBatch(List<Slice> batch) throws IOException {
    budget.checkDeadline();
    Map<String, List<long[]>> pageRanges = new LinkedHashMap<>();
    Map<String, FileTask> byLocation = new LinkedHashMap<>();
    for (Slice slice : batch) {
      RowSelection selection = slice::overlaps;
      for (String column : List.of(GEOMETRY, FEATURE_JSON)) {
        Chunk chunk = slice.rowGroup.chunks.get(column);
        int[] pages = chunk.pages(selection, slice.rowGroup.rowCount);
        slice.pages.put(column, pages);
        stats.pagesRead += addRanges(pageRanges.computeIfAbsent(slice.fileTask.location, k -> new ArrayList<>()), chunk, pages);
        byLocation.putIfAbsent(slice.fileTask.location, slice.fileTask);
      }
    }
    Map<String, List<Long>> fetched = fetch(pageRanges, byLocation);

    long cpu = cpuNanos();
    try {
      for (Slice slice : batch)
        if (readSlice(slice))
          return true;
      return false;
    }
    finally {
      stats.round2Nanos += cpuNanos() - cpu;
      fetched.forEach((location, offsets) -> byLocation.get(location).file.removeAll(offsets));
    }
  }

  /**
   * @return true if the limit was reached
   */
  private boolean readSlice(Slice slice) throws IOException {
    long[] rows = slice.rows;
    boolean[] intersects = new boolean[rows.length];
    int[] next = {0};
    decode(slice.fileTask, slice.rowGroup, slice.rowGroup.chunks.get(GEOMETRY), slice.pages.get(GEOMETRY), (row, reader) -> {
      checkDeadline();
      while (next[0] < rows.length && rows[next[0]] < row)
        next[0]++;
      if (next[0] < rows.length && rows[next[0]] == row && reader != null)
        intersects[next[0]] = intersects(reader.getBinary(), slice.rowGroup.firstRow + row, slice.fileTask);
      return next[0] >= rows.length;
    });

    int[] current = {0};
    boolean[] limitReached = {false};
    decode(slice.fileTask, slice.rowGroup, slice.rowGroup.chunks.get(FEATURE_JSON), slice.pages.get(FEATURE_JSON), (row, reader) -> {
      checkDeadline();
      while (current[0] < rows.length && rows[current[0]] < row)
        current[0]++;
      if (current[0] < rows.length && rows[current[0]] == row && intersects[current[0]] && reader != null
          && scan.addFeature(reader.getBinary().toStringUsingUTF8())) {
        limitReached[0] = true;
        return true;
      }
      return current[0] >= rows.length;
    });
    return limitReached[0];
  }

  private boolean intersects(Binary wkb, long position, FileTask fileTask) {
    try {
      return geometry.intersects(wkbReader.read(wkb.getBytes()));
    }
    catch (ParseException e) {
      throw new IllegalStateException("Invalid WKB geometry at row " + position + " of " + fileTask.location, e);
    }
  }

  /**
   * Fetches the page ranges of all files in parallel and adds them to the buffered files.
   *
   * @return The offsets of the fetched ranges by file location
   */
  private Map<String, List<Long>> fetch(Map<String, List<long[]>> pageRanges, Map<String, FileTask> byLocation) {
    List<String> locations = new ArrayList<>(pageRanges.keySet());
    List<FileRanges> requests = new ArrayList<>();
    for (String location : locations)
      requests.add(new FileRanges(byLocation.get(location).raw, pageRanges.get(location)));
    List<Map<Long, byte[]>> results = tables.rangeReader().fetch(requests, budget, stats);
    stats.rounds++;
    Map<String, List<Long>> offsets = new LinkedHashMap<>();
    for (int i = 0; i < locations.size(); i++) {
      byLocation.get(locations.get(i)).file.addAll(results.get(i));
      offsets.put(locations.get(i), new ArrayList<>(results.get(i).keySet()));
    }
    return offsets;
  }

  /**
   * Adds the byte ranges of the pages of a column chunk, and of its dictionary page if it has one.
   */
  /**
   * @return The number of pages
   */
  private static int addRanges(List<long[]> ranges, Chunk chunk, int[] pages) {
    if (pages.length == 0)
      return 0;
    long firstPage = chunk.offsetIndex.getOffset(0);
    if (chunk.meta.getStartingPos() < firstPage)
      ranges.add(new long[] {chunk.meta.getStartingPos(), firstPage - chunk.meta.getStartingPos()});
    for (int page : pages)
      ranges.add(new long[] {chunk.offsetIndex.getOffset(page), chunk.offsetIndex.getCompressedPageSize(page)});
    return pages.length;
  }

  /**
   * The bytes which {@link RangeReader#fetch} reads for the page ranges.
   */
  private static long mergedBytes(Map<String, List<long[]>> pageRanges) {
    long bytes = 0;
    for (List<long[]> ranges : pageRanges.values())
      for (long[] range : RangeReader.merge(ranges))
        bytes += range[1];
    return bytes;
  }

  /**
   * Decodes whole pages of one column and stores the values by row.
   */
  private Values decode(FileTask fileTask, RowGroupPlan rowGroup, Chunk chunk, int[] pages) throws IOException {
    Values values = new Values(chunk.column.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE);
    decode(fileTask, rowGroup, chunk, pages, (row, reader) -> {
      checkDeadline();
      values.add(row, reader);
      return false;
    });
    return values;
  }

  /**
   * Decodes whole pages of one column of a row group, one page at a time, and passes each value with its row to the visitor.
   * The row ranges of the read are the rows of the pages, so that the reader never skips rows inside a page.
   */
  private void decode(FileTask fileTask, RowGroupPlan rowGroup, Chunk chunk, int[] pages, ValueVisitor visitor) throws IOException {
    if (pages.length == 0)
      return;
    RowRanges aligned = RowRanges.create(rowGroup.rowCount, IntStream.of(pages).iterator(), chunk.offsetIndex);
    try (ParquetFileReader reader = fileTask.reader(List.of(chunk.column))) {
      PageReadStore store = reader.readFilteredRowGroup(rowGroup.index, aligned);
      if (store == null)
        return;
      MessageType requested = requestedSchema(fileTask.footer.getFileMetaData().getSchema(), List.of(chunk.column));
      ColumnReader column = new ColumnReadStoreImpl(store, converter(requested), requested,
          fileTask.footer.getFileMetaData().getCreatedBy()).getColumnReader(chunk.column);
      PrimitiveIterator.OfLong rows = store.getRowIndexes().orElseGet(() -> LongStream.range(0, store.getRowCount()).iterator());
      int maxDefinitionLevel = chunk.column.getMaxDefinitionLevel();
      for (long i = 0; i < store.getRowCount(); i++) {
        long row = rows.nextLong();
        boolean present = column.getCurrentDefinitionLevel() == maxDefinitionLevel;
        boolean stop = visitor.visit(row, present ? column : null);
        if (present)
          column.skip();
        if (stop)
          return;
        column.consume();
      }
    }
  }

  private void checkDeadline() {
    if (++checkCounter % CHECK_INTERVAL_VALUES == 0)
      budget.checkDeadline();
  }

  private static long cpuNanos() {
    return THREADS.isCurrentThreadCpuTimeSupported() ? THREADS.getCurrentThreadCpuTime() : System.nanoTime();
  }

  /**
   * The schema with only the given columns (and the groups which contain them), in the order of the file.
   */
  static MessageType requestedSchema(MessageType fileSchema, List<ColumnDescriptor> columns) {
    return new MessageType(fileSchema.getName(), select(fileSchema, columns.stream().map(ColumnDescriptor::getPath).toList(), 0));
  }

  private static List<Type> select(GroupType group, List<String[]> paths, int depth) {
    List<Type> fields = new ArrayList<>();
    for (Type field : group.getFields()) {
      List<String[]> below = paths.stream().filter(path -> path.length > depth && path[depth].equals(field.getName())).toList();
      if (below.isEmpty())
        continue;
      fields.add(field.isPrimitive() ? field : field.asGroupType().withNewFields(select(field.asGroupType(), below, depth + 1)));
    }
    return fields;
  }

  /**
   * A converter tree for the column readers, which are read directly and never write to their converter.
   */
  private static GroupConverter converter(GroupType type) {
    List<Converter> children = new ArrayList<>();
    for (Type field : type.getFields())
      children.add(field.isPrimitive() ? new PrimitiveConverter() {} : converter(field.asGroupType()));
    return new GroupConverter() {
      @Override
      public Converter getConverter(int fieldIndex) {
        return children.get(fieldIndex);
      }

      @Override
      public void start() {}

      @Override
      public void end() {}
    };
  }

  @FunctionalInterface
  private interface ValueVisitor {

    /**
     * @param row The row in the row group
     * @param reader The reader positioned at the value of the row, or null if the value is null
     * @return true to stop reading
     */
    boolean visit(long row, ColumnReader reader);
  }

  @FunctionalInterface
  private interface RowSelection {

    boolean overlaps(long fromRow, long toRow);
  }

  /**
   * A column chunk of a row group with its offset index.
   */
  private record Chunk(ColumnDescriptor column, ColumnChunkMetaData meta, OffsetIndex offsetIndex) {

    /**
     * The pages which contain selected rows.
     */
    int[] pages(RowSelection selection, long rowCount) {
      IntStream.Builder pages = IntStream.builder();
      for (int page = 0; page < offsetIndex.getPageCount(); page++)
        if (selection.overlaps(offsetIndex.getFirstRowIndex(page), offsetIndex.getLastRowIndex(page, rowCount)))
          pages.add(page);
      return pages.build().toArray();
    }
  }

  /**
   * The values of one column by row, in ascending row order.
   */
  private static final class Values {

    private final boolean numeric;
    private long[] rows = new long[16];
    private double[] doubles;
    private String[] strings;
    private int size;
    private int cursor;

    private Values(boolean numeric) {
      this.numeric = numeric;
      if (numeric)
        doubles = new double[16];
      else
        strings = new String[16];
    }

    private void add(long row, ColumnReader reader) {
      if (reader == null)
        return;
      if (size == rows.length) {
        rows = Arrays.copyOf(rows, size * 2);
        if (numeric)
          doubles = Arrays.copyOf(doubles, size * 2);
        else
          strings = Arrays.copyOf(strings, size * 2);
      }
      rows[size] = row;
      if (numeric)
        doubles[size] = reader.getDouble();
      else
        strings[size] = reader.getBinary().toStringUsingUTF8();
      size++;
    }

    /**
     * The index of the non-null value of a row, or -1. The rows must be looked up in ascending order.
     */
    private int find(long row) {
      while (cursor < size && rows[cursor] < row)
        cursor++;
      return cursor < size && rows[cursor] == row ? cursor : -1;
    }
  }

  private static final class RowGroupPlan {

    private final int index;
    private final long firstRow;
    private final long rowCount;
    private final RowRanges ranges;
    private final Map<String, Chunk> chunks;
    private final Map<String, int[]> round1Pages = new HashMap<>();
    private long[] kept = new long[0];

    private RowGroupPlan(int index, long firstRow, long rowCount, RowRanges ranges, Map<String, Chunk> chunks) {
      this.index = index;
      this.firstRow = firstRow;
      this.rowCount = rowCount;
      this.ranges = ranges;
      this.chunks = chunks;
    }
  }

  /**
   * Kept rows of a row group which are read in one batch.
   */
  private static final class Slice {

    private final FileTask fileTask;
    private final RowGroupPlan rowGroup;
    private final long[] rows;
    private final Map<String, int[]> pages = new HashMap<>();

    private Slice(FileTask fileTask, RowGroupPlan rowGroup, long[] rows) {
      this.fileTask = fileTask;
      this.rowGroup = rowGroup;
      this.rows = rows;
    }

    private boolean overlaps(long fromRow, long toRow) {
      int index = Arrays.binarySearch(rows, fromRow);
      if (index < 0)
        index = -index - 1;
      return index < rows.length && rows[index] <= toRow;
    }
  }

  private final class FileTask {

    private final FileScanTask task;
    private final String location;
    private final InputFile raw;
    private boolean twoStep;
    private ParquetMetadata footer;
    private PageIndex pageIndex;
    private PositionDeleteIndex deletes;
    private BufferedParquetFile file;
    /**
     * The Parquet columns by the names of the read columns
     */
    private final Map<String, ColumnDescriptor> columns = new LinkedHashMap<>();
    private final List<RowGroupPlan> rowGroups = new ArrayList<>();

    private FileTask(FileScanTask task) {
      this.task = task;
      location = task.file().location();
      FileIO io = files.table().io();
      raw = task.file().format() == FileFormat.PARQUET ? io.newInputFile(task.file()) : null;
      twoStep = raw != null && task.deletes().stream().noneMatch(delete -> delete.content() == FileContent.EQUALITY_DELETES);
    }

    /**
     * Loads the footer (through the footer cache), the page index (through its cache) and the position deletes.
     *
     * @return The bytes of the page index which were read
     */
    private long load() {
      footer = FeatureScan.footer(tables.dataFile(raw, budget));
      PageIndex cached = tables.cachedPageIndex(location);
      pageIndex = cached != null ? cached : tables.pageIndex(raw, footer, budget);
      if (!task.deletes().isEmpty()) {
        FileIO io = files.table().io();
        deletes = new BaseDeleteLoader((DeleteFile delete) -> new CountingInputFile(io.newInputFile(delete), budget))
            .loadPositionDeletes(task.deletes(), location);
      }
      file = new BufferedParquetFile(raw, tables.rangeReader(), budget, stats);
      byte[] tail = tables.cachedTail(location);
      if (tail != null)
        file.add(file.getLength() - tail.length, tail);
      if (pageIndex != null)
        file.add(pageIndex.offset(), pageIndex.bytes());
      return cached == null && pageIndex != null ? pageIndex.bytes().length : 0;
    }

    /**
     * Resolves the read columns in the file, by field ID, or by name for files without field IDs.
     *
     * @return false if a column is missing or has an unexpected type
     */
    private boolean resolveColumns() {
      Schema schema = files.schema();
      Map<Integer, ColumnDescriptor> byId = new HashMap<>();
      Map<String, ColumnDescriptor> byName = new HashMap<>();
      for (ColumnDescriptor column : footer.getFileMetaData().getSchema().getColumns()) {
        Type.ID id = column.getPrimitiveType().getId();
        if (id != null)
          byId.put(id.intValue(), column);
        byName.put(String.join(".", column.getPath()), column);
      }
      List<String> names = new ArrayList<>(round1Columns);
      names.add(GEOMETRY);
      names.add(FEATURE_JSON);
      for (String name : names) {
        NestedField field = schema.findField(name);
        ColumnDescriptor column = byId.isEmpty() ? byName.get(name) : field == null ? null : byId.get(field.fieldId());
        if (column == null || column.getMaxRepetitionLevel() != 0)
          return false;
        PrimitiveTypeName type = column.getPrimitiveType().getPrimitiveTypeName();
        if (type != (BBOX_COLUMNS.contains(name) ? PrimitiveTypeName.DOUBLE : PrimitiveTypeName.BINARY))
          return false;
        columns.put(name, column);
      }
      return true;
    }

    /**
     * The predicate of the column indexes: the bbox overlaps the envelope and the filters match.
     */
    private FilterPredicate predicate() {
      FilterPredicate predicate = and(
          and(gtEq(doubleColumn(path("bbox.xmax")), envelope.getMinX()), ltEq(doubleColumn(path("bbox.xmin")), envelope.getMaxX())),
          and(gtEq(doubleColumn(path("bbox.ymax")), envelope.getMinY()), ltEq(doubleColumn(path("bbox.ymin")), envelope.getMaxY())));
      if (filters == null)
        return predicate;
      FilterPredicate anyOf = null;
      for (List<ColumnFilter> allOf : filters) {
        FilterPredicate conjunction = null;
        for (ColumnFilter columnFilter : allOf) {
          Set<Binary> values = new LinkedHashSet<>();
          columnFilter.values().forEach(value -> values.add(Binary.fromString(value)));
          FilterPredicate valueIn = in(binaryColumn(path(columnFilter.column())), values);
          conjunction = conjunction == null ? valueIn : and(conjunction, valueIn);
        }
        anyOf = anyOf == null ? conjunction : or(anyOf, conjunction);
      }
      return and(predicate, anyOf);
    }

    private String path(String name) {
      return String.join(".", columns.get(name).getPath());
    }

    /**
     * A Parquet reader of the buffered file with the cached footer, reading only the given columns.
     */
    private ParquetFileReader reader(Collection<ColumnDescriptor> read) throws IOException {
      ParquetFileReader reader = new ParquetFileReader(file, footer, READ_OPTIONS, file.newStream());
      reader.setRequestedSchema(requestedSchema(footer.getFileMetaData().getSchema(), List.copyOf(read)));
      return reader;
    }
  }
}
