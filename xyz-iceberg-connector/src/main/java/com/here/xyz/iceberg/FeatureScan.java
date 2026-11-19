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

import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;
import static com.here.xyz.responses.XyzError.PAYLOAD_TO_LARGE;

import com.here.xyz.iceberg.IcebergTables.SnapshotFiles;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.Type;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * Scans the data files of a snapshot and collects the stored feature JSON of the matching rows.
 * Files are pruned by their column statistics, row groups by Parquet's statistics, and rows are filtered exactly: bbox overlap,
 * then a JTS intersection with the stored WKB geometry, then the property filters. Deletion vectors and position deletes are applied.
 * <p>
 * A paged scan (iterate) reads one row group at a time and starts at the row group and row offset of its {@link Position}, so the
 * cost of a page does not grow with its position in the file. Before it reads a row group, it compares the compressed size of the
 * projected column chunks (from the cached footer) with the rest of the request's byte budget. If the row group does not fit, the
 * page ends at the row group boundary. Only a page without features yet reads a row group that does not fit, so that every page
 * makes progress.
 * <p>
 * A spatial read which is not paged uses the two-step page-index reader {@link TwoStepScan} if {@link #withTwoStepReads(boolean)}
 * is set. It returns the same features in the same order, but reads only the pages of the matching rows.
 */
final class FeatureScan {

  static final String ID = "id";
  static final String BBOX = "bbox";
  static final String GEOMETRY = "geometry";
  static final String FEATURE_JSON = "feature_json";
  private static final List<String> PROJECTED_COLUMNS = List.of(ID, "bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax", GEOMETRY,
      FEATURE_JSON);
  private static final String ROW_POSITION = MetadataColumns.ROW_POSITION.name();
  private static final int CHECK_INTERVAL_ROWS = 1024;

  private final IcebergTables tables;
  private final SnapshotFiles files;
  private Envelope envelope;
  private PreparedGeometry geometry;
  private List<List<ColumnFilter>> filters;
  private long limit = Long.MAX_VALUE;
  private Position start = Position.FIRST;
  private boolean paged;
  private ReadBudget budget;
  private long maxResponseBytes = Long.MAX_VALUE;
  private boolean twoStepReads;
  private Schema projection;
  private Expression filter;
  private WKBReader wkbReader;
  private final Stats stats = new Stats();
  private final List<String> features = new ArrayList<>();
  private long responseBytes;

  FeatureScan(IcebergTables tables, SnapshotFiles files) {
    this.tables = tables;
    this.files = files;
  }

  /**
   * Restricts the scan to features whose geometry intersects the query geometry. Features without geometry are skipped.
   */
  FeatureScan withGeometry(Geometry queryGeometry) {
    envelope = queryGeometry.getEnvelopeInternal();
    geometry = PreparedGeometryFactory.prepare(queryGeometry);
    return this;
  }

  /**
   * Restricts the scan to rows whose filter columns match: an OR of AND lists of column value sets (IN).
   */
  FeatureScan withFilters(List<List<ColumnFilter>> filters) {
    this.filters = filters;
    return this;
  }

  FeatureScan withLimit(long limit) {
    this.limit = limit;
    return this;
  }

  /**
   * Makes this a paged scan which reads row group by row group, starting at the position.
   */
  FeatureScan withPaging(Position start) {
    this.start = start;
    paged = true;
    return this;
  }

  FeatureScan withBudget(ReadBudget budget) {
    this.budget = budget;
    return this;
  }

  FeatureScan withMaxResponseBytes(long maxResponseBytes) {
    this.maxResponseBytes = maxResponseBytes;
    return this;
  }

  /**
   * Lets a spatial read which is not paged use the two-step page-index reader.
   */
  FeatureScan withTwoStepReads(boolean twoStepReads) {
    this.twoStepReads = twoStepReads;
    return this;
  }

  Stats stats() {
    return stats;
  }

  long bytesRead() {
    return budget.bytesRead();
  }

  Result run() {
    Schema schema = files.schema();
    List<String> columns = projectedColumns();
    projection = TypeUtil.join(schema.select(columns), new Schema(MetadataColumns.ROW_POSITION));
    filter = filterExpression();
    InclusiveMetricsEvaluator metrics = filter == null ? null : new InclusiveMetricsEvaluator(schema, filter);
    ProjectedColumns projected = projectedColumns(schema, columns);
    wkbReader = new WKBReader();
    List<FileScanTask> tasks = files.tasks();
    stats.filesPlanned = tasks.size();
    boolean twoStep = twoStepReads && geometry != null && !paged;
    stats.reader = twoStep ? "TS" : "RG";
    List<FileScanTask> twoStepTasks = new ArrayList<>();

    try {
      for (int fileIndex = start.fileIndex(); fileIndex < tasks.size(); fileIndex++) {
        budget.checkDeadline();
        FileScanTask task = tasks.get(fileIndex);
        if (metrics != null && !metrics.eval(task.file())) {
          stats.filesPruned++;
          continue;
        }
        stats.filesRead++;
        if (twoStep)
          twoStepTasks.add(task);
        else if (paged) {
          Position next = scanRowGroups(fileIndex, task, projection, filter, projected, wkbReader);
          if (next != null)
            return new Result(features, next);
        }
        else if (scanFile(task, projection, filter, wkbReader))
          return new Result(features, null);
      }
      if (twoStep)
        new TwoStepScan(this, tables, files, envelope, geometry, filters, budget, stats).run(twoStepTasks);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new Result(features, null);
  }

  /**
   * The largest compressed size of the projected column chunks of a row group in the data files of the snapshot. The footers are
   * read in parallel (through the footer cache) and the result is cached per snapshot.
   *
   * @param deadlineMillis The time after which the footers are no longer awaited (TIMEOUT)
   */
  long maxRowGroupBytes(long deadlineMillis) {
    List<String> columns = projectedColumns();
    ProjectedColumns projected = projectedColumns(files.schema(), columns);
    //Footer reads for this check do not count against the budget of the page
    ReadBudget footerBudget = new ReadBudget(Long.MAX_VALUE, deadlineMillis);
    return tables.maxRowGroupBytes(files, Set.copyOf(columns), task -> {
      long max = 0;
      for (RowGroup rowGroup : rowGroups(footer(dataFile(task, footerBudget)), projected))
        max = Math.max(max, rowGroup.projectedBytes());
      return max;
    }, deadlineMillis);
  }

  /**
   * Reads a whole file task with the row-group reader, for the tasks which {@link TwoStepScan} cannot read.
   *
   * @return true if the limit was reached
   */
  boolean scanTask(FileScanTask task) throws IOException {
    return scanFile(task, projection, filter, wkbReader);
  }

  /**
   * @return The number of features which are still missing to reach the limit
   */
  long missingFeatures() {
    return limit - features.size();
  }

  private List<String> projectedColumns() {
    List<String> columns = new ArrayList<>(PROJECTED_COLUMNS);
    if (filters != null)
      filters.forEach(allOf -> allOf.forEach(filter -> columns.add(filter.column())));
    return List.copyOf(new LinkedHashSet<>(columns));
  }

  /**
   * Reads a whole file task.
   *
   * @return true if the limit was reached
   */
  private boolean scanFile(FileScanTask task, Schema projection, Expression filter, WKBReader wkbReader) throws IOException {
    DeleteFilter<Record> deletes = new CountingDeleteFilter(files.table().io(), task, files.schema(), projection, budget);
    try (CloseableIterable<Record> records = open(task, dataFile(task, budget), deletes, filter, task.start(), task.length())) {
      for (Record record : records) {
        countRow();
        if (matches(record, wkbReader) && add(record))
          return true;
      }
    }
    return false;
  }

  /**
   * Reads the row groups of a file task one by one, starting at the scan's start position if the file is the start file.
   *
   * @return The position to continue at if the page ends in this file, or null to continue with the next file
   */
  private Position scanRowGroups(int fileIndex, FileScanTask task, Schema projection, Expression filter, ProjectedColumns projected,
      WKBReader wkbReader) throws IOException {
    InputFile dataFile = dataFile(task, budget);
    boolean startFile = fileIndex == start.fileIndex();
    long splitStart = task.start(), splitEnd = task.start() + task.length();
    DeleteFilter<Record> deletes = null;
    for (RowGroup rowGroup : rowGroups(footer(dataFile), projected)) {
      if (startFile && rowGroup.index() < start.rowGroup())
        continue;
      //Like Parquet's split filter: a row group belongs to the split which contains its midpoint
      long midpoint = rowGroup.start() + rowGroup.length() / 2;
      if (midpoint < splitStart || midpoint >= splitEnd)
        continue;
      long offset = startFile && rowGroup.index() == start.rowGroup() ? start.rowOffset() : 0;
      if (offset >= rowGroup.rowCount())
        continue;
      budget.checkDeadline();

      //The delete files of the file are read with its first row group
      long bytes = rowGroup.projectedBytes() + (deletes == null ? deleteBytes(task) : 0);
      if (bytes > budget.remaining()) {
        if (!features.isEmpty())
          return new Position(fileIndex, rowGroup.index(), offset);
        //An empty page reads one row group even if it does not fit, so that every page makes progress
        budget.allow(bytes);
      }
      if (deletes == null)
        deletes = new CountingDeleteFilter(files.table().io(), task, files.schema(), projection, budget);
      stats.rowGroupsRead++;
      try (CloseableIterable<Record> records = open(task, dataFile, deletes, filter, rowGroup.start(), rowGroup.length())) {
        for (Record record : records) {
          countRow();
          long row = (Long) record.getField(ROW_POSITION) - rowGroup.firstRow();
          if (row < offset || !matches(record, wkbReader))
            continue;
          if (add(record))
            return row + 1 < rowGroup.rowCount() ? new Position(fileIndex, rowGroup.index(), row + 1)
                : new Position(fileIndex, rowGroup.index() + 1, 0);
        }
      }
    }
    return null;
  }

  private void countRow() {
    if (++stats.rowsScanned % CHECK_INTERVAL_ROWS == 0)
      budget.checkDeadline();
  }

  /**
   * Adds the feature of a matching row to the response.
   *
   * @return true if the limit was reached
   */
  private boolean add(Record record) {
    Object json = record.getField(FEATURE_JSON);
    return json != null && addFeature(json.toString());
  }

  /**
   * Adds the feature JSON of a matching row to the response.
   *
   * @return true if the limit was reached
   * @throws ScanException with PAYLOAD_TO_LARGE if the response would be larger than the response limit
   */
  boolean addFeature(String feature) {
    responseBytes += feature.length();
    if (responseBytes > maxResponseBytes)
      throw new ScanException(PAYLOAD_TO_LARGE, "The response would be larger than " + maxResponseBytes + " bytes");
    features.add(feature);
    stats.rowsMatched++;
    return features.size() >= limit;
  }

  /**
   * The bytes of the delete files of a task; for a deletion vector only its blob.
   */
  private static long deleteBytes(FileScanTask task) {
    long bytes = 0;
    for (DeleteFile delete : task.deletes())
      bytes += delete.contentSizeInBytes() != null ? delete.contentSizeInBytes() : delete.fileSizeInBytes();
    return bytes;
  }

  private Expression filterExpression() {
    Expression filter = null;
    if (envelope != null)
      filter = Expressions.and(
          Expressions.and(Expressions.greaterThanOrEqual("bbox.xmax", envelope.getMinX()),
              Expressions.lessThanOrEqual("bbox.xmin", envelope.getMaxX())),
          Expressions.and(Expressions.greaterThanOrEqual("bbox.ymax", envelope.getMinY()),
              Expressions.lessThanOrEqual("bbox.ymin", envelope.getMaxY())));
    if (filters != null) {
      Expression anyOf = Expressions.alwaysFalse();
      for (List<ColumnFilter> allOf : filters) {
        Expression conjunction = Expressions.alwaysTrue();
        for (ColumnFilter columnFilter : allOf)
          conjunction = Expressions.and(conjunction, Expressions.in(columnFilter.column(), columnFilter.values()));
        anyOf = Expressions.or(anyOf, conjunction);
      }
      filter = filter == null ? anyOf : Expressions.and(filter, anyOf);
    }
    return filter;
  }

  /**
   * Opens a data file so that the bytes read count against the budget and the Parquet footer comes from the footer cache.
   */
  private InputFile dataFile(FileScanTask task, ReadBudget readBudget) {
    if (task.file().format() != FileFormat.PARQUET)
      throw new ScanException(NOT_IMPLEMENTED, "Reading " + task.file().format() + " files is not supported");
    return tables.dataFile(files.table().io().newInputFile(task.file()), readBudget);
  }

  /**
   * Opens a range of one task like Iceberg's (package-private) {@code org.apache.iceberg.data.GenericReader#open(FileScanTask)},
   * but with byte counting, the footer cache and the row group filter.
   * Parquet reads the row groups whose midpoint is inside the split, so the starting offset and the compressed size of a row group
   * select exactly that row group.
   */
  private static CloseableIterable<Record> open(FileScanTask task, InputFile dataFile, DeleteFilter<Record> deletes,
      Expression filter, long splitStart, long splitLength) {
    Schema readSchema = deletes.requiredSchema();
    Map<Integer, ?> partition = PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);
    Parquet.ReadBuilder builder = Parquet.read(dataFile)
        .project(readSchema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema, partition))
        .split(splitStart, splitLength);
    if (filter != null)
      builder.filter(filter);
    return deletes.filter(builder.build());
  }

  static ParquetMetadata footer(InputFile file) {
    try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInputFile(file))) {
      return reader.getFooter();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The Parquet columns of the projection: by field ID, and by name for files without field IDs.
   */
  private static ProjectedColumns projectedColumns(Schema schema, List<String> columns) {
    Set<Integer> fieldIds = new LinkedHashSet<>();
    for (String column : columns)
      if (schema.findField(column) != null)
        fieldIds.add(schema.findField(column).fieldId());
    return new ProjectedColumns(Set.copyOf(fieldIds), Set.copyOf(columns));
  }

  /**
   * The row groups of a file, with the compressed size of their projected column chunks.
   */
  static List<RowGroup> rowGroups(ParquetMetadata footer, ProjectedColumns projected) {
    Map<ColumnPath, Integer> fieldIds = new HashMap<>();
    for (ColumnDescriptor column : footer.getFileMetaData().getSchema().getColumns()) {
      Type.ID id = column.getPrimitiveType().getId();
      if (id != null)
        fieldIds.put(ColumnPath.get(column.getPath()), id.intValue());
    }
    List<RowGroup> rowGroups = new ArrayList<>();
    long firstRow = 0;
    List<BlockMetaData> blocks = footer.getBlocks();
    for (int i = 0; i < blocks.size(); i++) {
      BlockMetaData block = blocks.get(i);
      long projectedBytes = 0;
      for (ColumnChunkMetaData chunk : block.getColumns()) {
        Integer id = fieldIds.get(chunk.getPath());
        if (id != null ? projected.fieldIds().contains(id) : projected.names().contains(chunk.getPath().toDotString()))
          projectedBytes += chunk.getTotalSize();
      }
      rowGroups.add(new RowGroup(i, firstRow, block.getRowCount(), block.getStartingPos(), block.getCompressedSize(), projectedBytes));
      firstRow += block.getRowCount();
    }
    return rowGroups;
  }

  private boolean matches(Record record, WKBReader wkbReader) {
    if (geometry != null && !intersects(record, wkbReader))
      return false;
    return filters == null || filtersMatch(record);
  }

  private boolean intersects(Record record, WKBReader wkbReader) {
    Record bbox = (Record) record.getField(BBOX);
    ByteBuffer wkb = (ByteBuffer) record.getField(GEOMETRY);
    if (bbox == null || wkb == null)
      return false;
    Double xmin = (Double) bbox.getField("xmin"), ymin = (Double) bbox.getField("ymin"), xmax = (Double) bbox.getField("xmax"),
        ymax = (Double) bbox.getField("ymax");
    if (xmin == null || ymin == null || xmax == null || ymax == null)
      return false;
    if (xmax < envelope.getMinX() || xmin > envelope.getMaxX() || ymax < envelope.getMinY() || ymin > envelope.getMaxY())
      return false;
    try {
      return geometry.intersects(wkbReader.read(ByteBuffers.toByteArray(wkb)));
    }
    catch (ParseException e) {
      throw new IllegalStateException("Invalid WKB geometry of feature " + record.getField(ID), e);
    }
  }

  private boolean filtersMatch(Record record) {
    for (List<ColumnFilter> allOf : filters) {
      boolean all = true;
      for (ColumnFilter columnFilter : allOf) {
        Object value = record.getField(columnFilter.column());
        if (value == null || !columnFilter.values().contains(value.toString())) {
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
   * A position in a paged scan: the index of the file in the sorted file list, the index of the row group in that file and the
   * offset of the row in that row group to continue at.
   */
  record Position(int fileIndex, int rowGroup, long rowOffset) {

    static final Position FIRST = new Position(0, 0, 0);
  }

  /**
   * A row group of a data file.
   *
   * @param index The index of the row group in the file
   * @param firstRow The position in the file of the first row of the row group
   * @param start The starting offset of the row group in the file
   * @param length The compressed size of all column chunks of the row group
   * @param projectedBytes The compressed size of the projected column chunks, which a read of the row group reads
   */
  record RowGroup(int index, long firstRow, long rowCount, long start, long length, long projectedBytes) {}

  /**
   * The projected columns by Iceberg field ID, and by name for files without field IDs.
   */
  record ProjectedColumns(Set<Integer> fieldIds, Set<String> names) {}

  /**
   * Matches rows whose top-level string column has one of the values.
   */
  record ColumnFilter(String column, Set<String> values) {}

  /**
   * @param features The stored feature JSON strings
   * @param next The position to continue at, or null if the scan is complete
   */
  record Result(List<String> features, Position next) {}

  static final class Stats {
    int filesPlanned;
    int filesPruned;
    int filesRead;
    int rowGroupsRead;
    long rowsScanned;
    long rowsMatched;
    /**
     * RG (the row-group reader), TS (the two-step reader) or TS+RG (the two-step reader, some files with the row-group reader)
     */
    String reader;
    /**
     * The rounds of parallel range reads of the two-step reader
     */
    int rounds;
    long rangeReads;
    /**
     * The reads of the two-step reader which its planning did not foresee
     */
    long unplannedReads;
    long pagesRead;
    /**
     * The rows whose bbox the first step of the two-step reader decoded
     */
    long round1Rows;
    /**
     * The thread CPU time of the planning and decoding of the first step, and of the decoding of the second step
     */
    long round1Nanos;
    long round2Nanos;
    /**
     * The bytes of page indexes which were read (not cached)
     */
    long pageIndexBytes;
  }

  /**
   * Reads an Iceberg input file with Parquet's file reader, like Iceberg's (package-private) {@code ParquetIO.file(InputFile)}.
   */
  private record ParquetInputFile(InputFile file) implements org.apache.parquet.io.InputFile {

    @Override
    public long getLength() {
      return file.getLength();
    }

    @Override
    public org.apache.parquet.io.SeekableInputStream newStream() {
      org.apache.iceberg.io.SeekableInputStream stream = file.newStream();
      return new DelegatingSeekableInputStream(stream) {
        @Override
        public long getPos() throws IOException {
          return stream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
          stream.seek(newPos);
        }
      };
    }
  }

  /**
   * Iceberg's delete filter for generic records, reading the delete files through the byte counter.
   */
  private static final class CountingDeleteFilter extends GenericDeleteFilter {

    private final FileIO io;
    private final ReadBudget budget;

    private CountingDeleteFilter(FileIO io, FileScanTask task, Schema tableSchema, Schema requestedSchema, ReadBudget budget) {
      super(io, task, tableSchema, requestedSchema);
      this.io = io;
      this.budget = budget;
    }

    @Override
    protected InputFile getInputFile(String location) {
      return new CountingInputFile(io.newInputFile(location), budget);
    }

    @Override
    protected InputFile loadInputFile(DeleteFile deleteFile) {
      return new CountingInputFile(io.newInputFile(deleteFile), budget);
    }
  }
}
