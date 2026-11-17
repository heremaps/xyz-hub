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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
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
 * then a JTS intersection with the stored WKB geometry, then the momType predicate. Deletion vectors and position deletes are applied.
 * <p>
 * A scan can start at a row position inside a file. It then reads the file from the row group which contains that position, so the
 * cost of a page does not grow with its position in the file.
 */
final class FeatureScan {

  static final String ID = "id";
  static final String MOM_TYPE = "mom_type";
  static final String BBOX = "bbox";
  static final String GEOMETRY = "geometry";
  static final String FEATURE_JSON = "feature_json";
  private static final List<String> PROJECTED_COLUMNS = List.of(ID, MOM_TYPE, "bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax",
      GEOMETRY, FEATURE_JSON);
  private static final String ROW_POSITION = MetadataColumns.ROW_POSITION.name();
  private static final int CHECK_INTERVAL_ROWS = 1024;

  private final IcebergTables tables;
  private final SnapshotFiles files;
  private Envelope envelope;
  private PreparedGeometry geometry;
  private List<List<Set<String>>> momTypes;
  private long limit = Long.MAX_VALUE;
  private Position start = new Position(0, 0);
  private ReadBudget budget;
  private long maxResponseBytes = Long.MAX_VALUE;
  private final Stats stats = new Stats();

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
   * Restricts the scan to features whose root momType matches: an OR of AND lists of value sets (IN).
   */
  FeatureScan withMomTypes(List<List<Set<String>>> momTypes) {
    this.momTypes = momTypes;
    return this;
  }

  FeatureScan withLimit(long limit) {
    this.limit = limit;
    return this;
  }

  FeatureScan withStart(Position start) {
    this.start = start;
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

  Stats stats() {
    return stats;
  }

  long bytesRead() {
    return budget.bytesRead();
  }

  Result run() {
    Schema schema = files.schema();
    Schema projection = TypeUtil.join(schema.select(PROJECTED_COLUMNS), new Schema(MetadataColumns.ROW_POSITION));
    Expression filter = filterExpression();
    InclusiveMetricsEvaluator metrics = filter == null ? null : new InclusiveMetricsEvaluator(schema, filter);
    WKBReader wkbReader = new WKBReader();
    List<FileScanTask> tasks = files.tasks();
    stats.filesPlanned = tasks.size();

    List<String> features = new ArrayList<>();
    long responseBytes = 0;
    for (int fileIndex = start.fileIndex(); fileIndex < tasks.size(); fileIndex++) {
      budget.checkDeadline();
      FileScanTask task = tasks.get(fileIndex);
      if (metrics != null && !metrics.eval(task.file())) {
        stats.filesPruned++;
        continue;
      }
      long startPosition = fileIndex == start.fileIndex() ? start.rowPosition() : 0;
      stats.filesRead++;
      try (CloseableIterable<Record> records = open(task, projection, filter, startPosition)) {
        for (Record record : records) {
          if (++stats.rowsScanned % CHECK_INTERVAL_ROWS == 0)
            budget.checkDeadline();
          long position = (Long) record.getField(ROW_POSITION);
          if (position < startPosition || !matches(record, wkbReader))
            continue;
          Object json = record.getField(FEATURE_JSON);
          if (json == null)
            continue;
          String feature = json.toString();
          responseBytes += feature.length();
          if (responseBytes > maxResponseBytes)
            throw new ScanException(PAYLOAD_TO_LARGE, "The response would be larger than " + maxResponseBytes + " bytes");
          features.add(feature);
          stats.rowsMatched++;
          if (features.size() >= limit)
            return new Result(features, new Position(fileIndex, position + 1));
        }
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return new Result(features, null);
  }

  private Expression filterExpression() {
    Expression filter = null;
    if (envelope != null)
      filter = Expressions.and(
          Expressions.and(Expressions.greaterThanOrEqual("bbox.xmax", envelope.getMinX()),
              Expressions.lessThanOrEqual("bbox.xmin", envelope.getMaxX())),
          Expressions.and(Expressions.greaterThanOrEqual("bbox.ymax", envelope.getMinY()),
              Expressions.lessThanOrEqual("bbox.ymin", envelope.getMaxY())));
    if (momTypes != null) {
      Expression anyOf = Expressions.alwaysFalse();
      for (List<Set<String>> allOf : momTypes) {
        Expression conjunction = Expressions.alwaysTrue();
        for (Set<String> values : allOf)
          conjunction = Expressions.and(conjunction, Expressions.in(MOM_TYPE, values));
        anyOf = Expressions.or(anyOf, conjunction);
      }
      filter = filter == null ? anyOf : Expressions.and(filter, anyOf);
    }
    return filter;
  }

  /**
   * Opens one task like Iceberg's (package-private) {@code org.apache.iceberg.data.GenericReader#open(FileScanTask)}, but with
   * byte counting, the footer cache and the row group filter.
   *
   * @param startPosition The row position in the file to start at. Row groups before the one containing it are not read.
   */
  private CloseableIterable<Record> open(FileScanTask task, Schema projection, Expression filter, long startPosition)
      throws IOException {
    if (task.file().format() != FileFormat.PARQUET)
      throw new ScanException(NOT_IMPLEMENTED, "Reading " + task.file().format() + " files is not supported");
    FileIO io = files.table().io();
    InputFile dataFile = tables.dataFile(io.newInputFile(task.file()), budget);
    long splitStart = task.start();
    long splitEnd = task.start() + task.length();
    if (startPosition > 0) {
      long rowGroupStart = rowGroupStart(dataFile, startPosition);
      if (rowGroupStart < 0)
        return CloseableIterable.empty();
      splitStart = Math.max(splitStart, rowGroupStart);
      if (splitStart >= splitEnd)
        return CloseableIterable.empty();
    }
    DeleteFilter<Record> deletes = new CountingDeleteFilter(io, task, files.schema(), projection, budget);
    Schema readSchema = deletes.requiredSchema();
    Map<Integer, ?> partition = PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);
    //Parquet reads the row groups whose midpoint is inside the split. The starting offset of a row group selects it and all later ones.
    Parquet.ReadBuilder builder = Parquet.read(dataFile)
        .project(readSchema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema, partition))
        .split(splitStart, splitEnd - splitStart);
    if (filter != null)
      builder.filter(filter);
    return deletes.filter(builder.build());
  }

  /**
   * @return The starting offset of the row group which contains the row position, or -1 if the file has fewer rows
   */
  private static long rowGroupStart(InputFile file, long position) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInputFile(file))) {
      long firstRow = 0;
      for (BlockMetaData rowGroup : reader.getFooter().getBlocks()) {
        if (position < firstRow + rowGroup.getRowCount())
          return rowGroup.getStartingPos();
        firstRow += rowGroup.getRowCount();
      }
      return -1;
    }
  }

  private boolean matches(Record record, WKBReader wkbReader) {
    if (geometry != null && !intersects(record, wkbReader))
      return false;
    return momTypes == null || momTypeMatches((CharSequence) record.getField(MOM_TYPE));
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

  private boolean momTypeMatches(CharSequence value) {
    if (value == null)
      return false;
    String momType = value.toString();
    for (List<Set<String>> allOf : momTypes) {
      boolean all = true;
      for (Set<String> values : allOf)
        if (!values.contains(momType)) {
          all = false;
          break;
        }
      if (all)
        return true;
    }
    return false;
  }

  /**
   * A position in the scan: the index of the file in the sorted file list and the position of the row in that file to continue at.
   */
  record Position(int fileIndex, long rowPosition) {}

  /**
   * @param features The stored feature JSON strings
   * @param next The position to continue at, or null if the scan is complete
   */
  record Result(List<String> features, Position next) {}

  static final class Stats {
    int filesPlanned;
    int filesPruned;
    int filesRead;
    long rowsScanned;
    long rowsMatched;
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
