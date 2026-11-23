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

import static java.util.Comparator.naturalOrder;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.HQuad.MortonUtlis;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

/**
 * Writes Iceberg tables in the schema of exported space versions, for tests.
 * Columns are computed from the feature JSON: the id, the root momType, version / createdAt / updatedAt / author from
 * {@code properties.@ns:com:here:xyz}, the bbox of the geometry, the level 12 HERE tile of the bbox centre, a size class and the WKB
 * geometry. Data files are Parquet (zstd), partitioned by {@code truncate(4096, tile_id)}, and deletes are written as deletion
 * vectors (format version 3, merge-on-read).
 * <p>
 * {@link #createTable(Catalog, TableIdentifier, Layout, Map)} creates a table in one of the two {@link Layout}s, with the writer
 * settings for large values.
 */
public final class IcebergTestTables {

  public static final Schema SCHEMA = new Schema(
      required(1, "id", Types.StringType.get()),
      optional(2, "mom_type", Types.StringType.get()),
      optional(3, "iml_version", Types.LongType.get()),
      optional(4, "created_at", Types.TimestampType.withZone()),
      optional(5, "updated_at", Types.TimestampType.withZone()),
      optional(6, "author", Types.StringType.get()),
      optional(7, "bbox", Types.StructType.of(
          optional(8, "xmin", Types.DoubleType.get()),
          optional(9, "ymin", Types.DoubleType.get()),
          optional(10, "xmax", Types.DoubleType.get()),
          optional(11, "ymax", Types.DoubleType.get()))),
      optional(12, "tile_id", Types.LongType.get()),
      optional(13, "size_class", Types.IntegerType.get()),
      optional(14, "geometry", Types.BinaryType.get()),
      optional(15, "feature_json", Types.StringType.get()));
  /**
   * The schema of the cell layout: {@link #SCHEMA} plus the level 24 HERE tile of the bbox centre
   */
  public static final Schema CELL_SCHEMA = TypeUtil.join(SCHEMA, new Schema(optional(16, "cell_id", Types.LongType.get())));
  public static final int TILE_LEVEL = 12;
  public static final int CELL_LEVEL = 24;
  private static final double TILE_SIZE = 360.0 / (1 << TILE_LEVEL);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String XYZ_NAMESPACE = "@ns:com:here:xyz";

  private IcebergTestTables() {}

  /**
   * Creates the table (and its namespace, if the catalog supports namespaces).
   */
  public static Table createTable(Catalog catalog, TableIdentifier identifier) {
    if (catalog instanceof SupportsNamespaces namespaces && !identifier.namespace().isEmpty()
        && !namespaces.namespaceExists(identifier.namespace()))
      namespaces.createNamespace(identifier.namespace());
    return catalog.buildTable(identifier, SCHEMA)
        .withPartitionSpec(PartitionSpec.builderFor(SCHEMA).truncate("tile_id", 4096).build())
        .withSortOrder(SortOrder.builderFor(SCHEMA).asc("size_class").asc("tile_id").asc("id").build())
        .withProperties(Map.of(
            "format-version", "3",
            "write.delete.mode", "merge-on-read",
            "write.update.mode", "merge-on-read",
            "write.merge.mode", "merge-on-read",
            "write.parquet.compression-codec", "zstd"))
        .create();
  }

  /**
   * The table layouts of the exported space versions.
   */
  public enum Layout {
    /**
     * Sorted by size class, tile and id, with dictionary encoding
     */
    CURRENT,
    /**
     * With the column {@code cell_id}, sorted by size class, tile, cell and id, without dictionary encoding
     */
    CELL
  }

  /**
   * Creates the table in the layout, with the writer settings for large values (no statistics and no column metrics of
   * {@code geometry} and {@code feature_json}).
   *
   * @param properties Table properties which override the defaults, e.g. {@code write.parquet.page-row-limit} or
   *     {@code format-version}
   */
  public static Table createTable(Catalog catalog, TableIdentifier identifier, Layout layout, Map<String, String> properties) {
    if (catalog instanceof SupportsNamespaces namespaces && !identifier.namespace().isEmpty()
        && !namespaces.namespaceExists(identifier.namespace()))
      namespaces.createNamespace(identifier.namespace());
    Schema schema = layout == Layout.CELL ? CELL_SCHEMA : SCHEMA;
    SortOrder.Builder sortOrder = SortOrder.builderFor(schema).asc("size_class").asc("tile_id");
    if (layout == Layout.CELL)
      sortOrder.asc("cell_id");
    Map<String, String> tableProperties = new HashMap<>(Map.of(
        "format-version", "3",
        "write.delete.mode", "merge-on-read",
        "write.update.mode", "merge-on-read",
        "write.merge.mode", "merge-on-read",
        "write.parquet.compression-codec", "zstd",
        "write.parquet.stats-enabled.column.feature_json", "false",
        "write.parquet.stats-enabled.column.geometry", "false",
        "write.metadata.metrics.column.feature_json", "none",
        "write.metadata.metrics.column.geometry", "none"));
    if (layout == Layout.CELL)
      tableProperties.put("parquet.enable.dictionary", "false");
    tableProperties.putAll(properties);
    return catalog.buildTable(identifier, schema)
        .withPartitionSpec(PartitionSpec.builderFor(schema).truncate("tile_id", 4096).build())
        .withSortOrder(sortOrder.asc("id").build())
        .withProperties(tableProperties)
        .create();
  }

  /**
   * Appends the features to the branch (null means main) as new data files, one per partition.
   */
  public static void append(Table table, String branch, List<String> featureJsons) {
    table.refresh();
    List<DataFile> dataFiles = writeDataFiles(table, records(table.schema(), featureJsons));
    AppendFiles append = table.newAppend();
    dataFiles.forEach(append::appendFile);
    append.toBranch(branchName(branch));
    append.commit();
  }

  /**
   * Replaces the features with the ids of the upserts, deletes the features with the deleteIds and adds the upserts, in one
   * RowDelta commit: a deletion vector for the replaced and deleted rows plus new data files.
   */
  public static void upsertAndDelete(Table table, String branch, List<String> upserts, List<String> deleteIds) {
    table.refresh();
    String target = branchName(branch);
    Schema schema = table.schema();
    List<Record> upsertRecords = records(schema, upserts);
    Set<String> removedIds = new HashSet<>(deleteIds);
    upsertRecords.forEach(record -> removedIds.add((String) record.getField("id")));

    Map<String, FileScanTask> tasksByPath = new HashMap<>();
    Snapshot current = table.snapshot(target);
    if (current != null)
      try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(current.snapshotId()).planFiles()) {
        tasks.forEach(task -> tasksByPath.put(task.file().location(), task));
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }

    OutputFileFactory deleteFiles = OutputFileFactory.builderFor(table, 1, System.nanoTime()).format(FileFormat.PUFFIN).build();
    BaseDVFileWriter dvWriter = new BaseDVFileWriter(deleteFiles, path -> previousDeletes(table, tasksByPath.get(path)));
    try (dvWriter) {
      for (FileScanTask task : tasksByPath.values())
        forEachLiveRow(table, task, (id, pos) -> {
          if (removedIds.contains(id))
            dvWriter.delete(task.file().location(), pos, task.spec(), task.file().partition());
        });
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    DeleteWriteResult deletes = dvWriter.result();

    RowDelta delta = table.newRowDelta();
    writeDataFiles(table, upsertRecords).forEach(delta::addRows);
    deletes.deleteFiles().forEach(delta::addDeletes);
    deletes.rewrittenDeleteFiles().forEach(delta::removeDeletes);
    delta.toBranch(target);
    delta.commit();
  }

  /**
   * Tags the current snapshot of main.
   */
  public static void tag(Table table, String name) {
    tag(table, name, null);
  }

  /**
   * Tags the current snapshot of the branch (null means main).
   */
  public static void tag(Table table, String name, String branch) {
    table.refresh();
    table.manageSnapshots().createTag(name, table.snapshot(branchName(branch)).snapshotId()).commit();
  }

  /**
   * Creates a branch at the snapshot of a tag.
   */
  public static void branchFromTag(Table table, String branch, String tag) {
    table.refresh();
    table.manageSnapshots().createBranch(branch, table.refs().get(tag).snapshotId()).commit();
  }

  private static String branchName(String branch) {
    return branch == null ? SnapshotRef.MAIN_BRANCH : branch;
  }

  private static PositionDeleteIndex previousDeletes(Table table, FileScanTask task) {
    if (task == null || task.deletes().isEmpty())
      return null;
    return new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile)).loadPositionDeletes(task.deletes(),
        task.file().location());
  }

  private static void forEachLiveRow(Table table, FileScanTask task, RowConsumer consumer) throws IOException {
    Schema projection = new Schema(table.schema().findField("id"), MetadataColumns.ROW_POSITION);
    GenericDeleteFilter deletes = new GenericDeleteFilter(table.io(), task, table.schema(), projection);
    Schema readSchema = deletes.requiredSchema();
    Map<Integer, ?> constants = PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);
    try (CloseableIterable<Record> records = deletes.filter(Parquet.read(table.io().newInputFile(task.file()))
        .project(readSchema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema, constants))
        .build())) {
      for (Record record : records)
        consumer.accept(record.getField("id").toString(), (Long) record.getField(MetadataColumns.ROW_POSITION.name()));
    }
  }

  /**
   * Appends records (see {@link #record(Schema, String, String, Geometry, String)}) to main as new data files, one per partition.
   */
  public static void appendRecords(Table table, List<Record> records) {
    table.refresh();
    AppendFiles append = table.newAppend();
    writeDataFiles(table, records).forEach(append::appendFile);
    append.commit();
  }

  /**
   * Deletes rows of a data file of main with a position delete file (format version 2).
   */
  public static void positionDeletes(Table table, String dataFileLocation, List<Long> positions) {
    table.refresh();
    DataFile dataFile = dataFile(table, dataFileLocation);
    OutputFile out = table.io().newOutputFile(table.locationProvider().newDataLocation(table.spec(), dataFile.partition(),
        "pos-deletes-" + UUID.randomUUID() + ".parquet"));
    try {
      PositionDeleteWriter<Record> writer = Parquet.writeDeletes(out)
          .setAll(table.properties())
          .withSpec(table.spec())
          .withPartition(dataFile.partition())
          .overwrite()
          .buildPositionWriter();
      try (writer) {
        PositionDelete<Record> delete = PositionDelete.create();
        for (long position : positions.stream().sorted().toList())
          writer.write(delete.set(dataFileLocation, position));
      }
      table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Deletes the features with the ids from the partition of a data file of main with an equality delete file on {@code id}.
   */
  public static void equalityDeletes(Table table, String dataFileLocation, List<String> ids) {
    table.refresh();
    DataFile dataFile = dataFile(table, dataFileLocation);
    Schema idSchema = table.schema().select("id");
    OutputFile out = table.io().newOutputFile(table.locationProvider().newDataLocation(table.spec(), dataFile.partition(),
        "eq-deletes-" + UUID.randomUUID() + ".parquet"));
    try {
      EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(out)
          .forTable(table)
          .withSpec(table.spec())
          .withPartition(dataFile.partition())
          .rowSchema(idSchema)
          .createWriterFunc(GenericParquetWriter::create)
          .equalityFieldIds(table.schema().findField("id").fieldId())
          .overwrite()
          .buildEqualityWriter();
      try (writer) {
        for (String id : ids) {
          GenericRecord record = GenericRecord.create(idSchema);
          record.setField("id", id);
          writer.write(record);
        }
      }
      table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static DataFile dataFile(Table table, String location) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks)
        if (task.file().location().equals(location))
          return task.file();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    throw new IllegalArgumentException("No data file " + location);
  }

  private static List<DataFile> writeDataFiles(Table table, List<Record> records) {
    List<Record> sorted = new ArrayList<>(records);
    boolean cells = table.schema().findField("cell_id") != null;
    sorted.sort(Comparator.<Record, Integer>comparing(r -> (Integer) r.getField("size_class"), Comparator.nullsFirst(naturalOrder()))
        .thenComparing(r -> (Long) r.getField("tile_id"), Comparator.nullsFirst(naturalOrder()))
        .thenComparing(r -> cells ? (Long) r.getField("cell_id") : null, Comparator.nullsFirst(naturalOrder()))
        .thenComparing(r -> (String) r.getField("id")));

    Map<PartitionKey, List<Record>> partitions = new LinkedHashMap<>();
    PartitionKey key = new PartitionKey(table.spec(), table.schema());
    for (Record record : sorted) {
      key.partition(record);
      partitions.computeIfAbsent(key.copy(), k -> new ArrayList<>()).add(record);
    }

    List<DataFile> dataFiles = new ArrayList<>();
    partitions.forEach((partition, partitionRecords) -> {
      OutputFile out = table.io().newOutputFile(table.locationProvider().newDataLocation(table.spec(), partition,
          UUID.randomUUID() + ".parquet"));
      try {
        DataWriter<Record> writer = Parquet.writeData(out)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::create)
            .setAll(table.properties())
            .metricsConfig(MetricsConfig.forTable(table))
            .withSpec(table.spec())
            .withPartition(partition)
            .overwrite()
            .build();
        try (writer) {
          partitionRecords.forEach(writer::write);
        }
        dataFiles.add(writer.toDataFile());
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    return dataFiles;
  }

  private static List<Record> records(Schema schema, List<String> featureJsons) {
    return featureJsons.stream().map(json -> record(schema, json)).toList();
  }

  /**
   * A record of a feature whose columns are computed from the given values, without parsing the feature JSON (for large
   * features). The version columns stay null.
   */
  public static Record record(Schema schema, String id, String momType, Geometry geometry, String featureJson) {
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", id);
    record.setField("mom_type", momType);
    setGeometry(record, schema, geometry);
    record.setField("feature_json", featureJson);
    return record;
  }

  private static Record record(Schema schema, String featureJson) {
    try {
      JsonNode feature = MAPPER.readTree(featureJson);
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", feature.get("id").asText());
      if (feature.path("momType").isTextual())
        record.setField("mom_type", feature.get("momType").asText());
      JsonNode xyz = feature.path("properties").path(XYZ_NAMESPACE);
      if (xyz.has("version"))
        record.setField("iml_version", xyz.get("version").asLong());
      if (xyz.has("createdAt"))
        record.setField("created_at", timestamp(xyz.get("createdAt").asLong()));
      if (xyz.has("updatedAt"))
        record.setField("updated_at", timestamp(xyz.get("updatedAt").asLong()));
      if (xyz.path("author").isTextual())
        record.setField("author", xyz.get("author").asText());

      JsonNode geometryJson = feature.get("geometry");
      setGeometry(record, schema, geometryJson == null || geometryJson.isNull() ? null
          : XyzSerializable.deserialize(geometryJson.toString(), com.here.xyz.models.geojson.implementation.Geometry.class)
              .getJTSGeometry());
      record.setField("feature_json", featureJson);
      return record;
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void setGeometry(GenericRecord record, Schema schema, Geometry geometry) {
    if (geometry == null) {
      record.setField("size_class", 0);
      return;
    }
    Envelope envelope = geometry.getEnvelopeInternal();
    GenericRecord bbox = GenericRecord.create(schema.findField("bbox").type().asStructType());
    bbox.setField("xmin", envelope.getMinX());
    bbox.setField("ymin", envelope.getMinY());
    bbox.setField("xmax", envelope.getMaxX());
    bbox.setField("ymax", envelope.getMaxY());
    record.setField("bbox", bbox);
    record.setField("tile_id", tileId(envelope.centre().x, envelope.centre().y));
    if (schema.findField("cell_id") != null)
      record.setField("cell_id", tileId(envelope.centre().x, envelope.centre().y, CELL_LEVEL));
    record.setField("size_class", sizeClass(envelope));
    record.setField("geometry", java.nio.ByteBuffer.wrap(new WKBWriter(3).write(geometry)));
  }

  private static OffsetDateTime timestamp(long epochMillis) {
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
  }

  /**
   * The HERE tile key (with the level bit) of the level 12 tile containing the point.
   */
  static long tileId(double lon, double lat) {
    return tileId(lon, lat, TILE_LEVEL);
  }

  /**
   * The HERE tile key (with the level bit) of the tile of the level containing the point.
   */
  static long tileId(double lon, double lat, int level) {
    double size = 360.0 / (1 << level);
    int x = (int) Math.min((1 << level) - 1, Math.max(0, Math.floor((lon + 180) / size)));
    int y = (int) Math.min((1 << (level - 1)) - 1, Math.max(0, Math.floor((lat + 90) / size)));
    return MortonUtlis.convertXYToLongKey(x, y) | (1L << (level * 2));
  }

  private static int sizeClass(Envelope envelope) {
    double extent = Math.max(envelope.getWidth(), envelope.getHeight());
    return extent < TILE_SIZE ? 0 : extent < 16 * TILE_SIZE ? 1 : 2;
  }

  @FunctionalInterface
  private interface RowConsumer {

    void accept(String id, long pos);
  }
}
