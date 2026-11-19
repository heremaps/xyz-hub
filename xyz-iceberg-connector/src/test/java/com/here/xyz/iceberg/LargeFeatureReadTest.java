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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.iceberg.IcebergTables.SnapshotFiles;
import com.here.xyz.iceberg.IcebergTestTables.Layout;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.net.URI;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

/**
 * Reads of a table with a feature of about 110 MB: a LineString of 4,200,000 coordinates (synthetic data), in the cell layout with
 * the writer settings for large values.
 */
class LargeFeatureReadTest {

  private static final String NAMESPACE = "large";
  private static final String TABLE = "large_feature";
  private static final int COORDINATES = 4_200_000;
  private static final long MIB = 1024 * 1024;
  private static final long RESPONSE_LIMIT_OF_THE_HUB = 50 * MIB;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  @TempDir
  static Path warehouseDir;
  private static String warehouse;
  private static IcebergTables tables;
  private static long bigLength;
  private static String bigMd5;
  private static long startMillis;

  @BeforeAll
  static void writeTable() {
    startMillis = System.currentTimeMillis();
    ManagementFactory.getMemoryPoolMXBeans().forEach(MemoryPoolMXBean::resetPeakUsage);
    warehouse = warehouseDir.toUri().toString();
    Catalog catalog = CatalogUtil.buildIcebergCatalog("writer", Map.of("type", "hadoop", "warehouse", warehouse), new Configuration());
    Table table = IcebergTestTables.createTable(catalog, TableIdentifier.of(Namespace.of(NAMESPACE), TABLE), Layout.CELL, Map.of(
        "write.parquet.row-group-size-bytes", String.valueOf(32 * MIB),
        "write.parquet.page-row-limit", "2000",
        "write.parquet.row-group-check-min-record-count", "1",
        "write.parquet.row-group-check-max-record-count", "1"));

    List<Record> records = new ArrayList<>();
    records.add(bigRecord(table));
    Random random = new Random(3);
    //Points around the feature (outside of its bbox, in the same partition) and far away
    for (int i = 0; i < 200; i++) {
      double lon = 8 + random.nextDouble() * 1.5, lat = 50.5 + random.nextDouble() * 0.1;
      records.add(point(table, "near" + i, lon, lat));
    }
    for (int i = 0; i < 200; i++)
      records.add(point(table, "far" + i, 20 + random.nextDouble() * 0.1, 10 + random.nextDouble() * 0.1));
    IcebergTestTables.appendRecords(table, records);
    records.clear();
    IcebergTestTables.tag(table, "v1");

    tables = IcebergTables.forConfig(IcebergConnectorConfig.from(TwoStepScanTest.connectorParams(warehouse, Map.of(
        "catalogName", "large"))));
  }

  @AfterAll
  static void reportHeap() {
    long peak = 0;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans())
      if (pool.getType() == MemoryType.HEAP && pool.getPeakUsage() != null)
        peak += pool.getPeakUsage().getUsed();
    System.out.printf("LargeFeatureReadTest: %d MB feature, %.1f s, peak heap %d MB (sum of the pool peaks), max heap %d MB%n",
        bigLength / MIB, (System.currentTimeMillis() - startMillis) / 1000.0, peak / MIB, Runtime.getRuntime().maxMemory() / MIB);
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void aReadWithTheLargeFeatureEndsInPayloadTooLarge(boolean twoStep) throws Exception {
    assertThat(bigLength).isGreaterThan(100 * MIB).isGreaterThan(RESPONSE_LIMIT_OF_THE_HUB);
    GetFeaturesByBBoxEvent<?> event = new GetFeaturesByBBoxEvent<>().withBbox(new BBox(8.7, 49.99, 8.8, 50.01));
    event.setParams(Map.of(IcebergLocation.EVENT_PARAM, Map.of(IcebergLocation.EVENT_PARAM_LOCATION,
        new IcebergLocation(NAMESPACE, TABLE, "v1").toString(), IcebergLocation.EVENT_PARAM_MAX_RESPONSE_BYTES,
        RESPONSE_LIMIT_OF_THE_HUB, IcebergLocation.EVENT_PARAM_COUNT_MATCHES, true)));
    event.setConnectorParams(TwoStepScanTest.connectorParams(warehouse, Map.of("catalogName", "large", "twoStepReads", twoStep)));
    event.setStreamId("large-feature-test");
    event.setSpace("test-space");
    //The response limit, not the read budget, and the only match: the Hub does not fall back to the space's own connector
    IcebergConnectorTest.assertTooLarge(invoke(event), IcebergLocation.LIMIT_RESPONSE_BYTES, RESPONSE_LIMIT_OF_THE_HUB, true);
  }

  /**
   * A read of the large feature and the 200 small ones near it, the large one last in file order. With a limit at or below the
   * matches, the read may be cut off by its limit: the space's own connector might return only small features in its order, so the
   * Hub falls back. With a limit above, the read has all its matches and the Hub replies the 513.
   */
  @ParameterizedTest(name = "limit={0}")
  @ValueSource(longs = {201, 202, 1000})
  void aTooLargeReadTellsWhetherItHasAllItsMatches(long limit) {
    long matches = 201;
    FeatureScan scan = scan(box(7.9, 49.99, 9.7, 50.7), IcebergConnectorConfig.DEFAULT_MAX_BYTES_PER_REQUEST)
        .withMaxResponseBytes(RESPONSE_LIMIT_OF_THE_HUB).withCountMatches(true).withLimit(limit);
    assertThatThrownBy(scan::run).isInstanceOfSatisfying(ScanException.class, e -> {
      assertThat(e.error()).isEqualTo(XyzError.PAYLOAD_TO_LARGE);
      assertThat(e.details()).containsEntry(IcebergLocation.ERROR_DETAIL_LIMIT, IcebergLocation.LIMIT_RESPONSE_BYTES)
          .containsEntry(IcebergLocation.ERROR_DETAIL_COMPLETE, limit > matches);
    });
    assertThat(scan.stats().rowsMatched).isEqualTo(matches);

    //Below the matches, the read leaves out the large feature, which the space's own connector might return
    assertThat(scan(box(7.9, 49.99, 9.7, 50.7), IcebergConnectorConfig.DEFAULT_MAX_BYTES_PER_REQUEST)
        .withMaxResponseBytes(RESPONSE_LIMIT_OF_THE_HUB).withLimit(matches - 1).run().features()).hasSize((int) matches - 1);
  }

  @Test
  void aLargeFeatureIsReturnedIntactWithinTheLimits() throws Exception {
    FeatureScan scan = scan(box(8.7, 49.99, 8.8, 50.01), IcebergConnectorConfig.DEFAULT_MAX_BYTES_PER_REQUEST)
        .withMaxResponseBytes(200 * MIB);
    List<String> features = scan.run().features();
    assertThat(features).hasSize(1);
    assertThat((long) features.get(0).length()).isEqualTo(bigLength);
    assertThat(md5(features.get(0))).isEqualTo(bigMd5);
    assertThat(scan.stats().reader).isEqualTo("TS");
    assertThat(scan.bytesRead()).isGreaterThan(64 * MIB).isLessThanOrEqualTo(IcebergConnectorConfig.DEFAULT_MAX_BYTES_PER_REQUEST);
  }

  @Test
  void aReadNearTheLargeFeatureDoesNotFetchIt() {
    FeatureScan far = scan(box(19.9, 9.9, 20.2, 10.2), IcebergConnectorConfig.DEFAULT_MAX_BYTES_PER_REQUEST);
    assertThat(far.run().features()).hasSize(200);
    assertThat(far.bytesRead()).isLessThan(5 * MIB);
    assertThat(far.stats().filesPruned).isPositive();
  }

  @Test
  void theByteBudgetStopsBeforeTheLargePages() throws IOException {
    //The geometry and feature pages of the large feature (round 2) are larger than the budget
    long budget = 64 * MIB;
    long largePages = 0;
    for (FileScanTask task : files().tasks())
      try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(Path.of(URI.create(task.file().location()))))) {
        for (BlockMetaData block : reader.getFooter().getBlocks()) {
          long payload = 0;
          for (ColumnChunkMetaData chunk : block.getColumns())
            if (List.of("geometry", "feature_json").contains(chunk.getPath().toDotString()))
              payload += chunk.getTotalSize();
          largePages = Math.max(largePages, payload);
        }
      }
    assertThat(largePages).isGreaterThan(budget);

    FeatureScan scan = scan(box(8.7, 49.99, 8.8, 50.01), budget).withMaxResponseBytes(200 * MIB);
    //The read budget, not the response limit: the Hub falls back to the space's own connector
    assertThatThrownBy(scan::run).isInstanceOfSatisfying(ScanException.class, e -> {
      assertThat(e.error()).isEqualTo(XyzError.PAYLOAD_TO_LARGE);
      assertThat(e.details()).containsEntry(IcebergLocation.ERROR_DETAIL_LIMIT, IcebergLocation.LIMIT_READ_BYTES)
          .containsEntry(IcebergLocation.ERROR_DETAIL_MAX_BYTES, budget);
    });
    //Round 2 is counted before it is fetched, so only the footer, the page index and round 1 were read
    assertThat(scan.bytesRead()).isLessThan(MIB);
  }

  private static FeatureScan scan(Geometry geometry, long maxBytes) {
    return new FeatureScan(tables, files())
        .withBudget(new ReadBudget(maxBytes, Long.MAX_VALUE))
        .withGeometry(geometry)
        .withTwoStepReads(true);
  }

  private static SnapshotFiles files() {
    return tables.resolve(new IcebergLocation(NAMESPACE, TABLE, "v1"));
  }

  /**
   * The large feature: a LineString of {@link #COORDINATES} coordinates with 9 decimals from 8 to about 9.6 degrees of longitude,
   * so of size class 2. Its JSON is written directly and its columns are computed without parsing it.
   */
  private static Record bigRecord(Table table) {
    Random random = new Random(5);
    double[] packed = new double[2 * COORDINATES];
    StringBuilder json = new StringBuilder(120_000_000);
    json.append("{\"type\":\"Feature\",\"id\":\"big\",\"momType\":\"Road\",\"geometry\":{\"type\":\"LineString\",\"coordinates\":[");
    long scale = 1_000_000_000L;
    long x = 8 * scale;
    for (int i = 0; i < COORDINATES; i++) {
      long y = 50 * scale + random.nextInt(2_000_000) - 1_000_000;
      packed[2 * i] = (double) x / scale;
      packed[2 * i + 1] = (double) y / scale;
      json.append(i == 0 ? "[" : ",[");
      appendFixed(json, x, scale);
      json.append(',');
      appendFixed(json, y, scale);
      json.append(']');
      x += 300 + random.nextInt(162);
    }
    json.append("]},\"properties\":{\"name\":\"big\",\"@ns:com:here:xyz\":{\"version\":1}}}");
    LineString line = GEOMETRY_FACTORY.createLineString(new PackedCoordinateSequence.Double(packed, 2, 0));
    String feature = json.toString();
    json.setLength(0);
    json.trimToSize();
    bigLength = feature.length();
    bigMd5 = md5(feature);
    return IcebergTestTables.record(table.schema(), "big", "Road", line, feature);
  }

  private static void appendFixed(StringBuilder target, long value, long scale) {
    String fraction = Long.toString(value % scale);
    target.append(value / scale).append('.');
    for (int i = fraction.length(); i < 9; i++)
      target.append('0');
    target.append(fraction);
  }

  private static Record point(Table table, String id, double lon, double lat) {
    String json = "{\"type\":\"Feature\",\"id\":\"" + id + "\",\"momType\":\"Road\",\"geometry\":{\"type\":\"Point\",\"coordinates\":["
        + lon + "," + lat + "]},\"properties\":{\"name\":\"" + id + "\"}}";
    return IcebergTestTables.record(table.schema(), id, "Road", GEOMETRY_FACTORY.createPoint(new org.locationtech.jts.geom.Coordinate(
        lon, lat)), json);
  }

  private static Geometry box(double minX, double minY, double maxX, double maxY) {
    return GEOMETRY_FACTORY.toGeometry(new Envelope(minX, maxX, minY, maxY));
  }

  private static String md5(String value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(value.getBytes(UTF_8)));
    }
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static XyzResponse invoke(GetFeaturesByBBoxEvent<?> event) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new IcebergConnector().handleRequest(new ByteArrayInputStream(event.serialize().getBytes(UTF_8)), out,
        new SimulatedContext("large-feature-test", Map.of()));
    return XyzSerializable.deserialize(out.toByteArray());
  }
}
