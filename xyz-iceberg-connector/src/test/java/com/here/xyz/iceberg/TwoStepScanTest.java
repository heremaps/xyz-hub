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
import com.here.xyz.iceberg.FeatureScan.ColumnFilter;
import com.here.xyz.iceberg.IcebergTables.SnapshotFiles;
import com.here.xyz.iceberg.IcebergTestTables.Layout;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

class TwoStepScanTest {

  private static final String NAMESPACE = "two_step";
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final double TILE_SIZE = 360.0 / (1 << IcebergTestTables.TILE_LEVEL);
  /**
   * The level 12 tiles of the features: 3 x 3 tiles across the boundary of two partitions ({@code truncate(4096, tile_id)}
   * groups 64 x 64 tiles, the boundary at x = 2112 is at 5.625 degrees)
   */
  private static final double[] TILE_LONS = {5.5, 5.6, 5.7};
  private static final double[] TILE_LATS = {50.3, 50.4, 50.5};
  private static final List<String> MOM_TYPES = java.util.Arrays.asList("Road", "Building", "Water", null);
  private static final Map<String, String> MANY_PAGES = Map.of(
      "write.parquet.page-row-limit", "7",
      "write.parquet.row-group-size-bytes", "8192",
      "write.parquet.row-group-check-min-record-count", "1",
      "write.parquet.row-group-check-max-record-count", "1");

  @TempDir
  static Path warehouseDir;
  private static Catalog catalog;
  private static IcebergTables tables;

  @BeforeAll
  static void createCatalog() {
    String warehouse = warehouseDir.toUri().toString();
    catalog = CatalogUtil.buildIcebergCatalog("writer", Map.of("type", "hadoop", "warehouse", warehouse), new Configuration());
    tables = IcebergTables.forConfig(IcebergConnectorConfig.from(connectorParams(warehouse, Map.of())));
  }

  @ParameterizedTest
  @EnumSource(Layout.class)
  void twoStepEqualsTheRowGroupReader(Layout layout) {
    String name = "equal_" + layout.name().toLowerCase(Locale.ROOT);
    Table table = IcebergTestTables.createTable(catalog, id(name), layout, MANY_PAGES);
    Random random = new Random(42);
    List<String> all = new ArrayList<>();
    for (int i = 0; i < 3000; i++)
      all.add(randomFeature(random, String.format("f%04d", i), "v1"));
    for (int i = 0; i < 3; i++)
      IcebergTestTables.append(table, null, all.subList(i * 1000, (i + 1) * 1000));
    List<String> updates = new ArrayList<>();
    List<String> deletes = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      updates.add(randomFeature(random, String.format("f%04d", i * 30), "v2"));
      deletes.add(String.format("f%04d", i * 30 + 7));
    }
    IcebergTestTables.upsertAndDelete(table, null, updates, deletes);
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);
    assertThat(files.tasks()).hasSizeGreaterThanOrEqualTo(2);
    assertThat(files.tasks()).anySatisfy(task -> assertThat(task.deletes()).isNotEmpty());
    assertThat(files.tasks()).anySatisfy(task -> assertThat(task.file().splitOffsets()).hasSizeGreaterThan(10));

    List<Read> reads = new ArrayList<>();
    for (int i = 0; i < 60; i++) {
      double size = Math.pow(10, -4 + 3.5 * random.nextDouble());
      double x = TILE_LONS[0] - 0.1 + random.nextDouble() * 0.4, y = TILE_LATS[0] - 0.1 + random.nextDouble() * 0.4;
      reads.add(new Read(box(x, y, x + size, y + size * (0.5 + random.nextDouble())), null, Long.MAX_VALUE));
    }
    for (int i = 0; i < 10; i++) {
      double lon = TILE_LONS[i % 3], lat = TILE_LATS[(i / 3) % 3];
      long tile = IcebergTestTables.tileId(lon, lat);
      Envelope tileEnvelope = tileEnvelope(lon, lat);
      double margin = TILE_SIZE * (i % 4) / 16;
      assertThat(IcebergTestTables.tileId(tileEnvelope.centre().x, tileEnvelope.centre().y)).isEqualTo(tile);
      reads.add(new Read(box(tileEnvelope.getMinX() - margin, tileEnvelope.getMinY() - margin, tileEnvelope.getMaxX() + margin,
          tileEnvelope.getMaxY() + margin), null, Long.MAX_VALUE));
    }
    for (int i = 0; i < 10; i++) {
      double x = TILE_LONS[0] + random.nextDouble() * 0.25, y = TILE_LATS[0] + random.nextDouble() * 0.25;
      reads.add(new Read(buffer(GEOMETRY_FACTORY.createPoint(new Coordinate(x, y)), 200 + random.nextInt(5000)), null,
          Long.MAX_VALUE));
    }
    Geometry wide = box(TILE_LONS[0] - 0.1, TILE_LATS[0] - 0.1, TILE_LONS[2] + 0.1, TILE_LATS[2] + 0.1);
    reads.add(new Read(wide, List.of(List.of(new ColumnFilter("mom_type", Set.of("Road")))), Long.MAX_VALUE));
    reads.add(new Read(wide, List.of(List.of(new ColumnFilter("mom_type", Set.of("Water"))),
        List.of(new ColumnFilter("mom_type", Set.of("Building", "Road")), new ColumnFilter("mom_type", Set.of("Building")))), 500));
    reads.add(new Read(box(5.55, 50.35, 5.65, 50.45), List.of(List.of(new ColumnFilter("mom_type", Set.of("Building")))), 50));
    for (long limit : new long[] {1, 5, 50}) {
      reads.add(new Read(wide, null, limit));
      reads.add(new Read(box(5.6, 50.4, 5.7, 50.5), null, limit));
    }

    int nonEmpty = 0;
    for (Read read : reads) {
      FeatureScan rowGroups = scan(files, read, false);
      FeatureScan twoStep = scan(files, read, true);
      List<String> expected = rowGroups.run().features();
      List<String> actual = twoStep.run().features();
      assertThat(actual).as(read.toString()).isEqualTo(expected);
      assertThat(twoStep.stats().reader).isEqualTo("TS");
      assertThat(rowGroups.stats().reader).isEqualTo("RG");
      assertThat(twoStep.stats().unplannedReads).as("unplanned reads of " + read).isZero();
      if (!expected.isEmpty())
        nonEmpty++;
    }
    assertThat(nonEmpty).isGreaterThan(reads.size() / 2);
    //The deletes and updates are applied
    List<String> wideRead = scan(files, new Read(wide, null, Long.MAX_VALUE), true).run().features();
    Set<String> ids = ids(wideRead);
    assertThat(ids).doesNotContainAnyElementsOf(deletes);
    assertThat(wideRead).filteredOn(json -> json.contains("\"f0030\"")).allSatisfy(json -> assertThat(json).contains("f0030-v2"));
  }

  /**
   * Concurrent two-step scans in one JVM, as concurrent tile requests to the embedded connector (DS-2908): shared Parquet read
   * options made {@code ParquetFileReader.close()} of one scan iterate the decompressor map of a CodecFactory while the readers
   * of other scans added to it, which failed scans with a ConcurrentModificationException.
   */
  @Test
  void concurrentTwoStepScans() throws Exception {
    String name = "concurrent";
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, MANY_PAGES);
    Random random = new Random(42);
    List<String> all = new ArrayList<>();
    for (int i = 0; i < 3000; i++)
      all.add(randomFeature(random, String.format("k%04d", i), "v1"));
    for (int i = 0; i < 3; i++)
      IcebergTestTables.append(table, null, all.subList(i * 1000, (i + 1) * 1000));
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);

    List<Read> reads = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      double size = 0.02 + random.nextDouble() * 0.2;
      double x = TILE_LONS[0] - 0.1 + random.nextDouble() * 0.4, y = TILE_LATS[0] - 0.1 + random.nextDouble() * 0.4;
      reads.add(new Read(box(x, y, x + size, y + size), null, Long.MAX_VALUE));
    }
    List<List<String>> expected = new ArrayList<>();
    for (Read read : reads)
      expected.add(scan(files, read, false).run().features());
    assertThat(expected).anySatisfy(features -> assertThat(features).isNotEmpty());

    int threads = 16, scansPerThread = 40;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        Random threadRandom = new Random(t);
        futures.add(pool.submit(() -> {
          start.await();
          for (int i = 0; i < scansPerThread; i++) {
            int read = threadRandom.nextInt(reads.size());
            FeatureScan twoStep = scan(files, reads.get(read), true);
            assertThat(twoStep.run().features()).as(reads.get(read).toString()).isEqualTo(expected.get(read));
            assertThat(twoStep.stats().reader).isEqualTo("TS");
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> future : futures)
        future.get();
    }
    finally {
      pool.shutdownNow();
    }
  }

  @Test
  void rowsOnBothSidesOfAPageBoundary() {
    String name = "page_boundary";
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, MANY_PAGES);
    //a and b are neighbours across the boundary of the tile columns x = 2111 and 2112, which are far apart in the tile order:
    //the tiles of other rows (y) of the column x = 2111 come between them
    double boundary = -180 + 2112 * TILE_SIZE;
    double lat = TILE_LATS[1];
    List<String> features = new ArrayList<>();
    features.add(pointFeature("a", "Road", boundary - 0.0001, lat, "a"));
    features.add(pointFeature("b", "Road", boundary + 0.0001, lat, "b"));
    for (int i = 0; i < 300; i++)
      features.add(pointFeature(String.format("x%03d", i), "Road", boundary - 0.0002 - (i % 10) * 0.008,
          lat - 2 * TILE_SIZE + (i / 10) * TILE_SIZE / 7.5, "filler" + i));
    IcebergTestTables.append(table, null, features);
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);

    Map<String, Long> positions = positions(table);
    assertThat(Math.abs(positions.get("b") - positions.get("a"))).as("rows apart").isGreaterThan(14);
    Read read = new Read(box(boundary - 0.00015, lat - 0.00005, boundary + 0.00015, lat + 0.00005), null, Long.MAX_VALUE);
    FeatureScan twoStep = scan(files, read, true);
    List<String> actual = twoStep.run().features();
    assertThat(ids(actual)).containsExactlyInAnyOrder("a", "b");
    assertThat(actual).allSatisfy(json -> assertThat(json).contains("\"name\":\"" + idOf(json) + "\""));
    assertThat(actual).isEqualTo(scan(files, read, false).run().features());
    assertThat(twoStep.stats().round1Rows).isLessThan(100);
  }

  @Test
  void positionDeleteFilesAreApplied() {
    String name = "position_deletes";
    Map<String, String> properties = new HashMap<>(MANY_PAGES);
    properties.put("format-version", "2");
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, properties);
    Random random = new Random(7);
    List<String> features = new ArrayList<>();
    for (int i = 0; i < 200; i++)
      features.add(pointFeature(String.format("p%03d", i), "Road", 5.5 + random.nextDouble() * 0.05, 50.3 + random.nextDouble() * 0.05,
          "p" + i));
    IcebergTestTables.append(table, null, features);
    table.refresh();
    String dataFile = table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location();
    Map<String, Long> positions = positions(table);
    List<String> deleted = List.of("p003", "p050", "p051", "p199");
    IcebergTestTables.positionDeletes(table, dataFile, deleted.stream().map(positions::get).toList());
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);
    assertThat(files.tasks().get(0).deletes()).isNotEmpty();

    Read read = new Read(box(5.4, 50.2, 5.6, 50.4), null, Long.MAX_VALUE);
    FeatureScan twoStep = scan(files, read, true);
    List<String> actual = twoStep.run().features();
    assertThat(actual).isEqualTo(scan(files, read, false).run().features());
    assertThat(ids(actual)).hasSize(196).doesNotContainAnyElementsOf(deleted);
    assertThat(twoStep.stats().reader).isEqualTo("TS");
  }

  @Test
  void equalityDeletesFallBackToTheRowGroupReader() {
    String name = "equality_deletes";
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, MANY_PAGES);
    List<String> features = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      features.add(pointFeature(String.format("w%02d", i), "Road", 5.6 + i * 0.0001, 50.4, "w" + i));
      features.add(pointFeature(String.format("e%02d", i), "Road", 5.7 + i * 0.0001, 50.4, "e" + i));
    }
    IcebergTestTables.append(table, null, features);
    table.refresh();
    String westFile = null;
    for (var file : table.currentSnapshot().addedDataFiles(table.io()))
      if (fileContains(table, file.location(), "w00"))
        westFile = file.location();
    assertThat(westFile).isNotNull();
    IcebergTestTables.equalityDeletes(table, westFile, List.of("w01", "w02"));
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);
    assertThat(files.tasks()).hasSize(2);

    Read read = new Read(box(5.5, 50.3, 5.8, 50.5), null, Long.MAX_VALUE);
    FeatureScan twoStep = scan(files, read, true);
    List<String> actual = twoStep.run().features();
    assertThat(actual).isEqualTo(scan(files, read, false).run().features());
    assertThat(ids(actual)).hasSize(98).doesNotContain("w01", "w02");
    assertThat(twoStep.stats().reader).isEqualTo("TS+RG");
  }

  @Test
  void aSmallReadFetchesOnlyItsPages() {
    String name = "small_read";
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, Map.of("write.parquet.page-row-limit", "2000"));
    Random random = new Random(11);
    //20,000 points of about 2 KB in 4 x 4 tiles of one partition, so one data file
    double minLon = -180 + 2100 * TILE_SIZE, minLat = -90 + 1590 * TILE_SIZE;
    List<String> features = new ArrayList<>();
    for (int i = 0; i < 20_000; i++) {
      StringBuilder text = new StringBuilder();
      for (int c = 0; c < 2000; c++)
        text.append((char) ('a' + random.nextInt(26)));
      features.add(pointFeature(String.format("s%05d", i), MOM_TYPES.get(i % 4), minLon + random.nextDouble() * 4 * TILE_SIZE,
          minLat + random.nextDouble() * 4 * TILE_SIZE, text.toString()));
    }
    IcebergTestTables.append(table, null, features);
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);
    assertThat(files.tasks()).hasSize(1);

    //A box of 1e-6 square degrees around one of the points
    String target = features.get(1234);
    double[] xy = coordinates(target);
    Read read = new Read(box(xy[0] - 0.0005, xy[1] - 0.0005, xy[0] + 0.0005, xy[1] + 0.0005), null, Long.MAX_VALUE);
    FeatureScan cold = scan(files, read, true);
    assertThat(cold.run().features()).contains(target);
    assertThat(cold.stats().rounds).isEqualTo(3);
    FeatureScan rowGroups = scan(files, read, false);
    List<String> expected = rowGroups.run().features();
    FeatureScan warm = scan(files, read, true);
    assertThat(warm.run().features()).isEqualTo(expected);
    assertThat(warm.stats().rounds).isEqualTo(2);
    assertThat(warm.stats().pageIndexBytes).isZero();
    assertThat(warm.bytesRead()).as("bytes of TS against RG " + rowGroups.bytesRead()).isLessThanOrEqualTo(rowGroups.bytesRead() / 5);
  }

  @Test
  void aLimitedReadOverManyFilesStopsAtTheLimit() {
    SnapshotFiles files = manyFiles("limit_many_files", false);
    Read read = new Read(box(4.99, 49.99, 5.01, 50.01), null, 10);
    FeatureScan rowGroups = scan(files, read, false);
    List<String> expected = rowGroups.run().features();
    assertThat(expected).hasSize(10);
    long rowGroupBytes = rowGroups.bytesRead();

    //A budget which the row-group reader fits
    FeatureScan twoStep = scan(files, read, true, new ReadBudget(rowGroupBytes + 1, Long.MAX_VALUE));
    assertThat(twoStep.run().features()).isEqualTo(expected);
    assertThat(twoStep.stats().reader).isEqualTo("TS");
    assertThat(twoStep.stats().pageIndexBytes).as("page indexes of the first file only").isLessThan(rowGroupBytes / 4);
    assertThat(twoStep.bytesRead()).isLessThanOrEqualTo(rowGroupBytes);
    assertThat(twoStep.stats().unplannedReads).isZero();

    //More features than the first file holds
    Read more = new Read(read.geometry(), null, 4500);
    List<String> expectedMore = scan(files, more, false).run().features();
    FeatureScan twoStepMore = scan(files, more, true);
    assertThat(twoStepMore.run().features()).isEqualTo(expectedMore).hasSize(4500);
    assertThat(twoStepMore.stats().reader).isEqualTo("TS");
    //One group of the five files which are estimated to hold twice the limit
    assertThat(twoStepMore.stats().rounds).isLessThanOrEqualTo(3);
  }

  @Test
  void aLimitedReadWhoseFirstRoundDoesNotFitUsesTheRowGroupReader() {
    //One far point per file spreads the bbox bounds of each file, so that the estimate puts all files into the first group
    SnapshotFiles files = manyFiles("limit_round1_too_large", true);
    Read read = new Read(box(4.99, 49.99, 5.01, 50.01), null, 10);
    //Footers and page indexes are cached
    scan(files, new Read(read.geometry(), null, Long.MAX_VALUE), true).run();
    FeatureScan rowGroups = scan(files, read, false);
    List<String> expected = rowGroups.run().features();
    assertThat(expected).hasSize(10);

    FeatureScan twoStep = scan(files, read, true, new ReadBudget(rowGroups.bytesRead() + 1, Long.MAX_VALUE));
    assertThat(twoStep.run().features()).isEqualTo(expected);
    assertThat(twoStep.stats().reader).isEqualTo("RG");
    assertThat(twoStep.stats().rangeReads).isZero();
    //Without a limit, the read still fails before round 1
    ReadBudget budget = new ReadBudget(rowGroups.bytesRead() + 1, Long.MAX_VALUE);
    FeatureScan unlimited = scan(files, new Read(read.geometry(), null, Long.MAX_VALUE), true, budget);
    assertThatThrownBy(unlimited::run).isInstanceOfSatisfying(ScanException.class,
        e -> assertThat(e.error()).isEqualTo(XyzError.PAYLOAD_TO_LARGE));
    assertThat(budget.bytesRead()).isZero();
  }

  /**
   * A table of 20 data files of 2,000 points each, in one partition, around (5.0, 50.0).
   *
   * @param farPoint Whether each file also has a point at (0.5, 45.5)
   */
  private static SnapshotFiles manyFiles(String name, boolean farPoint) {
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, Map.of("write.parquet.page-row-limit", "2000"));
    Random random = new Random(13);
    for (int file = 0; file < 20; file++) {
      List<String> features = new ArrayList<>();
      for (int i = 0; i < 2000; i++)
        features.add(pointFeature(String.format("m%02d_%04d", file, i), "Road", 4.995 + random.nextDouble() * 0.01,
            49.995 + random.nextDouble() * 0.01, "m" + file + "_" + i));
      if (farPoint)
        features.add(pointFeature(String.format("m%02d_far", file), "Road", 0.5, 45.5, "far" + file));
      IcebergTestTables.append(table, null, features);
    }
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);
    assertThat(files.tasks()).hasSize(20);
    return files;
  }

  @Test
  void thePageIndexIsCached() {
    String name = "page_index_cache";
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CURRENT, MANY_PAGES);
    List<String> features = new ArrayList<>();
    for (int i = 0; i < 100; i++)
      features.add(pointFeature(String.format("c%03d", i), "Road", 5.6 + i * 0.0003, 50.4, "c" + i));
    IcebergTestTables.append(table, null, features);
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);

    Read read = new Read(box(5.6, 50.39, 5.601, 50.41), null, Long.MAX_VALUE);
    FeatureScan first = scan(files, read, true);
    List<String> expected = first.run().features();
    assertThat(expected).isNotEmpty();
    assertThat(first.stats().pageIndexBytes).isPositive();
    FeatureScan second = scan(files, read, true);
    assertThat(second.run().features()).isEqualTo(expected);
    assertThat(second.stats().pageIndexBytes).isZero();
    assertThat(second.stats().rounds).isEqualTo(2);
  }

  @Test
  void budgetAndDeadline() throws Exception {
    String name = "budget";
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, MANY_PAGES);
    List<String> features = new ArrayList<>();
    for (int i = 0; i < 300; i++)
      features.add(pointFeature(String.format("b%03d", i), "Road", 5.6 + (i % 20) * 0.001, 50.4 + (i / 20) * 0.001, "b" + i));
    IcebergTestTables.append(table, null, features);
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);
    Read read = new Read(box(5.6, 50.4, 5.605, 50.405), null, Long.MAX_VALUE);
    scan(files, read, true).run();
    FeatureScan warm = scan(files, read, true);
    List<String> expected = warm.run().features();
    assertThat(expected).isNotEmpty();
    long bytes = warm.bytesRead();
    long rangeReads = warm.stats().rangeReads;

    //Round 2 does not fit: it fails before it reads anything
    ReadBudget budget = new ReadBudget(bytes - 1, Long.MAX_VALUE);
    FeatureScan tooSmall = new FeatureScan(tables, files).withBudget(budget).withGeometry(read.geometry()).withTwoStepReads(true);
    assertThatThrownBy(tooSmall::run).isInstanceOfSatisfying(ScanException.class,
        e -> assertThat(e.error()).isEqualTo(XyzError.PAYLOAD_TO_LARGE));
    assertThat(budget.bytesRead()).isLessThan(bytes);
    assertThat(tooSmall.stats().rangeReads).isLessThan(rangeReads);
    //Round 1 does not fit
    ReadBudget tiny = new ReadBudget(1, Long.MAX_VALUE);
    FeatureScan nothing = new FeatureScan(tables, files).withBudget(tiny).withGeometry(read.geometry()).withTwoStepReads(true);
    assertThatThrownBy(nothing::run).isInstanceOfSatisfying(ScanException.class,
        e -> assertThat(e.error()).isEqualTo(XyzError.PAYLOAD_TO_LARGE));
    assertThat(tiny.bytesRead()).isZero();
    assertThat(nothing.stats().rangeReads).isZero();

    //A new catalog configuration starts cold: loading the table alone takes longer than the timeout
    Map<String, Object> params = connectorParams(warehouseDir.toUri().toString(), Map.of("requestTimeoutMs", 1,
        "catalogName", "two_step_timeout"));
    GetFeaturesByBBoxEvent<?> event = new GetFeaturesByBBoxEvent<>().withBbox(new BBox(5.6, 50.4, 5.605, 50.405));
    event.setParams(Map.of(IcebergLocation.EVENT_PARAM, Map.of(IcebergLocation.EVENT_PARAM_LOCATION,
        new IcebergLocation(NAMESPACE, name, "v1").toString())));
    event.setConnectorParams(params);
    event.setStreamId("two-step-test");
    event.setSpace("test-space");
    XyzResponse response = invoke(event);
    assertThat(response).isInstanceOf(ErrorResponse.class);
    assertThat(((ErrorResponse) response).getError()).isEqualTo(XyzError.TIMEOUT);
  }

  @Test
  void twoStepReadsFalseUsesTheRowGroupReader() {
    String name = "row_group_reader";
    Table table = IcebergTestTables.createTable(catalog, id(name), Layout.CELL, MANY_PAGES);
    IcebergTestTables.append(table, null, List.of(pointFeature("r1", "Road", 5.6, 50.4, "r1")));
    IcebergTestTables.tag(table, "v1");
    SnapshotFiles files = resolve(name);
    Read read = new Read(box(5.5, 50.3, 5.7, 50.5), null, Long.MAX_VALUE);
    FeatureScan rowGroups = scan(files, read, false);
    assertThat(ids(rowGroups.run().features())).containsExactly("r1");
    assertThat(rowGroups.stats().reader).isEqualTo("RG");
    assertThat(rowGroups.stats().rounds).isZero();
    assertThat(IcebergConnectorConfig.from(Map.of(), n -> null).twoStepReads()).isTrue();
    //A paged read (iterate) never uses the two-step reader
    FeatureScan paged = new FeatureScan(tables, files).withBudget(new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE))
        .withGeometry(read.geometry()).withTwoStepReads(true).withPaging(FeatureScan.Position.FIRST);
    assertThat(ids(paged.run().features())).containsExactly("r1");
    assertThat(paged.stats().reader).isEqualTo("RG");
  }

  private record Read(Geometry geometry, List<List<ColumnFilter>> filters, long limit) {}

  private static FeatureScan scan(SnapshotFiles files, Read read, boolean twoStep) {
    return scan(files, read, twoStep, new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE));
  }

  private static FeatureScan scan(SnapshotFiles files, Read read, boolean twoStep, ReadBudget budget) {
    FeatureScan scan = new FeatureScan(tables, files)
        .withBudget(budget)
        .withGeometry(read.geometry())
        .withLimit(read.limit())
        .withTwoStepReads(twoStep);
    return read.filters() == null ? scan : scan.withFilters(read.filters());
  }

  private static SnapshotFiles resolve(String table) {
    return tables.resolve(new IcebergLocation(NAMESPACE, table, "v1"));
  }

  private static TableIdentifier id(String table) {
    return TableIdentifier.of(Namespace.of(NAMESPACE), table);
  }

  /**
   * The row positions of the rows by id, in the data files of main (without applying deletes).
   */
  private static Map<String, Long> positions(Table table) {
    table.refresh();
    Schema projection = new Schema(table.schema().findField("id"), MetadataColumns.ROW_POSITION);
    Map<String, Long> positions = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks)
        try (CloseableIterable<Record> records = Parquet.read(table.io().newInputFile(task.file()))
            .project(projection)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema))
            .build()) {
          for (Record record : records)
            positions.put(record.getField("id").toString(), (Long) record.getField(MetadataColumns.ROW_POSITION.name()));
        }
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return positions;
  }

  private static boolean fileContains(Table table, String location, String id) {
    Schema projection = new Schema(table.schema().findField("id"));
    try (CloseableIterable<Record> records = Parquet.read(table.io().newInputFile(location))
        .project(projection)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema))
        .build()) {
      for (Record record : records)
        if (id.equals(record.getField("id").toString()))
          return true;
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return false;
  }

  private static String randomFeature(Random random, String id, String version) {
    double lon = TILE_LONS[random.nextInt(3)] + random.nextDouble() * TILE_SIZE * 0.9;
    double lat = TILE_LATS[random.nextInt(3)] + random.nextDouble() * TILE_SIZE * 0.9;
    String momType = MOM_TYPES.get(random.nextInt(MOM_TYPES.size()));
    int kind = random.nextInt(100);
    String geometry;
    if (kind < 5)
      geometry = null;
    else if (kind < 65)
      geometry = point(lon, lat);
    else if (kind < 85) {
      StringBuilder line = new StringBuilder("{\"type\":\"LineString\",\"coordinates\":[");
      int points = 2 + random.nextInt(6);
      double x = lon, y = lat;
      for (int i = 0; i < points; i++) {
        line.append(i == 0 ? "" : ",").append("[").append(x).append(",").append(y).append("]");
        x += (random.nextDouble() - 0.5) * 0.02;
        y += (random.nextDouble() - 0.5) * 0.02;
      }
      geometry = line.append("]}").toString();
    }
    else {
      //Size classes 0 to 2: up to 0.03, 0.3 or 2 degrees
      double size = kind < 93 ? 0.03 * random.nextDouble() : kind < 98 ? 0.1 + 0.2 * random.nextDouble() : 1.5 + random.nextDouble();
      geometry = polygon(lon, lat, lon + size, lat + size * (0.3 + random.nextDouble()));
    }
    return feature(id, momType, geometry, id + "-" + version);
  }

  private static String pointFeature(String id, String momType, double lon, double lat, String name) {
    return feature(id, momType, point(lon, lat), name);
  }

  private static String point(double lon, double lat) {
    return "{\"type\":\"Point\",\"coordinates\":[" + lon + "," + lat + "]}";
  }

  private static String polygon(double minX, double minY, double maxX, double maxY) {
    return "{\"type\":\"Polygon\",\"coordinates\":[[[" + minX + "," + minY + "],[" + maxX + "," + minY + "],[" + maxX + "," + maxY
        + "],[" + minX + "," + maxY + "],[" + minX + "," + minY + "]]]}";
  }

  private static String feature(String id, String momType, String geometry, String name) {
    return "{\"type\":\"Feature\",\"id\":\"" + id + "\"" + (momType == null ? "" : ",\"momType\":\"" + momType + "\"")
        + ",\"geometry\":" + geometry + ",\"properties\":{\"name\":\"" + name + "\",\"@ns:com:here:xyz\":{\"version\":1,"
        + "\"createdAt\":1700000000000,\"updatedAt\":1700000000000,\"author\":\"tester\"}}}";
  }

  private static final Pattern ID = Pattern.compile("\"id\":\"([^\"]+)\"");
  private static final Pattern COORDINATES = Pattern.compile("\"coordinates\":\\[([-0-9.E]+),([-0-9.E]+)]");

  private static String idOf(String json) {
    Matcher matcher = ID.matcher(json);
    assertThat(matcher.find()).isTrue();
    return matcher.group(1);
  }

  private static Set<String> ids(List<String> features) {
    Set<String> ids = new HashSet<>();
    for (String json : features)
      ids.add(idOf(json));
    assertThat(ids).hasSameSizeAs(features);
    return ids;
  }

  private static double[] coordinates(String pointJson) {
    Matcher matcher = COORDINATES.matcher(pointJson);
    assertThat(matcher.find()).isTrue();
    return new double[] {Double.parseDouble(matcher.group(1)), Double.parseDouble(matcher.group(2))};
  }

  private static Geometry box(double minX, double minY, double maxX, double maxY) {
    return GEOMETRY_FACTORY.toGeometry(new Envelope(minX, maxX, minY, maxY));
  }

  private static Envelope tileEnvelope(double lon, double lat) {
    double x = Math.floor((lon + 180) / TILE_SIZE) * TILE_SIZE - 180, y = Math.floor((lat + 90) / TILE_SIZE) * TILE_SIZE - 90;
    return new Envelope(x, x + TILE_SIZE, y, y + TILE_SIZE);
  }

  private static Geometry buffer(Geometry geometry, int radius) {
    try {
      return IcebergConnector.buffer(geometry, radius);
    }
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static Map<String, Object> connectorParams(String warehouse, Map<String, Object> overrides) {
    Map<String, Object> params = new HashMap<>();
    params.put("connectorId", "iceberg");
    params.put("className", IcebergConnector.class.getName());
    params.put("catalogName", "two_step");
    params.put("catalogProperties", Map.of("type", "hadoop", "warehouse", warehouse));
    params.putAll(overrides);
    return params;
  }

  private static XyzResponse invoke(GetFeaturesByBBoxEvent<?> event) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new IcebergConnector().handleRequest(new ByteArrayInputStream(event.serialize().getBytes(UTF_8)), out,
        new SimulatedContext("two-step-test", Map.of()));
    return XyzSerializable.deserialize(out.toByteArray());
  }
}
