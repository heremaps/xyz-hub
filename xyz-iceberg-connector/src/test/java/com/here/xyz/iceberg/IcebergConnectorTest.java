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

import static com.here.xyz.events.PropertyQuery.QueryOperation.EQUALS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.iceberg.FeatureScan.Result;
import com.here.xyz.iceberg.IcebergTables.SnapshotFiles;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class IcebergConnectorTest {

  private static final String TABLE = "space_a";
  private static final String NAMESPACE = "exports";
  private static final String BRANCH = "ext_" + TABLE + "_b2";
  private static final String BRANCH_TAG = BRANCH + "_v5";
  private static final Set<String> V1_IDS = Set.of("p1", "p2", "p3", "p4", "l1", "poly1", "n1", "r1", "r2", "r3", "far1", "far2");
  private static final Set<String> V2_IDS = Set.of("p1", "p2", "p4", "l1", "poly1", "n1", "r1", "r2", "r3", "far1", "far2", "p5");
  //A table whose data file has many row groups of ROW_GROUP_ROWS rows
  private static final String ROW_GROUP_TABLE = "space_row_groups";
  private static final int ROW_GROUP_ROWS = 4;
  private static final int ROW_GROUP_TABLE_ROWS = 40;
  private static final Set<String> ROW_GROUP_DELETED_IDS = Set.of("g05", "g17", "g33");

  @TempDir
  static Path warehouseDir;
  private static String warehouse;
  private static Catalog catalog;

  @BeforeAll
  static void writeTable() {
    warehouse = warehouseDir.toUri().toString();
    catalog = CatalogUtil.buildIcebergCatalog("writer", Map.of("type", "hadoop", "warehouse", warehouse), new Configuration());
    Table table = IcebergTestTables.createTable(catalog, TableIdentifier.of(Namespace.of(NAMESPACE), TABLE));

    IcebergTestTables.append(table, null, List.of(
        point("p1", "Road", 1, 1, "p1"),
        point("p2", "Building", 2, 2, "p2-v1"),
        point("p3", "Road", 3, 3, "p3"),
        point("p4", null, 4.5, 4.5, "p4"),
        feature("l1", "Road", "{\"type\":\"LineString\",\"coordinates\":[[0.5,3.0],[3.0,0.5]]}", "l1"),
        feature("poly1", "Building", "{\"type\":\"Polygon\",\"coordinates\":[[[5,5],[6,5],[6,6],[5,6],[5,5]]]}", "poly1"),
        feature("n1", "Road", null, "n1")));
    IcebergTestTables.append(table, null, List.of(
        point("r1", "Road", 10.3, 10, "r1"),
        point("r2", "Road", 10.6, 10, "r2"),
        point("r3", "Water", 10, 10.2, "r3"),
        point("far1", "Water", 50, 40, "far1"),
        point("far2", "Building", -70, -30, "far2")));
    IcebergTestTables.tag(table, "v1");

    IcebergTestTables.upsertAndDelete(table, null, List.of(point("p2", "Building", 2, 2, "p2-v2"), point("p5", "Road", 1.5, 1.5,
        "p5")), List.of("p3"));
    IcebergTestTables.tag(table, "v2");

    IcebergTestTables.branchFromTag(table, BRANCH, "v2");
    IcebergTestTables.append(table, BRANCH, List.of(point("b1", "Road", 1.2, 1.2, "b1")));
    IcebergTestTables.tag(table, BRANCH_TAG, BRANCH);

    Table rowGroups = IcebergTestTables.createTable(catalog, TableIdentifier.of(Namespace.of(NAMESPACE), ROW_GROUP_TABLE));
    rowGroups.updateProperties()
        .set("write.parquet.row-group-size-bytes", "1")
        .set("write.parquet.row-group-check-min-record-count", String.valueOf(ROW_GROUP_ROWS))
        .set("write.parquet.row-group-check-max-record-count", String.valueOf(ROW_GROUP_ROWS))
        .commit();
    List<String> features = new ArrayList<>();
    for (int i = 0; i < ROW_GROUP_TABLE_ROWS; i++)
      features.add(point(rowGroupId(i), "Road", 20 + i * 0.001, 20, "g" + i));
    IcebergTestTables.append(rowGroups, null, features);
    IcebergTestTables.upsertAndDelete(rowGroups, null, List.of(point("g10", "Road", 20.01, 20, "g10-v2")),
        List.copyOf(ROW_GROUP_DELETED_IDS));
    IcebergTestTables.tag(rowGroups, "v1");
  }

  private static String rowGroupId(int i) {
    return String.format("g%02d", i);
  }

  @Test
  void iteratesAllPagesWithStableTokens() throws Exception {
    List<List<String>> pages = iterateAll("v2", 3);
    List<String> all = pages.stream().flatMap(List::stream).toList();
    assertThat(all).doesNotHaveDuplicates();
    assertThat(new HashSet<>(all)).isEqualTo(V2_IDS);
    assertThat(pages.size()).isGreaterThan(3);
    assertThat(pages).allSatisfy(page -> assertThat(page.size()).isLessThanOrEqualTo(3));

    assertThat(iterateAll("v2", 3)).isEqualTo(pages);
  }

  @Test
  void iteratesAFileWithManyRowGroups() throws Exception {
    Set<String> expected = new HashSet<>();
    for (int i = 0; i < ROW_GROUP_TABLE_ROWS; i++)
      expected.add(rowGroupId(i));
    expected.removeAll(ROW_GROUP_DELETED_IDS);

    List<List<String>> pages = iterateAll(ROW_GROUP_TABLE, "v1", 3);
    List<String> all = pages.stream().flatMap(List::stream).toList();
    assertThat(all).doesNotHaveDuplicates();
    assertThat(new HashSet<>(all)).isEqualTo(expected);
  }

  @Test
  void aDeepPageOnlyReadsFromItsRowGroup() {
    IcebergTables tables = rowGroupTables();
    SnapshotFiles files = rowGroupFiles(tables);
    int fileIndex = rowGroupFileIndex(files);
    FileScanTask task = files.tasks().get(fileIndex);
    assertThat(task.file().splitOffsets()).hasSizeGreaterThanOrEqualTo(ROW_GROUP_TABLE_ROWS / ROW_GROUP_ROWS);

    int lastRowGroup = task.file().splitOffsets().size() - 1;
    FeatureScan scan = new FeatureScan(tables, files)
        .withBudget(new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE))
        .withLimit(2)
        .withPaging(new FeatureScan.Position(fileIndex, lastRowGroup, 1));
    Result result = scan.run();
    assertThat(result.features()).hasSize(2);
    assertThat(result.next()).isEqualTo(new FeatureScan.Position(fileIndex, lastRowGroup, 3));
    //Only the rows of the last row group were decoded, not the whole file before the position
    assertThat(scan.stats().rowGroupsRead).isEqualTo(1);
    assertThat(scan.stats().rowsScanned).isLessThanOrEqualTo(ROW_GROUP_ROWS);
  }

  @Test
  void pagesEndAtRowGroupBoundariesWithinTheBudget() {
    IcebergTables tables = rowGroupTables();
    SnapshotFiles files = rowGroupFiles(tables);
    List<String> expected = new FeatureScan(tables, files)
        .withBudget(new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE))
        .withPaging(FeatureScan.Position.FIRST)
        .run()
        .features();
    long maxRowGroupBytes = new FeatureScan(tables, files).maxRowGroupBytes(Long.MAX_VALUE);
    assertThat(maxRowGroupBytes).isPositive();
    long deleteBytes = files.tasks().stream().flatMap(task -> task.deletes().stream())
        .mapToLong(delete -> delete.contentSizeInBytes() != null ? delete.contentSizeInBytes() : delete.fileSizeInBytes()).sum();
    //Room for two row groups (the footers are cached by the first scan)
    long maxBytes = deleteBytes + 2 * maxRowGroupBytes + maxRowGroupBytes / 2;

    List<String> all = new ArrayList<>();
    FeatureScan.Position position = FeatureScan.Position.FIRST;
    int pages = 0;
    do {
      ReadBudget budget = new ReadBudget(maxBytes, Long.MAX_VALUE);
      FeatureScan scan = new FeatureScan(tables, files).withBudget(budget).withLimit(1000).withPaging(position);
      Result page = scan.run();
      pages++;
      assertThat(budget.bytesRead()).as("bytes read by page " + pages).isLessThanOrEqualTo(maxBytes);
      assertThat(scan.stats().rowGroupsRead).isPositive();
      all.addAll(page.features());
      position = page.next();
      if (position != null)
        assertThat(position.rowOffset()).as("the page ends at a row group boundary").isZero();
      assertThat(pages).isLessThan(100);
    }
    while (position != null);
    assertThat(pages).isGreaterThan(2);
    assertThat(all).isEqualTo(expected);
  }

  @Test
  void anEmptyPageReadsOneRowGroupAboveTheBudget() {
    IcebergTables tables = rowGroupTables();
    SnapshotFiles files = rowGroupFiles(tables);
    FeatureScan scan = new FeatureScan(tables, files).withBudget(new ReadBudget(1, Long.MAX_VALUE)).withLimit(1000)
        .withPaging(FeatureScan.Position.FIRST);
    Result page = scan.run();
    assertThat(page.features()).isNotEmpty();
    assertThat(scan.stats().rowGroupsRead).isEqualTo(1);
    assertThat(page.next()).isNotNull();
    assertThat(page.next().rowOffset()).isZero();
  }

  @Test
  void aVersionWithARowGroupAboveTheBudgetIsNotIterated() throws Exception {
    Map<String, Object> smallBudget = connectorParams(Map.of("maxBytesPerRequest", 10));
    IterateFeaturesEvent<?> firstPage = locatedIterate(new IcebergLocation(NAMESPACE, ROW_GROUP_TABLE, "v1"));
    assertError(invoke(withConnectorParams(firstPage, smallBudget)), XyzError.NOT_IMPLEMENTED);

    //A started iteration continues, one row group per page
    IterateFeaturesEvent<?> event = locatedIterate(new IcebergLocation(NAMESPACE, ROW_GROUP_TABLE, "v1"));
    String token = featureCollection(invoke(withConnectorParams(event, connectorParams(Map.of())))).getNextPageToken();
    assertThat(token).isNotNull();
    IterateFeaturesEvent<?> nextPage = locatedIterate(new IcebergLocation(NAMESPACE, ROW_GROUP_TABLE, "v1"));
    nextPage.setNextPageToken(token);
    FeatureCollection page = featureCollection(invoke(withConnectorParams(nextPage, smallBudget)));
    assertThat(ids(page)).isNotEmpty().hasSizeLessThanOrEqualTo(ROW_GROUP_ROWS);
    assertThat(page.getNextPageToken()).isNotNull().endsWith(":0");
  }

  private static IcebergTables rowGroupTables() {
    return IcebergTables.forConfig(IcebergConnectorConfig.from(connectorParams(Map.of())));
  }

  private static SnapshotFiles rowGroupFiles(IcebergTables tables) {
    return tables.resolve(new IcebergLocation(NAMESPACE, ROW_GROUP_TABLE, "v1"));
  }

  private static int rowGroupFileIndex(SnapshotFiles files) {
    int fileIndex = -1;
    for (int i = 0; i < files.tasks().size(); i++)
      if (files.tasks().get(i).file().recordCount() == ROW_GROUP_TABLE_ROWS)
        fileIndex = i;
    assertThat(fileIndex).isNotNegative();
    return fileIndex;
  }

  @Test
  void iterateTokensHaveTheIcebergPrefix() throws Exception {
    FeatureCollection firstPage = featureCollection(invoke(iterate("v2", 3, null)));
    assertThat(firstPage.getNextPageToken()).startsWith(IcebergLocation.ITERATE_TOKEN_PREFIX);
    assertThat(firstPage.getHandle()).isEqualTo(firstPage.getNextPageToken());
  }

  @Test
  void rejectsATokenOfAnotherSnapshot() throws Exception {
    String token = featureCollection(invoke(iterate("v1", 3, null))).getNextPageToken();
    assertError(invoke(iterate("v2", 3, token)), XyzError.ILLEGAL_ARGUMENT);
    assertError(invoke(iterate("v2", 3, "ice:not-a-token")), XyzError.ILLEGAL_ARGUMENT);
    //The token format of a row position in the file
    assertError(invoke(iterate("v2", 3, token.substring(0, token.lastIndexOf(':')))), XyzError.ILLEGAL_ARGUMENT);
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void readsByBBox(boolean twoStep) throws Exception {
    assertThat(ids(invoke(reader(bbox("v2", new BBox(0, 0, 2, 2)), twoStep)))).containsExactlyInAnyOrder("p1", "p2", "l1", "p5");
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void readsByTileWithMargin(boolean twoStep) throws Exception {
    BBox tile = new BBox(0, 0, 1.9, 1.9);
    BBox tileWithMargin = new BBox(-0.15, -0.15, 2.05, 2.05);
    assertThat(ids(invoke(reader(tile("v2", tile, 0), twoStep)))).containsExactlyInAnyOrder("p1", "l1", "p5");
    assertThat(ids(invoke(reader(tile("v2", tileWithMargin, 20), twoStep)))).containsExactlyInAnyOrder("p1", "p2", "l1", "p5");
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void mvtTilesAreReadAsGeoJson(boolean twoStep) throws Exception {
    GetFeaturesByTileEvent event = tile("v2", new BBox(0, 0, 2, 2), 0).withResponseType(ResponseType.MVT).withClip(true);
    assertThat(ids(invoke(reader(event, twoStep)))).containsExactlyInAnyOrder("p1", "p2", "l1", "p5");
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void readsByIntersectingPolygon(boolean twoStep) throws Exception {
    Polygon polygon = new Polygon().withCoordinates(polygon(4, 4, 7, 7));
    assertThat(ids(invoke(reader(geometry("v2", polygon, 0), twoStep)))).containsExactlyInAnyOrder("p4", "poly1");
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void readsByRadiusAroundAPoint(boolean twoStep) throws Exception {
    Point point = new Point().withCoordinates(new PointCoordinates(10, 10));
    assertThat(ids(invoke(reader(geometry("v2", point, 50_000), twoStep)))).containsExactlyInAnyOrder("r1", "r3");
  }

  @Test
  void searchesByMomTypeValues() throws Exception {
    PropertiesQuery query = query(List.of(List.of(List.of("Road", "Building"))));
    assertThat(ids(invoke(search("v2", query))))
        .containsExactlyInAnyOrder("p1", "p2", "l1", "poly1", "n1", "r1", "r2", "far2", "p5");
  }

  @Test
  void searchesByMomTypeWithAndOr() throws Exception {
    PropertiesQuery or = query(List.of(List.of(List.of("Water")), List.of(List.of("Building"))));
    assertThat(ids(invoke(search("v2", or)))).containsExactlyInAnyOrder("r3", "far1", "p2", "poly1", "far2");

    PropertiesQuery and = query(List.of(List.of(List.of("Road", "Water"), List.of("Water"))));
    assertThat(ids(invoke(search("v2", and)))).containsExactlyInAnyOrder("r3", "far1");
  }

  @Test
  void filtersByConfiguredColumns() throws Exception {
    PropertiesQuery query = new PropertiesQuery();
    PropertyQueryList andList = new PropertyQueryList();
    andList.add(new PropertyQuery().withKey("kind").withOperation(EQUALS).withValues(new ArrayList<>(List.of("Water"))));
    query.add(andList);
    SearchForFeaturesEvent<?> event = search("v2", query);
    assertError(invoke(event), XyzError.NOT_IMPLEMENTED);
    withConnectorParams(event, connectorParams(Map.of("filterColumns", Map.of("kind", "mom_type"))));
    assertThat(ids(invoke(event))).containsExactlyInAnyOrder("r3", "far1");
    //Without filter columns, no property query is supported
    withConnectorParams(event, connectorParams(Map.of("filterColumns", Map.of())));
    assertError(invoke(event), XyzError.NOT_IMPLEMENTED);
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void combinesSpatialAndMomTypeFilters(boolean twoStep) throws Exception {
    GetFeaturesByBBoxEvent<?> event = bbox("v2", new BBox(0, 0, 2, 2)).withPropertiesQuery(query(List.of(List.of(List.of("Road")))));
    assertThat(ids(invoke(reader(event, twoStep)))).containsExactlyInAnyOrder("p1", "l1", "p5");
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void spatialReadsApplyDeletionVectors(boolean twoStep) throws Exception {
    FeatureCollection v1 = featureCollection(invoke(reader(bbox("v1", new BBox(-180, -90, 180, 90)), twoStep)));
    FeatureCollection v2 = featureCollection(invoke(reader(bbox("v2", new BBox(-180, -90, 180, 90)), twoStep)));
    Set<String> withoutGeometry = Set.of("n1");
    assertThat(ids(v1)).containsExactlyInAnyOrderElementsOf(V1_IDS.stream().filter(id -> !withoutGeometry.contains(id)).toList());
    assertThat(ids(v2)).containsExactlyInAnyOrderElementsOf(V2_IDS.stream().filter(id -> !withoutGeometry.contains(id)).toList());
    assertThat(names(v1, "p2")).containsExactly("p2-v1");
    assertThat(names(v2, "p2")).containsExactly("p2-v2");

    Set<String> expected = new HashSet<>();
    for (int i = 0; i < ROW_GROUP_TABLE_ROWS; i++)
      expected.add(rowGroupId(i));
    expected.removeAll(ROW_GROUP_DELETED_IDS);
    GetFeaturesByBBoxEvent<?> rowGroups = new GetFeaturesByBBoxEvent<>().withBbox(new BBox(19, 19, 21, 21));
    rowGroups.setParams(icebergParams(new IcebergLocation(NAMESPACE, ROW_GROUP_TABLE, "v1")));
    FeatureCollection rowGroupRead = featureCollection(invoke(reader(rowGroups, twoStep)));
    assertThat(ids(rowGroupRead)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(names(rowGroupRead, "g10")).containsExactly("g10-v2");
  }

  @Test
  void appliesDeletionVectors() throws Exception {
    FeatureCollection v1 = featureCollection(invoke(iterate("v1", 1000, null)));
    FeatureCollection v2 = featureCollection(invoke(iterate("v2", 1000, null)));
    assertThat(ids(v1)).containsExactlyInAnyOrderElementsOf(V1_IDS);
    assertThat(ids(v2)).containsExactlyInAnyOrderElementsOf(V2_IDS);
    assertThat(names(v1, "p2")).containsExactly("p2-v1");
    assertThat(names(v2, "p2")).containsExactly("p2-v2");
    assertThat(v1.getNextPageToken()).isNull();
  }

  /**
   * A table which references the data files and deletion vectors of another table at a tag, as the table of a composite space on
   * a pinned base version does, plus changes of its own: a delete in a data file which has a deletion vector already (merged into
   * a new deletion vector of the second table) and an update.
   */
  @Test
  void readsATableWhichSharesTheFilesOfAnother() throws Exception {
    Table base = catalog.loadTable(TableIdentifier.of(Namespace.of(NAMESPACE), TABLE));
    String sharedTable = TABLE + "_shared__b2";
    Table shared = IcebergTestTables.createTable(catalog, TableIdentifier.of(Namespace.of(NAMESPACE), sharedTable));
    assertThat(shared.schema().asStruct()).isEqualTo(base.schema().asStruct());
    IcebergTestTables.shareFiles(base, "v2", shared);
    //p1 is in the data file with the deletion vector of p2 and p3
    IcebergTestTables.upsertAndDelete(shared, null, List.of(point("r1", "Road", 10.3, 10, "r1-shared")), List.of("p1"));
    IcebergTestTables.tag(shared, "v5");
    Set<String> expected = new HashSet<>(V2_IDS);
    expected.remove("p1");
    IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>().withLimit(1000);
    event.setParams(icebergParams(new IcebergLocation(NAMESPACE, sharedTable, "v5")));
    FeatureCollection read = featureCollection(invoke(withConnectorParams(event, connectorParams(Map.of()))));
    assertThat(ids(read)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(names(read, "r1")).containsExactly("r1-shared");
    assertThat(names(read, "p2")).containsExactly("p2-v2");
    //The base is unchanged
    FeatureCollection baseRead = featureCollection(invoke(iterate("v2", 1000, null)));
    assertThat(ids(baseRead)).containsExactlyInAnyOrderElementsOf(V2_IDS);
    assertThat(names(baseRead, "r1")).containsExactly("r1");
    //One deletion vector per data file, the merged one in the second table's folder; its manifests are its own
    //The locations of a HadoopCatalog differ in the form of the file URI (file:/ or file:///)
    String baseFolder = path(base.location()) + "/";
    String sharedFolder = path(shared.location()) + "/";
    shared.refresh();
    int sharedDataFiles = 0;
    int mergedDeletionVectors = 0;
    try (var tasks = shared.newScan().useRef("v5").planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(task.deletes()).hasSizeLessThanOrEqualTo(1);
        if (path(task.file().location()).startsWith(baseFolder))
          sharedDataFiles++;
        //p2 and p3 of the base plus p1
        if (!task.deletes().isEmpty() && task.deletes().get(0).recordCount() == 3) {
          mergedDeletionVectors++;
          assertThat(path(task.file().location())).startsWith(baseFolder);
          assertThat(path(task.deletes().get(0).location())).startsWith(sharedFolder);
        }
      }
    }
    assertThat(sharedDataFiles).isGreaterThan(0);
    assertThat(mergedDeletionVectors).isEqualTo(1);
    assertThat(deletedRowsByFolder(shared, "v5", sharedFolder)).isEqualTo(4);
    assertThat(shared.snapshot("v5").allManifests(shared.io())).isNotEmpty()
        .allSatisfy(manifest -> assertThat(path(manifest.path())).startsWith(sharedFolder).doesNotStartWith(baseFolder));
  }

  /**
   * The number of rows deleted by the deletion vectors in the folder, at the ref
   */
  private static long deletedRowsByFolder(Table table, String ref, String folder) throws Exception {
    long rows = 0;
    Set<String> seen = new HashSet<>();
    try (var tasks = table.newScan().useRef(ref).planFiles()) {
      for (FileScanTask task : tasks)
        for (var delete : task.deletes())
          if (path(delete.location()).startsWith(folder) && seen.add(delete.location() + "#" + delete.contentOffset()))
            rows += delete.recordCount();
    }
    return rows;
  }

  private static String path(String location) {
    return URI.create(location).getPath();
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void readsABranchTag(boolean twoStep) throws Exception {
    Set<String> expected = new HashSet<>(V2_IDS);
    expected.add("b1");
    assertThat(ids(invoke(iterate(BRANCH_TAG, 1000, null)))).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(ids(invoke(reader(bbox(BRANCH_TAG, new BBox(1.1, 1.1, 1.3, 1.3)), twoStep)))).containsExactly("b1");
  }

  @Test
  void unknownRefIsNotFound() throws Exception {
    assertError(invoke(iterate("no_such_tag", 10, null)), XyzError.NOT_FOUND);
  }

  @Test
  void aBranchIsRejected() throws Exception {
    assertError(invoke(iterate(BRANCH, 10, null)), XyzError.ILLEGAL_ARGUMENT);
  }

  @Test
  void missingTablesAndRefsAreRemembered() throws Exception {
    Map<String, Object> rememberingParams = connectorParams(Map.of("missingCacheTtlSeconds", 3600));
    IcebergLocation v1 = new IcebergLocation(NAMESPACE, "late_table", "v1");
    IcebergLocation v2 = new IcebergLocation(NAMESPACE, "late_table", "v2");
    assertError(invoke(withConnectorParams(locatedIterate(v1), rememberingParams)), XyzError.NOT_FOUND);

    Table late = IcebergTestTables.createTable(catalog, TableIdentifier.of(Namespace.of(NAMESPACE), "late_table"));
    IcebergTestTables.append(late, null, List.of(point("x1", "Road", 1, 1, "x1")));
    IcebergTestTables.tag(late, "v1");
    //Still missing for the configuration which remembered it, found by another one
    assertError(invoke(withConnectorParams(locatedIterate(v1), rememberingParams)), XyzError.NOT_FOUND);
    assertThat(ids(invoke(withConnectorParams(locatedIterate(v1), connectorParams(Map.of()))))).containsExactly("x1");

    Map<String, Object> otherRememberingParams = connectorParams(Map.of("missingCacheTtlSeconds", 3601));
    assertError(invoke(withConnectorParams(locatedIterate(v2), otherRememberingParams)), XyzError.NOT_FOUND);
    IcebergTestTables.tag(late, "v2");
    assertError(invoke(withConnectorParams(locatedIterate(v2), otherRememberingParams)), XyzError.NOT_FOUND);
  }

  private static IterateFeaturesEvent<?> locatedIterate(IcebergLocation location) {
    IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>().withLimit(10);
    event.setParams(icebergParams(location));
    return event;
  }

  @Test
  void unknownTableIsNotFound() throws Exception {
    IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>()
        .withParams(icebergParams(new IcebergLocation(NAMESPACE, "no_such_table", "v1")));
    assertError(invoke(withConnectorParams(event, connectorParams(Map.of()))), XyzError.NOT_FOUND);
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void stopsAtTheByteLimit(boolean twoStep) throws Exception {
    Event<?> event = withConnectorParams(bbox("v2", new BBox(-180, -90, 180, 90)), connectorParams(Map.of("maxBytesPerRequest", 10,
        "twoStepReads", twoStep)));
    assertTooLarge(invoke(event), IcebergLocation.LIMIT_READ_BYTES, 10);
  }

  @Test
  void theEventCanLowerTheResponseLimit() throws Exception {
    IterateFeaturesEvent<?> event = iterate("v2", 1000, null);
    event.setParams(Map.of(IcebergLocation.EVENT_PARAM, Map.of(IcebergLocation.EVENT_PARAM_LOCATION,
        new IcebergLocation(NAMESPACE, TABLE, "v2").toString(), IcebergLocation.EVENT_PARAM_MAX_RESPONSE_BYTES, 10,
        IcebergLocation.EVENT_PARAM_COUNT_MATCHES, true)));
    assertTooLarge(invoke(event), IcebergLocation.LIMIT_RESPONSE_BYTES, 10, true);
  }

  /**
   * With countMatches, a page with fewer matches than its limit has all its matches (complete), one with more may be cut off by its
   * limit
   */
  @ParameterizedTest(name = "limit={0}")
  @ValueSource(ints = {1000, 2})
  void stopsAtTheResponseLimit(int limit) throws Exception {
    Event<?> event = withConnectorParams(countingMatches(iterate("v2", limit, null)), connectorParams(Map.of("maxResponseBytes", 10)));
    assertTooLarge(invoke(event), IcebergLocation.LIMIT_RESPONSE_BYTES, 10, limit > 2);
  }

  /**
   * With countMatches, after the response limit, the scan counts the matches up to the limit of the read: complete if there are
   * fewer, as then the space's own connector has the same features. Exactly as many matches as the limit count as cut off.
   */
  @ParameterizedTest(name = "twoStepReads={0} limit={1}")
  @CsvSource({"true, 1000", "false, 1000", "true, 2", "false, 2", "true, 11", "false, 11"})
  void spatialReadsStopAtTheResponseLimit(boolean twoStep, long limit) throws Exception {
    BBox world = new BBox(-180, -90, 180, 90);
    long matches = ids(invoke(reader(bbox("v2", world), twoStep))).size();
    assertThat(matches).isEqualTo(11);
    Event<?> event = withConnectorParams(countingMatches(bbox("v2", world).withLimit(limit)), connectorParams(Map.of(
        "maxResponseBytes", 10, "twoStepReads", twoStep)));
    assertTooLarge(invoke(event), IcebergLocation.LIMIT_RESPONSE_BYTES, 10, limit > matches);
  }

  /**
   * Without countMatches (the Hub's shadow reads), a read fails fast at the response limit: the error has no detail complete, so the
   * Hub would not take it as final.
   */
  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void withoutCountingTheMatchesAReadFailsFastAtTheResponseLimit(boolean twoStep) throws Exception {
    Event<?> event = withConnectorParams(bbox("v2", new BBox(-180, -90, 180, 90)).withLimit(1000), connectorParams(Map.of(
        "maxResponseBytes", 10, "twoStepReads", twoStep)));
    XyzResponse response = invoke(event);
    assertTooLarge(response, IcebergLocation.LIMIT_RESPONSE_BYTES, 10);
    assertThat(((ErrorResponse) response).getErrorDetails()).doesNotContainKey(IcebergLocation.ERROR_DETAIL_COMPLETE);
  }

  /**
   * A scan without countMatches stops at the first feature over the response limit, one with it goes on to count the matches up to
   * the limit of the read.
   */
  @ParameterizedTest(name = "countMatches={0}")
  @ValueSource(booleans = {false, true})
  void aScanCountsTheMatchesAfterTheResponseLimitOnlyIfAsked(boolean countMatches) {
    IcebergTables tables = rowGroupTables();
    SnapshotFiles files = rowGroupFiles(tables);
    int matches = new FeatureScan(tables, files).withBudget(new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE))
        .withPaging(FeatureScan.Position.FIRST).run().features().size();
    assertThat(matches).isGreaterThan(1);
    FeatureScan scan = new FeatureScan(tables, files).withBudget(new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE)).withLimit(1000)
        .withPaging(FeatureScan.Position.FIRST).withMaxResponseBytes(10).withCountMatches(countMatches);
    assertThatThrownBy(scan::run).isInstanceOfSatisfying(ScanException.class, e -> {
      assertThat(e.error()).isEqualTo(XyzError.PAYLOAD_TO_LARGE);
      assertThat(e.details()).containsEntry(IcebergLocation.ERROR_DETAIL_LIMIT, IcebergLocation.LIMIT_RESPONSE_BYTES);
      if (countMatches)
        assertThat(e.details()).containsEntry(IcebergLocation.ERROR_DETAIL_COMPLETE, true);
      else
        assertThat(e.details()).doesNotContainKey(IcebergLocation.ERROR_DETAIL_COMPLETE);
    });
    //The first feature is larger than 10 characters
    assertThat(scan.stats().rowsMatched).isEqualTo(countMatches ? matches : 1);
  }

  @Test
  void stopsAtTheTimeout() throws Exception {
    Event<?> event = withConnectorParams(iterate("v2", 1000, null), connectorParams(Map.of("requestTimeoutMs", 0)));
    assertError(invoke(event), XyzError.TIMEOUT);
  }

  @ParameterizedTest(name = "twoStepReads={0}")
  @ValueSource(booleans = {true, false})
  void spatialReadsStopAtTheTimeout(boolean twoStep) throws Exception {
    Event<?> event = withConnectorParams(bbox("v2", new BBox(-180, -90, 180, 90)), connectorParams(Map.of("requestTimeoutMs", 0,
        "twoStepReads", twoStep)));
    assertError(invoke(event), XyzError.TIMEOUT);
  }

  @Test
  void unsupportedEventIsNotImplemented() throws Exception {
    GetFeaturesByIdEvent event = new GetFeaturesByIdEvent().withIds(List.of("p1"))
        .withParams(icebergParams(new IcebergLocation(NAMESPACE, TABLE, "v2")));
    assertError(invoke(withConnectorParams(event, connectorParams(Map.of()))), XyzError.NOT_IMPLEMENTED);
  }

  @Test
  void unsupportedOptionsAreNotImplemented() throws Exception {
    assertError(invoke(bbox("v2", new BBox(0, 0, 2, 2)).withClip(true)), XyzError.NOT_IMPLEMENTED);
    assertError(invoke(tile("v2", new BBox(0, 0, 2, 2), 0).withClip(true)), XyzError.NOT_IMPLEMENTED);
    assertError(invoke(bbox("v2", new BBox(0, 0, 2, 2)).withSelection(List.of("p.name"))), XyzError.NOT_IMPLEMENTED);
    assertError(invoke(bbox("v2", new BBox(0, 0, 2, 2)).withClusteringType("hexbin")), XyzError.NOT_IMPLEMENTED);
    assertError(invoke(bbox("v2", new BBox(0, 0, 2, 2)).withOptimizationMode("viz")), XyzError.NOT_IMPLEMENTED);
    assertError(invoke(iterate("v2", 10, null).withStartI(0).withEndI(10)), XyzError.NOT_IMPLEMENTED);

    PropertiesQuery otherProperty = new PropertiesQuery();
    PropertyQueryList andList = new PropertyQueryList();
    andList.add(new PropertyQuery().withKey("properties.name").withOperation(EQUALS).withValues(List.of("p1")));
    otherProperty.add(andList);
    assertError(invoke(search("v2", otherProperty)), XyzError.NOT_IMPLEMENTED);
  }

  @Test
  void missingLocationIsIllegalArgument() throws Exception {
    assertError(invoke(withConnectorParams(new IterateFeaturesEvent<>(), connectorParams(Map.of()))), XyzError.ILLEGAL_ARGUMENT);
  }

  @Test
  void resolvesPlaceholdersFromTheEnvironment() throws Exception {
    Map<String, Object> params = connectorParams(Map.of());
    params.put("catalogProperties", Map.of("type", "hadoop", "warehouse", "${ICEBERG_TEST_WAREHOUSE}"));
    Event<?> event = withConnectorParams(iterate("v2", 1000, null), params);

    assertThat(ids(invoke(event, Map.of("ICEBERG_TEST_WAREHOUSE", warehouse)))).containsExactlyInAnyOrderElementsOf(V2_IDS);
    assertError(invoke(event, Map.of()), XyzError.EXCEPTION);
  }

  private static List<List<String>> iterateAll(String ref, int limit) throws Exception {
    return iterateAll(TABLE, ref, limit);
  }

  private static List<List<String>> iterateAll(String table, String ref, int limit) throws Exception {
    List<List<String>> pages = new ArrayList<>();
    String token = null;
    do {
      IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>().withLimit(limit).withNextPageToken(token);
      event.setParams(icebergParams(new IcebergLocation(NAMESPACE, table, ref)));
      FeatureCollection page = featureCollection(invoke(withConnectorParams(event, connectorParams(Map.of()))));
      pages.add(ids(page));
      token = page.getNextPageToken();
      assertThat(pages.size()).isLessThan(100);
    }
    while (token != null);
    return pages;
  }

  private static IterateFeaturesEvent<?> iterate(String ref, int limit, String token) {
    IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>().withLimit(limit).withNextPageToken(token);
    return prepare(event, ref);
  }

  private static GetFeaturesByBBoxEvent<?> bbox(String ref, BBox bbox) {
    return prepare(new GetFeaturesByBBoxEvent<>().withBbox(bbox), ref);
  }

  private static GetFeaturesByTileEvent tile(String ref, BBox bbox, int margin) {
    return prepare(new GetFeaturesByTileEvent().withBbox(bbox).withMargin(margin).withResponseType(ResponseType.GEO_JSON), ref);
  }

  private static GetFeaturesByGeometryEvent geometry(String ref, com.here.xyz.models.geojson.implementation.Geometry geometry,
      int radius) {
    return prepare(new GetFeaturesByGeometryEvent().withGeometry(geometry).withRadius(radius), ref);
  }

  private static SearchForFeaturesEvent<?> search(String ref, PropertiesQuery query) {
    return prepare(new SearchForFeaturesEvent<>().withPropertiesQuery(query), ref);
  }

  private static <T extends Event<?>> T prepare(T event, String ref) {
    event.setParams(icebergParams(new IcebergLocation(NAMESPACE, TABLE, ref)));
    return withConnectorParams(event, connectorParams(Map.of()));
  }

  /**
   * Selects the reader of a spatial read: the two-step page-index reader or the row-group reader.
   */
  private static <T extends Event<?>> T reader(T event, boolean twoStep) {
    return withConnectorParams(event, connectorParams(Map.of("twoStepReads", twoStep)));
  }

  private static <T extends Event<?>> T withConnectorParams(T event, Map<String, Object> connectorParams) {
    event.setConnectorParams(connectorParams);
    event.setStreamId("iceberg-test");
    event.setSpace("test-space");
    return event;
  }

  /**
   * Adds countMatches to the Iceberg params of the event, as the Hub does for routed reads
   */
  @SuppressWarnings("unchecked")
  private static <T extends Event<?>> T countingMatches(T event) {
    Map<String, Object> icebergParams = new HashMap<>((Map<String, Object>) event.getParams().get(IcebergLocation.EVENT_PARAM));
    icebergParams.put(IcebergLocation.EVENT_PARAM_COUNT_MATCHES, true);
    event.setParams(Map.of(IcebergLocation.EVENT_PARAM, icebergParams));
    return event;
  }

  private static Map<String, Object> icebergParams(IcebergLocation location) {
    return Map.of(IcebergLocation.EVENT_PARAM, Map.of(IcebergLocation.EVENT_PARAM_LOCATION, location.toString()));
  }

  private static Map<String, Object> connectorParams(Map<String, Object> overrides) {
    Map<String, Object> params = new HashMap<>();
    params.put("connectorId", "iceberg");
    params.put("className", IcebergConnector.class.getName());
    params.put("catalogName", "test");
    params.put("catalogProperties", Map.of("type", "hadoop", "warehouse", warehouse));
    params.putAll(overrides);
    return params;
  }

  private static PropertiesQuery query(List<List<List<String>>> orOfAndOfValues) {
    PropertiesQuery query = new PropertiesQuery();
    for (List<List<String>> and : orOfAndOfValues) {
      PropertyQueryList andList = new PropertyQueryList();
      for (List<String> values : and)
        andList.add(new PropertyQuery().withKey("momType").withOperation(EQUALS).withValues(new ArrayList<>(values)));
      query.add(andList);
    }
    return query;
  }

  private static XyzResponse invoke(Event<?> event) throws Exception {
    return invoke(event, Map.of());
  }

  private static XyzResponse invoke(Event<?> event, Map<String, String> env) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new IcebergConnector().handleRequest(new ByteArrayInputStream(event.serialize().getBytes(UTF_8)), out,
        new SimulatedContext("iceberg-test", env));
    return XyzSerializable.deserialize(out.toByteArray());
  }

  private static FeatureCollection featureCollection(XyzResponse response) {
    if (response instanceof ErrorResponse error)
      throw new AssertionError("Unexpected error response: " + error.getError() + " " + error.getErrorMessage());
    assertThat(response).isInstanceOf(FeatureCollection.class);
    return (FeatureCollection) response;
  }

  private static List<String> ids(XyzResponse response) throws Exception {
    return ids(featureCollection(response));
  }

  private static List<String> ids(FeatureCollection collection) throws Exception {
    return collection.getFeatures().stream().map(Feature::getId).toList();
  }

  private static List<String> names(FeatureCollection collection, String id) throws Exception {
    return collection.getFeatures().stream().filter(f -> id.equals(f.getId())).map(f -> (String) f.getProperties().get("name"))
        .toList();
  }

  private static void assertError(XyzResponse response, XyzError expected) {
    assertThat(response).isInstanceOf(ErrorResponse.class);
    assertThat(((ErrorResponse) response).getError()).as(((ErrorResponse) response).getErrorMessage()).isEqualTo(expected);
  }

  /**
   * A PAYLOAD_TO_LARGE whose errorDetails name the limit which was hit, so that the Hub can tell them apart
   */
  static void assertTooLarge(XyzResponse response, String limit, long maxBytes) {
    assertError(response, XyzError.PAYLOAD_TO_LARGE);
    Map<String, Object> details = ((ErrorResponse) response).getErrorDetails();
    assertThat(details).containsEntry(IcebergLocation.ERROR_DETAIL_LIMIT, limit);
    assertThat(((Number) details.get(IcebergLocation.ERROR_DETAIL_MAX_BYTES)).longValue()).isEqualTo(maxBytes);
  }

  /**
   * A PAYLOAD_TO_LARGE of the response limit, with whether the read has all its matches
   */
  static void assertTooLarge(XyzResponse response, String limit, long maxBytes, boolean complete) {
    assertTooLarge(response, limit, maxBytes);
    assertThat(((ErrorResponse) response).getErrorDetails()).containsEntry(IcebergLocation.ERROR_DETAIL_COMPLETE, complete);
  }

  private static PolygonCoordinates polygon(double minX, double minY, double maxX, double maxY) {
    LinearRingCoordinates ring = new LinearRingCoordinates();
    ring.add(new Position(minX, minY));
    ring.add(new Position(maxX, minY));
    ring.add(new Position(maxX, maxY));
    ring.add(new Position(minX, maxY));
    ring.add(new Position(minX, minY));
    PolygonCoordinates coordinates = new PolygonCoordinates();
    coordinates.add(ring);
    return coordinates;
  }

  private static String point(String id, String momType, double lon, double lat, String name) {
    return feature(id, momType, "{\"type\":\"Point\",\"coordinates\":[" + lon + "," + lat + "]}", name);
  }

  private static String feature(String id, String momType, String geometry, String name) {
    return "{\"type\":\"Feature\",\"id\":\"" + id + "\"" + (momType == null ? "" : ",\"momType\":\"" + momType + "\"")
        + ",\"geometry\":" + geometry + ",\"properties\":{\"name\":\"" + name + "\",\"@ns:com:here:xyz\":{\"version\":1,"
        + "\"createdAt\":1700000000000,\"updatedAt\":1700000000000,\"author\":\"tester\"}}}";
  }
}
