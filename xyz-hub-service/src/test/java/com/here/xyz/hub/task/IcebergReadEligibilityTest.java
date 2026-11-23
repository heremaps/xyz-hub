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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.task.IcebergReadTestSupport.BASE_ID;
import static com.here.xyz.hub.task.IcebergReadTestSupport.SPACE_ID;
import static com.here.xyz.hub.task.IcebergReadTestSupport.composite;
import static com.here.xyz.hub.task.IcebergReadTestSupport.equalsQuery;
import static com.here.xyz.hub.task.IcebergReadTestSupport.momTypeQuery;
import static com.here.xyz.hub.task.IcebergReadTestSupport.routingContext;
import static com.here.xyz.hub.task.IcebergReadTestSupport.space;
import static org.assertj.core.api.Assertions.assertThat;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.IcebergVersionCache.IcebergVersion;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.FeatureTask.BBoxQuery;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.hub.task.FeatureTask.IdsQuery;
import com.here.xyz.hub.task.FeatureTask.IterateQuery;
import com.here.xyz.hub.task.FeatureTask.SearchQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery;
import com.here.xyz.iceberg.IcebergConnectorConfig;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.models.hub.Space.ListenerConnectorRef;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IcebergReadEligibilityTest {

  private Space space = space(SPACE_ID);
  private Config previousConfiguration;

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    Service.configuration = new Config();
  }

  @AfterEach
  void tearDown() {
    Service.configuration = previousConfiguration;
  }

  @Test
  void supportedReadsAreEligible() {
    assertThat(reason(geometryTask(geometryEvent()))).isNull();
    assertThat(reason(geometryTask(geometryEvent().withRadius(100)))).isNull();
    assertThat(reason(bboxTask(bboxEvent()))).isNull();
    assertThat(reason(bboxTask(bboxEvent().withOptimizationMode("raw")))).isNull();
    assertThat(reason(tileTask(tileEvent(ResponseType.GEO_JSON)))).isNull();
    assertThat(reason(tileTask(tileEvent(ResponseType.GEO_JSON).withHereTileFlag(true)))).isNull();
    assertThat(reason(iterateTask(new IterateFeaturesEvent()))).isNull();
    assertThat(reason(searchTask(new SearchForFeaturesEvent().withPropertiesQuery(momTypeQuery("a", "b"))))).isNull();
  }

  @Test
  void mvtTilesAreNotEligible() {
    assertThat(reason(tileTask(tileEvent(ResponseType.MVT).withClip(true)))).contains("MVT");
    assertThat(reason(tileTask(tileEvent(ResponseType.MVT_FLATTENED).withClip(true)))).contains("MVT");
    assertThat(reason(tileTask(tileEvent(ResponseType.MVT).withHereTileFlag(true)))).contains("MVT");
    assertThat(reason(tileTask(tileEvent(ResponseType.MVT_FLATTENED).withHereTileFlag(true)))).contains("MVT");
  }

  @Test
  void binaryTilesAndGeoJsonTilesWithClipAreNotEligible() {
    assertThat(reason(tileTask(tileEvent(ResponseType.BINARY)))).contains("BINARY");
    assertThat(reason(tileTask(tileEvent(ResponseType.GEO_JSON).withClip(true)))).isEqualTo("clip");
  }

  @Test
  void spatialOptionsAreNotEligible() {
    assertThat(reason(geometryTask(new GetFeaturesByGeometryEvent().withH3Index("8a1fb46622dffff")))).isEqualTo("no geometry");
    assertThat(reason(geometryTask(geometryEvent().withH3Index("8a1fb46622dffff")))).isEqualTo("h3Index");
    assertThat(reason(geometryTask(geometryEvent().withClip(true)))).isEqualTo("clip");
    assertThat(reason(bboxTask(bboxEvent().withClip(true)))).isEqualTo("clip");
    assertThat(reason(bboxTask(new GetFeaturesByBBoxEvent<>()))).isEqualTo("no bbox");
    assertThat(reason(bboxTask(new GetFeaturesByBBoxEvent<>().withBbox(new BBox(170, 0, -170, 10))))).contains("antimeridian");
  }

  @Test
  void clusteringTweaksAndVizAreNotEligible() {
    assertThat(reason(bboxTask(bboxEvent().withClusteringType("hexbin")))).isEqualTo("clustering");
    assertThat(reason(bboxTask(bboxEvent().withTweakType("sampling")))).isEqualTo("tweaks");
    assertThat(reason(bboxTask(bboxEvent().withOptimizationMode("viz")))).contains("optimizationMode");
    assertThat(reason(tileTask(tileEvent(ResponseType.GEO_JSON).withClusteringType("quadbin")))).isEqualTo("clustering");
    assertThat(reason(tileTask(tileEvent(ResponseType.GEO_JSON).withTweakType("simplification")))).isEqualTo("tweaks");
    assertThat(reason(tileTask(tileEvent(ResponseType.MVT).withOptimizationMode("viz").withVizSampling("high"))))
        .contains("optimizationMode");
  }

  @Test
  void selectionAndForce2DAreNotEligible() {
    assertThat(reason(bboxTask(bboxEvent().withSelection(List.of("p.name"))))).isEqualTo("selection");
    assertThat(reason(bboxTask(bboxEvent().withSelection(List.of())))).isNull();
    assertThat(reason(iterateTask(new IterateFeaturesEvent<IterateFeaturesEvent>().withForce2D(true)))).isEqualTo("force2D");
  }

  @Test
  void onlyRootMomTypeEqualsWithStringValuesIsEligible() {
    assertThat(reason(bboxTask(bboxEvent().withPropertiesQuery(momTypeQuery("a"))))).isNull();
    //An OR of momType values and an AND of momType conditions
    PropertiesQuery orAnd = equalsQuery(List.of(List.<Object[]>of(new Object[]{"momType", "a"}, new Object[]{"momType", "a", "b"}),
        List.<Object[]>of(new Object[]{"momType", "c"})));
    assertThat(reason(bboxTask(bboxEvent().withPropertiesQuery(orAnd)))).isNull();

    //p.momType is converted to properties.momType
    PropertiesQuery propertiesMomType = equalsQuery(List.of(List.<Object[]>of(new Object[]{"properties.momType", "a"})));
    assertThat(reason(bboxTask(bboxEvent().withPropertiesQuery(propertiesMomType)))).contains("f.momType");
    PropertiesQuery otherKey = equalsQuery(List.of(List.<Object[]>of(new Object[]{"momType", "a"}, new Object[]{"id", "x"})));
    assertThat(reason(iterateTask(new IterateFeaturesEvent<IterateFeaturesEvent>().withPropertiesQuery(otherKey)))).contains("f.momType");
    PropertiesQuery nonString = equalsQuery(List.of(List.<Object[]>of(new Object[]{"momType", 1L})));
    assertThat(reason(geometryTask(geometryEvent().withPropertiesQuery(nonString)))).contains("f.momType");
    assertThat(reason(tileTask(tileEvent(ResponseType.GEO_JSON).withPropertiesQuery(notEquals())))).contains("f.momType");
  }

  @Test
  void aSearchNeedsAMomTypeQuery() {
    assertThat(reason(searchTask(new SearchForFeaturesEvent()))).isEqualTo("search without a properties query");
    assertThat(reason(searchTask(new SearchForFeaturesEvent().withPropertiesQuery(new PropertiesQuery()))))
        .isEqualTo("search without a properties query");
    assertThat(reason(searchTask(new SearchForFeaturesEvent().withPropertiesQuery(notEquals())))).contains("f.momType");
  }

  @Test
  void partitionedIteratesAreNotEligible() {
    assertThat(reason(iterateTask(new IterateFeaturesEvent().withStartI(0).withEndI(10)))).isEqualTo("partitioned iterate");
    assertThat(reason(iterateTask(new IterateFeaturesEvent().withStartI(0)))).isEqualTo("partitioned iterate");
  }

  @Test
  void spacesWithProcessorsOrListenersAreNotEligible() {
    space.setProcessors(Map.of("processor", List.of(new ListenerConnectorRef())));
    assertThat(reason(bboxTask(bboxEvent()))).isEqualTo("processors or listeners");

    space = space(SPACE_ID);
    space.setListeners(Map.of("listener", List.of(new ListenerConnectorRef())));
    assertThat(reason(bboxTask(bboxEvent()))).isEqualTo("processors or listeners");

    space = space(SPACE_ID);
    space.setProcessors(Map.of());
    space.setListeners(Map.of());
    assertThat(reason(bboxTask(bboxEvent()))).isNull();
  }

  @Test
  void onlyTheDefaultContextOfTheRequestedSpaceIsEligible() {
    assertThat(reason(bboxTask(bboxEvent().withContext(SpaceContext.EXTENSION)))).isEqualTo("context EXTENSION");
    assertThat(reason(bboxTask(bboxEvent().withContext(SpaceContext.SUPER)))).isEqualTo("context SUPER");

    //A request with context SUPER on the composite was switched to the base with context DEFAULT
    space = space(BASE_ID);
    assertThat(reason(bboxTask(bboxEvent()))).isEqualTo("resolved space differs from the requested space");
  }

  @Test
  void otherTasksAreNotEligible() {
    IdsQuery ids = new IdsQuery(new GetFeaturesByIdEvent().withIds(List.of("a")), routingContext(new HashMap<>(), SPACE_ID),
        ApiResponseType.FEATURE_COLLECTION, false);
    ids.space = space;
    assertThat(IcebergReadEligibility.reasonNotEligible(ids)).contains("IdsQuery");
  }

  @Test
  void compositesNeedExactlyOnePinnedBase() {
    space = composite(5L);
    assertThat(reason(bboxTask(bboxEvent()), List.of(space(BASE_ID)))).isNull();

    //An unpinned composite, also on a read-only base
    space = composite(null);
    Space readOnlyBase = space(BASE_ID);
    readOnlyBase.setReadOnly(true);
    readOnlyBase.setReadOnlyHeadVersion(4);
    assertThat(reason(bboxTask(bboxEvent()), List.of(readOnlyBase))).isEqualTo("unpinned composite");

    //A composite of composites
    space = composite(5L);
    assertThat(reason(bboxTask(bboxEvent()), List.of(space(BASE_ID), space("iceberg-read-test-base-of-base")))).contains("2 extended");
  }

  @Test
  void theCompositeFilterAcceptsOnlyTagsOfThePinnedBase() {
    Predicate<IcebergVersion> plain = IcebergReadEligibility.compositeFilter(bboxTask(bboxEvent()), 7);
    assertThat(plain.test(version("v7"))).isTrue();
    assertThat(plain.test(version("ext_t_b4_v7"))).isTrue();

    space = composite(5L);
    Predicate<IcebergVersion> pinned = IcebergReadEligibility.compositeFilter(bboxTask(bboxEvent()), 7);
    assertThat(pinned.test(version("ext_t_b5_v7"))).isTrue();
    assertThat(pinned.test(version("ext_t_b4_v7"))).isFalse();
    assertThat(pinned.test(version("ext_t_b5_v8"))).isFalse();
    assertThat(pinned.test(version("ext_t_b15_v7"))).isFalse();
    assertThat(pinned.test(version("v7"))).isFalse();
  }

  @Test
  void theFilterPropertiesComeFromTheConnector() {
    Connector connector = new Connector();
    assertThat(IcebergReadEligibility.filterProperties(connector)).containsExactly("momType");
    connector.params = new HashMap<>(Map.of("filterColumns", Map.of("kind", "kind")));
    assertThat(IcebergReadEligibility.filterProperties(connector)).containsExactly("kind");
    connector.params = new HashMap<>(Map.of("filterColumns", "invalid"));
    assertThat(IcebergReadEligibility.filterProperties(connector)).isEmpty();

    PropertiesQuery kind = equalsQuery(List.of(List.<Object[]>of(new Object[]{"kind", "a"})));
    FeatureTask<?, ?> task = bboxTask(bboxEvent().withPropertiesQuery(kind));
    //Without the connector's filter properties, only the operations and values are checked
    assertThat(IcebergReadEligibility.reasonNotEligible(task)).isNull();
    assertThat(IcebergReadEligibility.reasonNotEligible(task, Set.of("kind"))).isNull();
    assertThat(IcebergReadEligibility.reasonNotEligible(task, Set.of("momType"))).isEqualTo("properties query other than f.momType");
    assertThat(IcebergReadEligibility.reasonNotEligible(task, Set.of())).contains("no filterColumns");
    assertThat(IcebergReadEligibility.reasonNotEligible(bboxTask(bboxEvent().withPropertiesQuery(notEquals())))).isNotNull();
  }

  @Test
  void theCompositeTagSuffixIsConfigurable() {
    space = composite(5L);
    Service.configuration.ICEBERG_READS_COMPOSITE_TAG_SUFFIX = "@{baseVersion}-{version}";
    Predicate<IcebergVersion> pinned = IcebergReadEligibility.compositeFilter(bboxTask(bboxEvent()), 7);
    assertThat(pinned.test(version("t@5-7"))).isTrue();
    assertThat(pinned.test(version("ext_t_b5_v7"))).isFalse();

    Service.configuration.ICEBERG_READS_COMPOSITE_TAG_SUFFIX = "";
    assertThat(reason(bboxTask(bboxEvent()), List.of(space(BASE_ID)))).contains("ICEBERG_READS_COMPOSITE_TAG_SUFFIX");
  }

  private String reason(FeatureTask<?, ?> task) {
    return IcebergReadEligibility.reasonNotEligible(task, IcebergConnectorConfig.DEFAULT_FILTER_COLUMNS.keySet());
  }

  private String reason(FeatureTask<?, ?> task, List<Space> extendedSpaces) {
    task.extendedSpaces = extendedSpaces;
    return IcebergReadEligibility.reasonNotEligible(task, IcebergConnectorConfig.DEFAULT_FILTER_COLUMNS.keySet());
  }

  private static IcebergVersion version(String tag) {
    return new IcebergVersion(7, new IcebergLocation("ns", "table", tag), null, 1, UUID.randomUUID());
  }

  private static PropertiesQuery notEquals() {
    PropertiesQuery query = new PropertiesQuery();
    PropertyQueryList andList = new PropertyQueryList();
    andList.add(new PropertyQuery().withKey("momType").withOperation(QueryOperation.NOT_EQUALS).withValues(List.of("a")));
    query.add(andList);
    return query;
  }

  private static GetFeaturesByGeometryEvent geometryEvent() {
    return new GetFeaturesByGeometryEvent().withGeometry(new Point().withCoordinates(new PointCoordinates(8.0, 50.0)));
  }

  private static GetFeaturesByBBoxEvent<?> bboxEvent() {
    return new GetFeaturesByBBoxEvent<>().withBbox(new BBox(8.0, 50.0, 8.1, 50.1));
  }

  private static GetFeaturesByTileEvent tileEvent(ResponseType responseType) {
    GetFeaturesByTileEvent event = new GetFeaturesByTileEvent().withLevel(12).withX(2138).withY(1388).withResponseType(responseType);
    event.setBbox(new BBox(8.0, 50.0, 8.1, 50.1));
    return event;
  }

  private <X extends FeatureTask<?, X>> X withSpace(X task) {
    task.space = space;
    return task;
  }

  private GeometryQuery geometryTask(GetFeaturesByGeometryEvent event) {
    return withSpace(new GeometryQuery(event, context(), ApiResponseType.FEATURE_COLLECTION, false, null, null));
  }

  private BBoxQuery bboxTask(GetFeaturesByBBoxEvent<?> event) {
    return withSpace(new BBoxQuery(event, context(), ApiResponseType.FEATURE_COLLECTION, false));
  }

  private TileQuery tileTask(GetFeaturesByTileEvent event) {
    ApiResponseType responseType = event.getResponseType() == ResponseType.MVT ? ApiResponseType.MVT
        : event.getResponseType() == ResponseType.MVT_FLATTENED ? ApiResponseType.MVT_FLATTENED : ApiResponseType.FEATURE_COLLECTION;
    return withSpace(new TileQuery(event, context(), responseType, false));
  }

  private IterateQuery iterateTask(IterateFeaturesEvent event) {
    return withSpace(new IterateQuery(event, context(), ApiResponseType.FEATURE_COLLECTION, false));
  }

  private SearchQuery searchTask(SearchForFeaturesEvent<?> event) {
    return withSpace(new SearchQuery(event, context(), ApiResponseType.FEATURE_COLLECTION, false));
  }

  private static io.vertx.ext.web.RoutingContext context() {
    return routingContext(new HashMap<>(), SPACE_ID);
  }
}
