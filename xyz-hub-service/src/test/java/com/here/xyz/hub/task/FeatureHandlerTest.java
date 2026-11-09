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

import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.LazySpaceTables.ON_DEMAND_INDEXES;
import static org.assertj.core.api.Assertions.assertThat;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.util.service.HttpException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FeatureHandlerTest {

  private static final Map<String, Boolean> OWN_PROPERTIES = Map.of("own", true);
  private static final Map<String, Boolean> BASE_PROPERTIES = Map.of("base", true);
  private static final Map<String, Boolean> ROOT_PROPERTIES = Map.of("root", true);

  @Test
  void writesOfALazySpaceCarryItsActivatedSearchableProperties() {
    Map<String, Boolean> properties = new LinkedHashMap<>();
    properties.put("a", true);
    properties.put("b", false);
    properties.put("c.d", true);
    properties.put("e", null);

    Map<String, Object> params = writeParams(space("lazy", "psql", true, properties));

    assertThat(params).containsEntry(ON_DEMAND_INDEXES, List.of("a", "c.d")).containsEntry(LAZY_TABLE, true);
  }

  @Test
  void writesOfALazySpaceWithoutSearchablePropertiesCarryAnEmptyList() {
    assertThat(writeParams(space("lazy", "psql", true, null))).containsEntry(ON_DEMAND_INDEXES, List.of());
  }

  @Test
  void writesOfASpaceWithoutLazyLevelCarryNoOnDemandIndexes() {
    assertThat(writeParams(space("eager", "psql", false, OWN_PROPERTIES))).doesNotContainKey(ON_DEMAND_INDEXES);
  }

  @Test
  void otherEventsOfALazySpaceCarryNoOnDemandIndexes() {
    Map<String, Object> params = params(new GetStatisticsEvent(), space("lazy", "psql", true, OWN_PROPERTIES));

    assertThat(params).doesNotContainKey(ON_DEMAND_INDEXES).containsEntry(LAZY_TABLE, true);
  }

  @Test
  void aOneLevelExtensionUsesThePropertiesOfItsBase() {
    Space base = space("base", "psql", false, BASE_PROPERTIES);
    Space extension = extending(space("extension", "psql", true, OWN_PROPERTIES), base);

    assertThat(writeParams(extension)).containsEntry(ON_DEMAND_INDEXES, List.of("base"));
  }

  @Test
  void aTwoLevelExtensionUsesThePropertiesOfItsRootBase() {
    Space root = space("root", "psql", true, ROOT_PROPERTIES);
    Space intermediate = extending(space("intermediate", "psql", false, BASE_PROPERTIES), root);
    Space extension = extending(space("extension", "psql", true, OWN_PROPERTIES), intermediate);

    assertThat(writeParams(extension)).containsEntry(ON_DEMAND_INDEXES, List.of("root"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void aLazyBaseOfAnEagerExtensionGetsTheOnDemandIndexes() {
    //A write with context SUPER materializes the base
    Space base = space("base", "psql", true, BASE_PROPERTIES);
    Space extension = extending(space("extension", "psql", false, null), base);

    Map<String, Object> params = writeParams(extension);

    assertThat(params).containsEntry(ON_DEMAND_INDEXES, List.of("base"));
    assertThat((Map<String, Object>) params.get("extends")).containsEntry(LAZY_TABLE, true);
  }

  @Test
  void aLazyBaseOnAnotherStorageIsNoLazyLevel() {
    Space base = space("base", "psql_db2", true, BASE_PROPERTIES);
    Space extension = extending(space("extension", "psql", false, null), base);

    assertThat(writeParams(extension)).doesNotContainKey(ON_DEMAND_INDEXES);
  }

  @Test
  void extensionsOfARootBaseWithoutSearchablePropertiesCarryAnEmptyList() {
    Space base = space("base", "psql", false, null);
    Space extension = extending(space("extension", "psql", true, OWN_PROPERTIES), base);
    Space intermediate = extending(space("intermediate", "psql", false, BASE_PROPERTIES), base);
    Space secondLevel = extending(space("second", "psql", true, OWN_PROPERTIES), intermediate);

    assertThat(writeParams(extension)).containsEntry(ON_DEMAND_INDEXES, List.of());
    assertThat(writeParams(secondLevel)).containsEntry(ON_DEMAND_INDEXES, List.of());
  }

  @Test
  void anExtensionWithoutResolvedRootBaseCarriesNoOnDemandIndexes() {
    Space intermediate = space("intermediate", "psql", false, BASE_PROPERTIES);
    intermediate.setExtension(new Extension().withSpaceId("root"));
    Space extension = extending(space("extension", "psql", true, null), intermediate);

    assertThat(writeParams(extension)).doesNotContainKey(ON_DEMAND_INDEXES);
  }

  @Test
  void paramsWithoutLazyLevelStayUnchanged() {
    Map<String, Object> params = new HashMap<>(Map.of(TABLE_NAME, "some_table"));

    FeatureHandler.injectOnDemandIndexes(params, space("eager", "psql", false, OWN_PROPERTIES));

    assertThat(params).isEqualTo(Map.of(TABLE_NAME, "some_table"));
  }

  private static Map<String, Object> writeParams(Space space) {
    return params(new WriteFeaturesEvent(), space);
  }

  private static Map<String, Object> params(ContextAwareEvent<?> event, Space space) {
    try {
      FeatureHandler.injectSpaceParams(event, space);
    }
    catch (HttpException e) {
      throw new AssertionError(e);
    }
    return event.getParams();
  }

  private static Space space(String id, String storageId, boolean lazy, Map<String, Boolean> searchableProperties) {
    Map<String, Object> params = new HashMap<>(Map.of(TABLE_NAME, id + "_table"));
    if (lazy)
      params.put(LAZY_TABLE, true);
    Space space = new Space();
    space.setId(id);
    space.setStorage(new ConnectorRef().withId(storageId).withParams(params));
    space.setSearchableProperties(searchableProperties);
    return space;
  }

  private static Space extending(Space space, Space base) {
    Extension extension = new Extension().withSpaceId(base.getId());
    extension.resolvedSpace = base;
    space.setExtension(extension);
    return space;
  }
}
