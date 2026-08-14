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

package com.here.xyz.hub.util;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.getTableNameFromSpaceParamsOrSpaceId;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.isGeneratedTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SpaceTableResolverTest {

  private static final int PG_MAX_IDENTIFIER_LENGTH = 63;
  private static final int LONGEST_DERIVED_SUFFIX_LENGTH = 25;

  private static Space spaceWithStorage(Map<String, Object> params) {
    return new Space()
        .withId("my-space")
        .withStorage(new ConnectorRef().withId("psql").withParams(params));
  }

  @Test
  void assignsAUniqueTableNameToANewSpace() {
    Space space = spaceWithStorage(null);

    String tableName = SpaceTableResolver.assignTableName(space);

    assertThat(tableName).isNotNull();
    assertThat(isGeneratedTableName(tableName)).isTrue();
    assertThat(space.getStorage().getParams()).containsEntry(TABLE_NAME, tableName);
    assertThat(SpaceTableResolver.getTableName(space)).isEqualTo(tableName);
  }

  @Test
  void generatedTableNameIsIndependentOfTheSpaceId() {
    String firstIncarnation = SpaceTableResolver.assignTableName(spaceWithStorage(null));
    String secondIncarnation = SpaceTableResolver.assignTableName(spaceWithStorage(null));

    //Re-creating a space with the same ID must result in a different physical table
    assertThat(firstIncarnation).isNotEqualTo(secondIncarnation);
    assertThat(firstIncarnation).doesNotContain("my-space");
  }

  @Test
  void generatedTableNameFitsIntoThePostgresIdentifierLimit() {
    String tableName = SpaceTableResolver.assignTableName(spaceWithStorage(null));

    assertThat(tableName.length() + LONGEST_DERIVED_SUFFIX_LENGTH).isLessThanOrEqualTo(PG_MAX_IDENTIFIER_LENGTH);
    //Postgres identifiers must not start with a digit
    assertThat(tableName).matches("[a-z_][a-z0-9_]*");
  }

  @Test
  void doesNotOverrideAnExistingTableName() {
    Space space = spaceWithStorage(new HashMap<>(Map.of(TABLE_NAME, "some_existing_table")));

    assertThat(SpaceTableResolver.assignTableName(space)).isEqualTo("some_existing_table");
  }

  @Test
  void keepsOtherStorageParams() {
    Space space = spaceWithStorage(Map.of("someParam", "someValue"));

    SpaceTableResolver.assignTableName(space);

    assertThat(space.getStorage().getParams()).containsEntry("someParam", "someValue");
    assertThat(space.getStorage().getParams()).containsKey(TABLE_NAME);
  }

  @Test
  void assignedTableNameIsPickedUpByTheStorageSideResolution() {
    Space space = spaceWithStorage(null);
    String tableName = SpaceTableResolver.assignTableName(space);

    assertThat(getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(), false))
        .isEqualTo(tableName);
    assertThat(getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(), true))
        .isEqualTo(tableName);
  }

  @Test
  void legacySpacesKeepTheirSpaceIdDerivedTableName() {
    Space space = spaceWithStorage(null);

    assertThat(SpaceTableResolver.getTableName(space)).isNull();
    assertThat(getTableNameFromSpaceParamsOrSpaceId(space.getStorage().getParams(), space.getId(), false))
        .isEqualTo("my-space");
  }

  @Test
  void tableNameIsNeverInheritedFromAnotherSpace() {
    Map<String, Object> extendedStorageParams = Map.of(TABLE_NAME, "base_table", "someParam", "someValue");

    Map<String, Object> params = SpaceTableResolver.withoutTableName(extendedStorageParams);

    assertThat(params).doesNotContainKey(TABLE_NAME);
    assertThat(params).containsEntry("someParam", "someValue");
  }

  @Test
  void failsForSpacesWithoutStorage() {
    assertThatThrownBy(() -> SpaceTableResolver.assignTableName(new Space().withId("my-space")))
        .isInstanceOf(IllegalStateException.class);
  }
}

