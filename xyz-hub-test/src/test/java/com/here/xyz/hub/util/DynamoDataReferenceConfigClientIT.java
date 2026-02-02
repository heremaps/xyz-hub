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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.here.xyz.hub.config.dynamo.DynamoDataReferenceConfigClient;
import com.here.xyz.models.hub.DataReference;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.here.xyz.hub.util.TestDataReferences.dataReference;
import static com.here.xyz.hub.util.TestDataReferences.dataReferenceAsMap;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

@Testcontainers(disabledWithoutDocker = true)
class DynamoDataReferenceConfigClientIT extends DynamoDbIT {

  private static final String TABLE_NAME = "data-references";

  private static final String TABLE_ARN = "arn:aws:dynamodb:local:000000000000:table/" + TABLE_NAME;

  private static final UUID referenceId = UUID.randomUUID();

  private DynamoDataReferenceConfigClient dataReferences;

  @BeforeEach
  void setUp() {
    dataReferences = new DynamoDataReferenceConfigClient(TABLE_ARN, endpoint);
    awaitResult(dataReferences.init());

    awaitTableActive();
  }

  @Test
  void shouldThrowWhenCallingStoreWithoutObjectToStore() {
    assertThatThrownBy(() -> dataReferences.store(null))
      .isInstanceOf(NullPointerException.class);
  }

  @Test
  void shouldThrowWhenCallingSimpleLoadWithoutId() {
    assertThatThrownBy(() -> dataReferences.load(null))
      .isInstanceOf(NullPointerException.class);
  }

  @Test
  void shouldThrowWhenCallingLoadByParametersWithoutEntityId() {
    assertThatThrownBy(() -> dataReferences.load(
      null,
      123,
      321,
      "content-type-A",
      "object-type-A",
      "source-system-A",
      "target-system-A"
    ))
      .isInstanceOf(NullPointerException.class);
  }

  @Test
  void shouldThrowWhenCallingDeleteWithoutId() {
    assertThatThrownBy(() -> dataReferences.delete(null))
      .isInstanceOf(NullPointerException.class);
  }

  static Stream<Arguments> referenceToStoreProvider() {
    return Stream.of(
      Arguments.argumentSet("DataReference containing id", dataReference(referenceId)),
      Arguments.argumentSet("DataReference without id", dataReference(referenceId).withId(null))
    );
  }

  @ParameterizedTest
  @MethodSource("referenceToStoreProvider")
  void shouldStoreDataReference(DataReference dataReference) {
    // when
    UUID newReferenceId = awaitResult(dataReferences.store(dataReference));

    // then
    assertThat(newReferenceId)
      .isNotNull()
      .isEqualTo(dataReference.getId());

    // and
    List<Map<String, Object>> referencesFromDb = fetchFromDb(dataReference.getId());
    Map<String, Object> expectedInDb = dataReferenceAsMap(referenceId);
    expectedInDb.put("id", newReferenceId.toString());
    assertThat(referencesFromDb)
      .containsExactly(expectedInDb);
  }

  @Test
  void shouldNotFetchDataReferenceByIdWhenItIsNotPresent() {
    // when
    Optional<DataReference> dataReferenceFromDb = awaitResult(
      dataReferences.load(referenceId)
    );

    // then
    assertThat(dataReferenceFromDb)
      .isEmpty();
  }

  @Test
  void shouldFetchDataReferenceByIdWhenItIsPresent() {
    // given
    Map<String, Object> referenceToStore = dataReferenceAsMap(referenceId);
    storeInDb(referenceToStore);

    // when
    Optional<DataReference> dataReferenceFromDb = awaitResult(
      dataReferences.load(referenceId)
    );

    // then
    assertThat(dataReferenceFromDb)
      .contains(dataReference(referenceId));
  }

  @Test
  void shouldNotFailDeletionOfNonExistentObject() {
    // when
    awaitResult(
      dataReferences.delete(referenceId)
    );

    // then
    Optional<DataReference> dataReferenceFromDb = awaitResult(
      dataReferences.load(referenceId)
    );
    assertThat(dataReferenceFromDb)
      .isEmpty();
  }

  @Test
  void shouldDeleteObjectIfItExists() {
    // given
    Map<String, Object> referenceToStore = dataReferenceAsMap(referenceId);
    storeInDb(referenceToStore);

    // when
    awaitResult(
      dataReferences.delete(referenceId)
    );

    // then
    Optional<DataReference> dataReferenceFromDb = awaitResult(
      dataReferences.load(referenceId)
    );
    assertThat(dataReferenceFromDb)
      .isEmpty();
  }

  static Stream<Arguments> queryParamsAndExpectedResultProvider() {
    return Stream.of(
      argumentSet(
        "query leading to empty result by entityId",
        "no-such-entity-id",
        null,
        null,
        null,
        null,
        null,
        null,
        emptyList()
      ),
      argumentSet(
        "query leading to empty result by startVersion",
        "entity-id-1",
        999,
        null,
        null,
        null,
        null,
        null,
        emptyList()
      ),
      argumentSet(
        "all objects for entity-id-1, sorted by endVersion",
        "entity-id-1",
        null,
        null,
        null,
        null,
        null,
        null,
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withEndVersion(1)
            .withContentType("content-type-B"),
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withEndVersion(2)
            .withObjectType("object-type-B"),
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withStartVersion(2)
            .withEndVersion(3)
            .withSourceSystem("source-system-B")
        )
      ),
      argumentSet(
        "all objects for entity-id-2, sorted by endVersion",
        "entity-id-2",
        null,
        null,
        null,
        null,
        null,
        null,
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-2")
            .withEndVersion(10)
            .withTargetSystem("target-system-B"),
          dataReference(referenceId)
            .withEntityId("entity-id-2")
            .withEndVersion(11),
          dataReference(referenceId)
            .withEntityId("entity-id-2")
            .withEndVersion(12),
          dataReference(referenceId)
            .withEntityId("entity-id-2")
            .withEndVersion(13)
        )
      ),
      argumentSet(
        "selection by entityId + endVersion",
        "entity-id-1",
        null,
        2,
        null,
        null,
        null,
        null,
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withEndVersion(2)
            .withObjectType("object-type-B")
        )
      ),
      argumentSet(
        "selection by entityId + contentType",
        "entity-id-1",
        null,
        null,
        "content-type-B",
        null,
        null,
        null,
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withEndVersion(1)
            .withContentType("content-type-B")
        )
      ),
      argumentSet(
        "selection by entityId + startVersion",
        "entity-id-1",
        2,
        null,
        null,
        null,
        null,
        null,
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withStartVersion(2)
            .withEndVersion(3)
            .withSourceSystem("source-system-B")
        )
      ),
      argumentSet(
        "selection by entityId + objectType",
        "entity-id-1",
        null,
        null,
        null,
        "object-type-B",
        null,
        null,
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withEndVersion(2)
            .withObjectType("object-type-B")
        )
      ),
      argumentSet(
        "selection by entityId + sourceSystem",
        "entity-id-1",
        null,
        null,
        null,
        null,
        "source-system-B",
        null,
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-1")
            .withStartVersion(2)
            .withEndVersion(3)
            .withSourceSystem("source-system-B")
        )
      ),
      argumentSet(
        "selection by entityId + targetSystem",
        "entity-id-2",
        null,
        null,
        null,
        null,
        null,
        "target-system-B",
        List.of(
          dataReference(referenceId)
            .withEntityId("entity-id-2")
            .withEndVersion(10)
            .withTargetSystem("target-system-B")
        )
      )
    );
  }

  @ParameterizedTest
  @MethodSource("queryParamsAndExpectedResultProvider")
  void shouldQueryForObjectsBasedOnCriteria(
    String entityId,
    Integer startVersion,
    Integer endVersion,
    String contentType,
    String objectType,
    String sourceSystem,
    String targetSystem,
    List<DataReference> expectedResults
  ) {
    // given
    String entityId1 = "entity-id-1";
    String entityId2 = "entity-id-2";
    
    Map<String, Object> dataReference1 = dataReferenceAsMap(referenceId);
    dataReference1.put("entityId", entityId1);
    dataReference1.put("startVersion", 2);
    dataReference1.put("endVersion", 3);
    dataReference1.put("sourceSystem", "source-system-B");
    storeInDb(dataReference1);

    Map<String, Object> dataReference2 = dataReferenceAsMap(referenceId);
    dataReference2.put("entityId", entityId1);
    dataReference2.put("endVersion", 1);
    dataReference2.put("contentType", "content-type-B");
    storeInDb(dataReference2);

    Map<String, Object> dataReference3 = dataReferenceAsMap(referenceId);
    dataReference3.put("entityId", entityId1);
    dataReference3.put("endVersion", 2);
    dataReference3.put("objectType", "object-type-B");
    storeInDb(dataReference3);

    Map<String, Object> dataReference4 = dataReferenceAsMap(referenceId);
    dataReference4.put("entityId", entityId2);
    dataReference4.put("endVersion", 10);
    dataReference4.put("targetSystem", "target-system-B");
    storeInDb(dataReference4);

    Map<String, Object> dataReference5 = dataReferenceAsMap(referenceId);
    dataReference5.put("entityId", entityId2);
    dataReference5.put("endVersion", 11);
    storeInDb(dataReference5);

    Map<String, Object> dataReference6 = dataReferenceAsMap(referenceId);
    dataReference6.put("entityId", entityId2);
    dataReference6.put("endVersion", 12);
    storeInDb(dataReference6);

    Map<String, Object> dataReference7 = dataReferenceAsMap(referenceId);
    dataReference7.put("entityId", entityId2);
    dataReference7.put("endVersion", 13);
    storeInDb(dataReference7);

    // when
    List<DataReference> queryResult = awaitResult(
      dataReferences.load(
        entityId,
        startVersion,
        endVersion,
        contentType,
        objectType,
        sourceSystem,
        targetSystem
      )
    );

    // then
    assertThat(queryResult)
      .containsExactlyElementsOf(expectedResults);
  }

  private static <T> T awaitResult(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().join();
  }

  @Override
  protected String tableName() {
    return TABLE_NAME;
  }

  private static List<Map<String, Object>> fetchFromDb(UUID id) {
    return fetchFromDb(
      new ScanRequest(TABLE_NAME)
        .withFilterExpression("id = :id")
        .withExpressionAttributeValues(
          Map.of(":id", new AttributeValue().withS(id.toString()))
        )
    );
  }

}
