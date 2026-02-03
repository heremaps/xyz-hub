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

package com.here.xyz.hub.rest;

import io.restassured.response.Response;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static com.here.xyz.hub.rest.DataReferenceApiIT.SameJsonAs.sameJsonAs;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

@TestMethodOrder(OrderAnnotation.class)
final class DataReferenceApiIT extends RestAssuredTest {

  private static final Collection<UUID> allCreatedIds = new ArrayList<>();
  
  @AfterAll
  static void afterAll() {
    deleteAllCreatedObjects();
  }

  static Stream<Arguments> requestsAndExpectedIdsProvider() {
    return Stream.of(
      argumentSet(
        "create data reference for full export without start version and with explicit ID",
        "create-data-reference-1.json",
        equalTo("308a8ebd-de83-42ac-a5ce-e83bf5c60abc")
      ),
      argumentSet(
        "create data reference for full export without start version and with no ID",
        "create-data-reference-2.json",
        not(blankOrNullString())
      ),
      argumentSet(
        "create data reference for patch export with start version and with explicit ID",
        "create-data-reference-3.json",
        equalTo("308a8ebd-de83-42ac-a5ce-e83bf5c60def")
      ),
      argumentSet(
        "create another data reference for same entityId and endVersion",
        "create-data-reference-4.json",
        equalTo("308a8ebd-de83-42ac-a5ce-e83bf5c60aaa")
      ),
      argumentSet(
        "create-data-reference-5.json",
        "create-data-reference-5.json",
        equalTo("308a8ebd-de83-42ac-a5ce-e83bf5c60bbb")
      ),
      argumentSet(
        "create-data-reference-6.json",
        "create-data-reference-6.json",
        equalTo("308a8ebd-de83-42ac-a5ce-e83bf5c60ccc")
      )
    );
  }

  @Order(1)
  @ParameterizedTest
  @MethodSource("requestsAndExpectedIdsProvider")
  void shouldCreateReferenceWhenItDoesNotExist(String requestFilename, Matcher<String> expectedReferenceIdMatcher) {
    createDataReference(requestFilename)
      .then()
      .statusCode(CREATED.code())
      .body("id", expectedReferenceIdMatcher);
  }

  @Order(2)
  @Test
  void shouldRejectAttemptToOverwriteExistingReferenceUsingId() {
    given()
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
      .body(loadFile("xyz/hub/data-references/requests/create-data-reference-1.json"))
      .post("/references")
      .then()
      .statusCode(BAD_REQUEST.code());
  }

  @Order(3)
  @Test
  void shouldReturnNotFoundWhenNoDataReferenceExists() {
    getReferenceById("00000000-0000-0000-0000-000000000000")
      .then()
      .statusCode(NOT_FOUND.code());
  }

  @Order(4)
  @Test
  void shouldReturnReferenceWhenItExists() {
    getReferenceById("308a8ebd-de83-42ac-a5ce-e83bf5c60abc")
      .then()
      .statusCode(OK.code())
      .contentType(APPLICATION_JSON)
      .body(sameJsonAs(loadFile("xyz/hub/data-references/responses/data-reference-1.json")));
  }

  static Stream<Arguments> queriesAndExpectedResultsProvider() {
    return Stream.of(
      argumentSet(
        "only entityId (non-existent)",
        Map.of("entityId", "no-such-entity-id"),
        "empty-data-reference-list.json"
      ),
      argumentSet(
        "only entityId (existing)",
        Map.of("entityId", "entity-id-1"),
        "data-reference-list-1.json"
      ),
      argumentSet(
        "entityId (existing) with endVersion (non-existing)",
        Map.of(
          "entityId", "entity-id-1",
          "endVersion", "9999"
        ),
        "empty-data-reference-list.json"
      ),
      argumentSet(
        "entityId (existing) with endVersion (existing)",
        Map.of(
          "entityId", "entity-id-1",
          "endVersion", "134"
        ),
        "data-reference-list-2.json"
      ),
      argumentSet(
        "entityId (existing) with startVersion (existing)",
        Map.of(
          "entityId", "entity-id-1",
          "startVersion", "123"
        ),
        "data-reference-list-3.json"
      ),
      argumentSet(
        "entityId (existing) with objectType (existing)",
        Map.of(
          "entityId", "entity-id-1",
          "objectType", "object-type-A"
        ),
        "data-reference-list-7.json"
      ),
      argumentSet(
        "entityId (existing) with contentType (existing)",
        Map.of(
          "entityId", "entity-id-1",
          "contentType", "content-type-C"
        ),
        "data-reference-list-4.json"
      ),
      argumentSet(
        "entityId (existing) with sourceSystem (existing)",
        Map.of(
          "entityId", "entity-id-1",
          "sourceSystem", "source-system-E"
        ),
        "data-reference-list-5.json"
      ),
      argumentSet(
        "entityId (existing) with targetSystem (existing)",
        Map.of(
          "entityId", "entity-id-1",
          "targetSystem", "target-system-F"
        ),
        "data-reference-list-6.json"
      )
    );
  }

  @Order(5)
  @ParameterizedTest
  @MethodSource("queriesAndExpectedResultsProvider")
  void shouldQueryForReferencesByGivenCriteria(Map<String, String> queryParameters, String responseFilename) {
    queryForReferences(queryParameters)
      .then()
      .statusCode(OK.code())
      .contentType(APPLICATION_JSON)
      .body(sameJsonAs(loadFile("xyz/hub/data-references/responses/%s".formatted(responseFilename))));
  }

  @Order(6)
  @Test
  void shouldDeleteReferenceWhenItExists() {
    deleteReference("308a8ebd-de83-42ac-a5ce-e83bf5c60abc")
      .then()
      .statusCode(NO_CONTENT.code());
  }

  @Order(7)
  @Test
  void shouldNotFailWhenAttemptingToDeleteNonExistentReference() {
    deleteReference("00000000-0000-0000-0000-000000000000")
      .then()
      .statusCode(NO_CONTENT.code());
  }

  @Order(8)
  @Test
  void shouldRejectIncorrectReferenceIdWhenGettingReference() {
    getReferenceById("Not an UUID")
      .then()
      .statusCode(BAD_REQUEST.code());
  }

  @Order(9)
  @Test
  void shouldRejectIncorrectReferenceIdWhenDeletingReference() {
    deleteReference("Not an UUID")
      .then()
      .statusCode(BAD_REQUEST.code());
  }

  static Stream<Arguments> incorrectQueries() {
    return Stream.of(
      argumentSet(
        "Missing entityId",
        Map.of("endVersion", "123")
      ),
      argumentSet(
        "endVersion non-numeric",
        Map.of(
          "entityId", "some-entity-id",
          "endVersion", "abc"
        )
      ),
      argumentSet(
        "endVersion is a negative number",
        Map.of(
          "entityId", "some-entity-id",
          "endVersion", "-1"
        )
      ),
      argumentSet(
        "startVersion non-numeric",
        Map.of(
          "entityId", "some-entity-id",
          "startVersion", "abc"
        )
      ),
      argumentSet(
        "startVersion is a negative number",
        Map.of(
          "entityId", "some-entity-id",
          "startVersion", "-1"
        )
      )
    );
  }

  @Order(9)
  @ParameterizedTest
  @MethodSource("incorrectQueries")
  void shouldRejectIncorrectParametersWhenQueryingForReference(Map<String, String> incorrectQuery) {
    queryForReferences(incorrectQuery)
      .then()
      .statusCode(BAD_REQUEST.code());
  }

  @Order(10)
  @Test
  void shouldRejectIncorrectDataReferenceCreationRequests() {
    createDataReference("incorrect-creation-request-1.json")
      .then()
      .statusCode(BAD_REQUEST.code());
  }

  private static Response getReferenceById(String referenceId) {
    return given()
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
      .get("/references/%s".formatted(referenceId));
  }

  private static Response queryForReferences(Map<String, String> queryParameters) {
    return given()
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
      .queryParams(queryParameters).get("/references");
  }

  private static Response createDataReference(String requestFilename) {
    Response response = given()
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
      .body(loadFile("xyz/hub/data-references/requests/%s".formatted(requestFilename)))
      .post("/references");

    int responseStatus = response.then().extract().statusCode();
    if (responseStatus == CREATED.code()) {
      registerCreatedObject(
        response.then().extract().body().path("id")
      );
    }

    return response;
  }

  private static void registerCreatedObject(String createdObjectId) {
    allCreatedIds.add(UUID.fromString(createdObjectId));
  }

  private static void deleteAllCreatedObjects() {
    allCreatedIds.forEach(DataReferenceApiIT::deleteReference);
  }

  private static void deleteReference(UUID referenceId) {
    deleteReference(referenceId.toString());
  }

  private static Response deleteReference(String referenceId) {
    return given()
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
      .delete("/references/" + referenceId);
  }

  private static String loadFile(String filePathRelativeOfTestResources) {
    try {
      Path absoluteFilePath = absulteFilePath(filePathRelativeOfTestResources);
      return Files.readString(absoluteFilePath);
    } catch (IOException ex) {
      throw new RuntimeException(
        "Unable to load file for path " + filePathRelativeOfTestResources, ex);
    }
  }

  private static Path absulteFilePath(String filePathRelativeOfTestResources) {
    return Paths.get(
      executionPath(),
      "src",
      "test",
      "resources"
    ).resolve(filePathRelativeOfTestResources);
  }

  private static String executionPath() {
    try {
      return new File(".").getCanonicalPath();
    } catch (IOException ex) {
      throw new RuntimeException("Cannot establish execution path.", ex);
    }
  }

  static final class SameJsonAs extends TypeSafeMatcher<String> {

    private final String expected;

    public SameJsonAs(String expected) {
      this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(String actual) {
      try {
        JSONAssert.assertEquals(expected, actual, JSONCompareMode.STRICT);
        return true;
      } catch (AssertionError | JSONException e) {
        return false;
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("same JSON as ").appendValue(expected);
    }

    public static SameJsonAs sameJsonAs(String expected) {
      return new SameJsonAs(expected);
    }
  }
}
