/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.app.service;

import static com.here.naksha.app.common.CommonApiTestSetup.createStorage;
import static com.here.naksha.app.common.TestUtil.*;
import static com.here.naksha.app.common.TestUtil.HDR_STREAM_ID;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static org.junit.jupiter.api.Named.named;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.CommonApiTestSetup;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class EventHandlerApiTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

  EventHandlerApiTest() {
    super(nakshaClient);
  }

  @BeforeAll
  static void setup() throws URISyntaxException, IOException, InterruptedException {
    createStorage(nakshaClient, "EventHandlerApi/setup/create_storage.json");
  }

  @Test
  void tc0100_testCreateEventHandler() throws Exception {
    // Test API : POST /hub/handlers
    // Given: Create handler request body & expected response
    final String requestBody = loadFileOrFail("EventHandlerApi/TC0100_createEventHandler/create_event_handler.json");
    final String expectedResponseBody = loadFileOrFail("EventHandlerApi/TC0100_createEventHandler/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When: creating event handler
    final HttpResponse<String> response = getNakshaClient().post("hub/handlers", requestBody, streamId);

    // Then: Operation succeeded and expected response was returned
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedResponseBody);
  }

  @Test
  void tc0101_testDuplicateEventHandler() throws Exception {
    // Test API : POST /hub/handlers
    // Given: Create handler request body & expected response
    final String bodyJson = loadFileOrFail("EventHandlerApi/TC0101_duplicateEventHandler/create_event_handler.json");
    final String expectedResponseBody = loadFileOrFail("EventHandlerApi/TC0101_duplicateEventHandler/response_conflict.json");
    final String streamId = UUID.randomUUID().toString();

    // And: Handler registered for the first time
    HttpResponse<String> response = getNakshaClient().post("hub/handlers", bodyJson, streamId);
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId);

    // When: Registering handler for the 2nd time
    response = getNakshaClient().post("hub/handlers", bodyJson, streamId);

    // Then: 409 Conflict should be returned
    assertThat(response)
        .hasStatus(409)
        .hasJsonBody(expectedResponseBody, "Expecting duplicated handler in response");
  }

  static Stream<Named> handlersWithoutRequiredProperty(){
    String location = "EventHandlerApi/TC0102_createHandlerWithoutProperty/";
    return Stream.of(
        named("Missing 'className' property", location + "no_class_name.json"),
        named("Missing 'description' property", location + "no_desc.json"),
        named("Missing 'title' property", location + "no_title.json"),
        named("Default handler - Missing 'storage' property", location + "default_handler_without_storage.json")
    );
  }

  @ParameterizedTest
  @MethodSource("handlersWithoutRequiredProperty")
  void tc0102_testCreateHandlerWithoutProperty(String requestBodyFileName) throws Exception {
    // Test API : POST /hub/handlers
    // Given: Creation request without specified property
    final String requestBody = loadFileOrFail(requestBodyFileName);
    final String expectedBodyPart = loadFileOrFail("EventHandlerApi/TC0102_createHandlerWithoutProperty/response_part.json");
    final String streamId = UUID.randomUUID().toString();

    // When: creating event handler without
    final HttpResponse<String> response = getNakshaClient().post("hub/handlers", requestBody, streamId);

    // 3. Perform assertions
    assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedBodyPart, "Expecting failure response");
  }

  @Test
  void tc0103_testCreateDefaultHandlerWithMissingStorage() throws URISyntaxException, IOException, InterruptedException {
    // Given: a default handler without storageId property defined
    final String streamId = UUID.randomUUID().toString();
    final String createJson = loadFileOrFail("EventHandlerApi/TC0103_createDefaultHandlerMissingStorage/create_event_handler.json");
    final String expectedNotFoundResponse = loadFileOrFail(
        "EventHandlerApi/TC0103_createDefaultHandlerMissingStorage/not_found_response.json");

    // When: trying to create such handler
    final HttpResponse<String> response = nakshaClient.post("hub/handlers", createJson, streamId);

    // Then: creating fails due to not-found storage
    assertThat(response)
        .hasStatus(404)
        .hasJsonBody(expectedNotFoundResponse)
        .hasStreamIdHeader(streamId);
  }

  @Test
  void tc0104_testCreateRandomHandlerWithoutStorageProperty() throws URISyntaxException, IOException, InterruptedException {
    // Given: a default handler without storageId property defined
    final String streamId = UUID.randomUUID().toString();
    final String createJson = loadFileOrFail("EventHandlerApi/TC0104_createRandomHandlerNoStorageProp/create_event_handler.json");
    final String expectedResponse = loadFileOrFail("EventHandlerApi/TC0104_createRandomHandlerNoStorageProp/response.json");

    // When: trying to create such handler
    final HttpResponse<String> response = nakshaClient.post("hub/handlers", createJson, streamId);

    // Then: creating fails due to validation error
    assertThat(response)
        .hasStatus(200)
        .hasJsonBody(expectedResponse)
        .hasStreamIdHeader(streamId);
  }

  @Test
  void tc0120_testGetHandlerById() throws Exception {
    // Test API : GET /hub/handlers/{handlerId}
    // Given: created handler
    final String streamId = UUID.randomUUID().toString();
    final String createEventRequestBody = loadFileOrFail("EventHandlerApi/TC0120_getHandlerById/create_event_handler.json");
    final String expectedResponseBody = loadFileOrFail("EventHandlerApi/TC0120_getHandlerById/get_response.json");

    // And: created event handler
    nakshaClient.post("hub/handlers", createEventRequestBody, streamId);

    // When: Fetching created handler by id
    final HttpResponse<String> response = getNakshaClient().get("hub/handlers/test-handler-by-id", streamId);

    // Then: Created handler is fetched
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedResponseBody, "Expecting handler response");
  }

  @Test
  void tc0121_testGetHandlerByWrongId() throws Exception {
    // Test API : GET /hub/handlers/{handlerId}
    // Given: wrong id
    final String streamId = UUID.randomUUID().toString();
    final String wrongId = "this-surely-does-not-exists";

    // When: Fetching created handler by wrong id
    final HttpResponse<String> response = getNakshaClient().get("hub/handlers/" + wrongId, streamId);

    // Then: Created handler is fetched
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId);
  }

  @Test
  void tc0140_testGetHandlers() throws Exception {
    // Given: handlers definition
    List<String> expectedHandlerIds = List.of("tc_140_event_handler_1", "tc_140_event_handler_2");
    final String streamId = UUID.randomUUID().toString();
    final String createFirstHandlerRequestBody = loadFileOrFail("EventHandlerApi/TC0140_getHandlers/create_event_handler_1.json");
    final String createSecondHandlerRequestBody = loadFileOrFail("EventHandlerApi/TC0140_getHandlers/create_event_handler_2.json");

    // And: created event handlers
    nakshaClient.post("hub/handlers", createFirstHandlerRequestBody, streamId);
    nakshaClient.post("hub/handlers", createSecondHandlerRequestBody, streamId);

    // When: Fetching all handlers
    final HttpResponse<String> response = getNakshaClient().get("hub/handlers", streamId);

    // Then: Response is successful
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId);

    // And: all saved handlers are returned
    List<XyzFeature> returnedXyzFeatures =
        parseJson(response.body(), XyzFeatureCollection.class).getFeatures();
    boolean allReturnedFeaturesAreEventHandlers = returnedXyzFeatures.stream()
        .allMatch(feature -> EventHandler.class.isAssignableFrom(feature.getClass()));
    Assertions.assertTrue(allReturnedFeaturesAreEventHandlers);
    List<String> eventHandlerIds =
        returnedXyzFeatures.stream().map(XyzFeature::getId).toList();
    Assertions.assertTrue(eventHandlerIds.containsAll(expectedHandlerIds));
  }

  @Test
  void tc0160_testUpdateEventHandler() throws Exception {
    // Test API : PUT /hub/handlers/{handlerId}
    // Given: request bodies
    final String streamId = UUID.randomUUID().toString();
    final String crateHandlerRequestBody = loadFileOrFail("EventHandlerApi/TC0160_updateEventHandler/create_event_handler.json");
    final String updateHandlerRequestBody = loadFileOrFail("EventHandlerApi/TC0160_updateEventHandler/update_event_handler.json");
    final String expectedResponseBody = loadFileOrFail("EventHandlerApi/TC0160_updateEventHandler/response.json");

    // And: created event handler
    nakshaClient.post("hub/handlers", crateHandlerRequestBody, streamId);

    // When: updating event handler
    final HttpResponse<String> response = nakshaClient.put("hub/handlers/test-update-handler", updateHandlerRequestBody, streamId);

    // Then: updated event handler is returned
    assertThat(response)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedResponseBody);
  }

  @Test
  void tc0161_testUpdateNonexistentEventHandler() throws Exception {
    // Test API : PUT /hub/handlers/{handlerId}
    // Given: requests details and expected response
    final String streamId = UUID.randomUUID().toString();
    final String wrongId = "this-surely-does-not-exists";
    final String updateRequestBody = loadFileOrFail("EventHandlerApi/TC0161_updateNonexistentEventHandler/update_event_handler.json");
    final String expectedResponse = loadFileOrFail("EventHandlerApi/TC0161_updateNonexistentEventHandler/response.json");

    // When: updating nonexistent handler
    final HttpResponse<String> response = getNakshaClient().put("hub/handlers/" + wrongId, updateRequestBody, streamId);

    // Then:
    assertThat(response)
        .hasStatus(404)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedResponse);
  }

  @Test
  void tc0162_testUpdateEventHandlerWithMismatchingId() throws Exception {
    // Test API : PUT /hub/handlers/{handlerId}
    // Given:
    final String updateOtherHandlerJson =
        loadFileOrFail("EventHandlerApi/TC0162_updateEventHandlerWithMismatchingId/update_event_handler.json");
    final String expectedErrorResponse = loadFileOrFail("EventHandlerApi/TC0162_updateEventHandlerWithMismatchingId/response.json");
    final String streamId = UUID.randomUUID().toString();

    // When:
    final HttpResponse<String> response =
        getNakshaClient().put("hub/handlers/test-handler", updateOtherHandlerJson, streamId);

    // Then:
    assertThat(response)
        .hasStatus(400)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(expectedErrorResponse);
  }

  @Test
  void tc0180_testDeleteHandler() throws Exception {
    // Test API : DELETE /hub/handlers/{handlerId}
    // Given: send a request to create an event handler
    final String streamId = UUID.randomUUID().toString();
    CommonApiTestSetup.createHandler(nakshaClient,"EventHandlerApi/TC0180_deleteHandler/create_handler.json");
    // When: here we send the deletion request to test if it works
    final HttpResponse<String> response =
            getNakshaClient().delete("hub/handlers/handler-to-delete", streamId);

    // Then: check that the delete request is successful
    assertThat(response)
            .hasStatus(200)
            .hasStreamIdHeader(getHeader(response, HDR_STREAM_ID));
    // and the handler does not exist anymore
    final HttpResponse<String> getResponse =
            getNakshaClient().get("hub/handlers/handler-to-delete", streamId);
    assertThat(getResponse)
            .hasStatus(404)
            .hasStreamIdHeader(getHeader(response, HDR_STREAM_ID));
  }

  @Test
  void tc0181_testDeleteHandlerInUse() throws Exception {
    // Test API : DELETE /hub/handlers/{handlerId}
    // Given: creating an event handler, and a space that uses this handler in its event pipeline
    final String streamId = UUID.randomUUID().toString();
    CommonApiTestSetup.createHandler(nakshaClient,"EventHandlerApi/TC0181_deleteHandlerInUse/create_handler.json");
    CommonApiTestSetup.createSpace(nakshaClient,"EventHandlerApi/TC0181_deleteHandlerInUse/create_space.json");
    // When: send a request to delete the handler
    final HttpResponse<String> response =
            getNakshaClient().delete("hub/handlers/handler-blocked-from-deletion", streamId);

    // Then: the delete request should fail, as at least 1 space is still using it
    final String expectedResponse = loadFileOrFail("EventHandlerApi/TC0181_deleteHandlerInUse/response.json");
    assertThat(response)
            .hasStatus(409)
            .hasJsonBody(expectedResponse)
            .hasStreamIdHeader(getHeader(response, HDR_STREAM_ID));
    // and the event handler still exists
    final HttpResponse<String> getResponse =
            getNakshaClient().get("hub/handlers/handler-blocked-from-deletion", streamId);
    assertThat(getResponse)
            .hasStatus(200)
            .hasStreamIdHeader(getHeader(response, HDR_STREAM_ID));
  }

  @Test
  void tc0182_testDeleteNonExistingHandler() throws Exception {
    // Test API : DELETE /hub/handlers/{handlerId}
    // Given: no event handler
    final String streamId = UUID.randomUUID().toString();

    // When: send the delete request
    final HttpResponse<String> response =
            getNakshaClient().delete("hub/handlers/unreal-handler-cannot-delete", streamId);

    // Then: the delete request fails because this event handler does not exist
    assertThat(response)
            .hasStatus(404)
            .hasStreamIdHeader(getHeader(response, HDR_STREAM_ID));
  }
}
