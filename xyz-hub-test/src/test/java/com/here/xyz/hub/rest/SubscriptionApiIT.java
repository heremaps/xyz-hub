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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.here.xyz.models.hub.Subscription;
import io.restassured.response.ValidatableResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubscriptionApiIT extends TestSpaceWithFeature {

    private static String cleanUpSpaceId = "space1";
    private static String cleanUpSpaceId2 = "space2";

    @BeforeClass
    public static void setupClass() {
        removeAll();
    }

    @Before
    public void setup() {
        createSpaceWithCustomStorage(cleanUpSpaceId, "psql", null);
        createSpaceWithVersionsToKeep(cleanUpSpaceId2, 10);
    }

    @After
    public void teardown() {
        removeAll();
    }

    private static void removeAll() {
        removeSubscription(AuthProfile.ACCESS_ALL, cleanUpSpaceId, "test-subscription-1");
        removeSubscription(AuthProfile.ACCESS_ALL, cleanUpSpaceId2, "test-subscription-1");
        removeSpace(cleanUpSpaceId);
        removeSpace(cleanUpSpaceId2);
    }

    public static Subscription addTestSubscription() {
        return given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                .body(content("/xyz/hub/createSubscription.json"))
                .post("/spaces/" + cleanUpSpaceId + "/subscriptions")
                .as(Subscription.class);
    }

    public static ValidatableResponse addSubscription(AuthProfile authProfile, String contentFile) {
        return given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(authProfile))
                .body(content(contentFile))
                .when()
                .post("/spaces/" + cleanUpSpaceId + "/subscriptions")
                .then();
    }
    public static ValidatableResponse removeSubscription(AuthProfile authProfile, String spaceId, String subscriptionId) {
        return given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(authProfile))
                .when()
                .delete("/spaces/" + spaceId + "/subscriptions/" + subscriptionId)
                .then();
    }

    @Test
    public void createSubscription() {
        addSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, "/xyz/hub/createSubscription.json")
                .statusCode(CREATED.code())
                .body("id", equalTo("test-subscription-1"));
    }

    @Test
    public void createSubscriptionWithAllAccess() {
        addSubscription(AuthProfile.ACCESS_ALL, "/xyz/hub/createSubscription.json")
                .statusCode(CREATED.code())
                .body("id", equalTo("test-subscription-1"));
    }

    @Test
    public void createSubscriptionWithNoAccess() {
        addSubscription(AuthProfile.NO_ACCESS, "/xyz/hub/createSubscription.json")
                .statusCode(FORBIDDEN.code());
    }

    @Test
    public void createSubscriptionWithAnotherSpaceAccess() {
        addSubscription(AuthProfile.ACCESS_SPACE_2_MANAGE_SPACES, "/xyz/hub/createSubscription.json")
                .statusCode(FORBIDDEN.code());
    }

    @Test
    public void createSubscriptionWithoutId() {
        addSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, "/xyz/hub/createSubscriptionWithoutId.json")
                .statusCode(BAD_REQUEST.code());
    }

    @Test
    public void createSubscriptionWithSameId() {
        addSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, "/xyz/hub/createSubscription.json")
                .statusCode(CREATED.code());
        addSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, "/xyz/hub/createSubscription.json")
                .statusCode(CONFLICT.code());
    }

    @Test
    public void createSubscriptionWithoutBody() {
         given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .when()
                .post("/spaces/" + cleanUpSpaceId + "/subscriptions")
                .then()
                .statusCode(BAD_REQUEST.code());
    }

    @Test
    public void createSubscriptionWithPUT() {
        String subscriptionId = "test-subscription-1";

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .body(content("/xyz/hub/createSubscription.json"))
                .when()
                .put("/spaces/" + cleanUpSpaceId + "/subscriptions/" + subscriptionId)
                .then()
                .statusCode(OK.code())
                .body("id", equalTo(subscriptionId));
    }

    @Test
    public void updateSubscription() {

        Subscription subscription = addTestSubscription();

        subscription.getStatus().withState(Subscription.SubscriptionStatus.State.INACTIVE).withStateReason("Test Inactive State");

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .body(subscription)
                .when()
                .put("/spaces/" + cleanUpSpaceId + "/subscriptions/" + subscription.getId())
                .then()
                .statusCode(OK.code())
                .body("status.state", equalTo(subscription.getStatus().getState().name()));
    }

    @Test
    public void updateSubscriptionByIdWithAnotherSpaceAccess() {
        Subscription subscription = addTestSubscription();
        subscription.getStatus().withState(Subscription.SubscriptionStatus.State.INACTIVE).withStateReason("Test Inactive State");

        given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_2_MANAGE_SPACES))
                .body(subscription)
                .when()
                .put("/spaces/" + cleanUpSpaceId + "/subscriptions/test-subscription-1")
                .then()
                .statusCode(FORBIDDEN.code());
    }

    @Test
    public void getSubscriptionById() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .when()
                .get("/spaces/" + cleanUpSpaceId + "/subscriptions/" + subscriptionId)
                .then()
                .statusCode(OK.code())
                .body("id", equalTo(subscriptionId));
    }

    @Test
    public void getSubscriptionByIdWithAnotherSpaceAccess() {
        addTestSubscription();

        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_2_MANAGE_SPACES))
                .when()
                .get("/spaces/" + cleanUpSpaceId + "/subscriptions/test-subscription-1")
                .then()
                .statusCode(FORBIDDEN.code());
    }

    @Test
    public void getSubscriptionByIdWithoutAccess() {
        addTestSubscription();

        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.NO_ACCESS))
                .when()
                .get("/spaces/" + cleanUpSpaceId + "/subscriptions/test-subscription-1")
                .then()
                .statusCode(FORBIDDEN.code());
    }

    @Test
    public void getUnknownSubscription() {
        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
                .when()
                .get("/subscriptions/unknown")
                .then()
                .statusCode(NOT_FOUND.code());
    }

    @Test
    public void getSubscriptionsForSpace() {
        addTestSubscription();

        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .when()
                .get("/spaces/" + cleanUpSpaceId + "/subscriptions")
                .then()
                .statusCode(OK.code())
                .body("size()", is(1))
                .body("[0].id", equalTo("test-subscription-1"));
    }

    @Test
    public void getSubscriptionForSpaceWithNoSubscriptions() {
        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .queryParam("source", "space1")
                .when()
                .get("/spaces/" + cleanUpSpaceId + "/subscriptions")
                .then()
                .statusCode(OK.code())
                .body("size()", is(0));
    }

    @Test
    public void deleteSubscription() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        removeSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, cleanUpSpaceId, subscriptionId)
                .statusCode(OK.code())
                .body("id", equalTo(subscriptionId));
    }

    @Test
    public void deleteSubscriptionNotPresent() {

        String subscriptionId = "unknown-subscription";
        removeSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, cleanUpSpaceId, subscriptionId)
                .statusCode(NOT_FOUND.code());
    }

    @Test
    public void deleteSubscriptionInsufficientRights() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        removeSubscription(AuthProfile.NO_ACCESS, cleanUpSpaceId, subscriptionId)
                .statusCode(FORBIDDEN.code());
    }

    @Test
    public void deleteSubscriptionWithAnotherSpaceAccess() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        removeSubscription(AuthProfile.ACCESS_SPACE_2_MANAGE_SPACES, cleanUpSpaceId, subscriptionId)
                .statusCode(FORBIDDEN.code());
    }

  @Test
  public void testAddSubscriptionOnSpaceV2K1() {
    addTestSubscription();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId)
        .then()
        .statusCode(OK.code())
        .body("versionsToKeep", equalTo(2));
  }


  @Test
  public void testAddSubscriptionOnSpaceV2K10() {
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/createSubscription.json"))
        .post("/spaces/" + cleanUpSpaceId2 + "/subscriptions")
        .then().statusCode(CREATED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId2)
        .then()
        .statusCode(OK.code())
        .body("versionsToKeep", equalTo(10));
  }
}
