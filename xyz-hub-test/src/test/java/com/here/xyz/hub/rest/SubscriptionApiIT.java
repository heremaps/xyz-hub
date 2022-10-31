package com.here.xyz.hub.rest;

import com.jayway.restassured.response.ValidatableResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SubscriptionApiIT extends TestSpaceWithFeature {

    private static String cleanUpId = "space-1";

    @BeforeClass
    public static void setupClass() {
        removeAll();
    }
    
    @Before
    public void setup() {
        createSpaceWithCustomStorage(cleanUpId, "psql", null);
    }

    @After
    public void teardown() {
        removeAll();
    }

    private static void removeAll() {
        removeSpace(cleanUpId);
        removeSubscription(AuthProfile.ACCESS_ALL, "test-subscription-1");
    }

    private void addTestSubscription() {
        addSubscription(AuthProfile.ACCESS_ALL,"/xyz/hub/createSubscription.json");
    }

    public static ValidatableResponse addSubscription(AuthProfile authProfile, String contentFile) {
        return given()
                .accept(APPLICATION_JSON)
                .contentType(APPLICATION_JSON)
                .headers(getAuthHeaders(authProfile))
                .body(content(contentFile))
                .when()
                .post("/spaces/" + cleanUpId + "/subscriptions")
                .then();
    }
    public static ValidatableResponse removeSubscription(AuthProfile authProfile, String subscriptionId) {
        return given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(authProfile))
                .when()
                .delete("/spaces/" + cleanUpId + "/subscriptions/" + subscriptionId)
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
    public void getSubscriptionById() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .when()
                .get("/spaces/" + cleanUpId + "/subscriptions/" + subscriptionId)
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
                .get("/spaces/" + cleanUpId + "/subscriptions/test-subscription-1")
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
                .get("/spaces/" + cleanUpId + "/subscriptions/test-subscription-1")
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
                .get("/spaces/" + cleanUpId + "/subscriptions")
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
                .queryParam("source", "space-1")
                .when()
                .get("/spaces/" + cleanUpId + "/subscriptions")
                .then()
                .statusCode(OK.code())
                .body("size()", is(0));
    }

    @Test
    public void deleteSubscription() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        removeSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, subscriptionId)
                .statusCode(OK.code())
                .body("id", equalTo(subscriptionId));
    }

    @Test
    public void deleteSubscriptionNotPresent() {

        String subscriptionId = "unknown-subscription";
        removeSubscription(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES, subscriptionId)
                .statusCode(NOT_FOUND.code());
    }

    @Test
    public void deleteSubscriptionInsufficientRights() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        removeSubscription(AuthProfile.NO_ACCESS, subscriptionId)
                .statusCode(FORBIDDEN.code());
    }

    @Test
    public void deleteSubscriptionWithAnotherSpaceAccesss() {
        addTestSubscription();

        String subscriptionId = "test-subscription-1";
        removeSubscription(AuthProfile.ACCESS_SPACE_2_MANAGE_SPACES, subscriptionId)
                .statusCode(FORBIDDEN.code());
    }

}
