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

    private String cleanUpId;

    @BeforeClass
    public static void setupClass() {
        removeAll();
    }
    
    @Before
    public void setup() {
        cleanUpId = "space-1";
        createSpaceWithCustomStorage(cleanUpId, "psql", null);
    }

    @After
    public void teardown() {
        removeSpace(cleanUpId);
        removeAll();
    }

    private static void removeAll() {
        // Delete all subscriptions which have potentially been created during the test
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
                .post("/subscriptions")
                .then();
    }
    public static ValidatableResponse removeSubscription(AuthProfile authProfile, String subscriptionId) {
        return given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(authProfile))
                .when()
                .delete("/subscriptions/" + subscriptionId)
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
                .get("/subscriptions/" + subscriptionId)
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
                .get("/subscriptions/test-subscription-1")
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
                .get("/subscriptions/test-subscription-1")
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
    public void getSubscriptionBySource() {
        addTestSubscription();

        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .queryParam("source", "space-1")
                .when()
                .get("/subscriptions")
                .then()
                .statusCode(OK.code())
                .body("size()", is(1))
                .body("[0].id", equalTo("test-subscription-1"));
    }

    @Test
    public void getSubscriptionBySourceWithNoSubscriptions() {
        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .queryParam("source", "space-1")
                .when()
                .get("/subscriptions")
                .then()
                .statusCode(OK.code())
                .body("size()", is(0));
    }

    @Test
    public void getSubscriptionBySourceWithoutSourceParam() {
        addTestSubscription();

        given()
                .accept(APPLICATION_JSON)
                .headers(getAuthHeaders(AuthProfile.ACCESS_SPACE_1_MANAGE_SPACES))
                .when()
                .get("/subscriptions")
                .then()
                .statusCode(BAD_REQUEST.code());
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
