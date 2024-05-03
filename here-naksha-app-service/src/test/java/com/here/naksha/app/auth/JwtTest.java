package com.here.naksha.app.auth;


import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.UUID;

import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.generateJWT;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

public class JwtTest extends ApiTest {
    // For this test suite, the default test-config.json denotes that
    // the service is launched in DUMMY auth mode

    @BeforeAll
    static void setup(){
        setupSpaceAndRelatedResources(new NakshaTestWebClient(), "Auth/setup");
    }

    private static final String SPACE_ID = "auth_test_space";

    @Test
    public void testDummyModeNoJWT() throws Exception {
        final String streamId = UUID.randomUUID().toString();
        // Providing no JWT, the master token should be employed automatically
        HttpResponse<String> response = getNakshaClient().get("hub/storages", streamId);
        assertThat(response).hasStatus(200);
    }

    @Test
    public void testDummyModeAppIdAuthorExistsInXyzNamespace() throws Exception {
        final String streamId = UUID.randomUUID().toString();
        final String bodyJson = loadFileOrFail("Auth/JWTAppIdAuthorExists/create_features.json");
        final String expectedBodyPart = loadFileOrFail("Auth/JWTAppIdAuthorExists/feature_response_part.json");

        final String jwtClaims = TestUtil.loadFileOrFail("Auth/validJwtClaims.json");
        // Sign the following JWT payload
        final String jwt = generateJWT(jwtClaims);
        HttpResponse<String> response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId,"Bearer "+jwt);

        // Then: Perform assertions
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match");
    }

    @Test
    public void testDummyModeInvalidJWT() throws Exception {
        final String streamId = UUID.randomUUID().toString();
        // Providing an invalid JWT, should return HTTP 401 code
        HttpResponse<String> response = getNakshaClient().get("hub/storages", streamId, "Bearer rdzftugzhkjn");
        assertThat(response).hasStatus(401);
    }

    @Test
    public void testDummyModeJWTSignedByUnknownSymmetricKey() throws Exception {
        final String streamId = UUID.randomUUID().toString();
        // Providing an invalid JWT, should return HTTP 401 code
        HttpResponse<String> response = getNakshaClient().get("hub/storages", streamId, "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c");
        assertThat(response).hasStatus(401);
    }

    @Test
    public void testDummyModeJWTSignedByUnknownKey() throws Exception {
        final String streamId = UUID.randomUUID().toString();
        final String jwtClaims = TestUtil.loadFileOrFail("Auth/validJwtClaims.json");
        // Sign the following JWT payload
        final String jwt = generateJWT(jwtClaims,"unit_test_data/Auth/test_keys/rsa256");
        // Providing an invalid JWT, should return HTTP 401 code
        HttpResponse<String> response = getNakshaClient().get("hub/storages", streamId, "Bearer "+jwt);
        assertThat(response).hasStatus(401);
    }

    @Test
    public void testDummyModeValidJWT() throws Exception {
        final String streamId = UUID.randomUUID().toString();
        final String jwtClaims = TestUtil.loadFileOrFail("Auth/validJwtClaims.json");
        // Sign the following JWT payload
        final String jwt = generateJWT(jwtClaims);
        // Providing a valid JWT, should success
        HttpResponse<String> response = getNakshaClient().get("hub/storages", streamId, "Bearer "+jwt);
        assertThat(response).hasStatus(200);
    }

    @Test
    public void testDummyModeExpiredJWT() throws Exception {
        final String streamId = UUID.randomUUID().toString();
        final String jwtClaims = TestUtil.loadFileOrFail("Auth/expiredJwtClaims.json");
        // Sign the following JWT payload
        final String jwt = generateJWT(jwtClaims);
        // Providing an expired JWT, should fail
        HttpResponse<String> response = getNakshaClient().get("hub/storages", streamId, "Bearer "+jwt);
        assertThat(response).hasStatus(401);
    }
}