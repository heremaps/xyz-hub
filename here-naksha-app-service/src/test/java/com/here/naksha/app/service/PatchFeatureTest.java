package com.here.naksha.app.service;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.UUID;

import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PatchFeatureTest extends ApiTest {

    private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

    private static final String SPACE_ID = "patch_features_test_space";

    public PatchFeatureTest() {
        super(nakshaClient);
    }

    @BeforeAll
    static void setup() {
        setupSpaceAndRelatedResources(nakshaClient, "PatchFeatures/setup");
    }

    @Test
    void testPatchOneFeatureById() throws Exception {
        // Test API : PATCH /hub/spaces/{spaceId}/features/{featureId}
        // Given: initial features
        final String streamId = UUID.randomUUID().toString();
        final String createFeaturesJson = loadFileOrFail("PatchFeatures/testPatchOneFeatureById/create_features.json");
        HttpResponse<String> response = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeaturesJson, streamId);
        assertEquals(200, response.statusCode(), "ResCode mismatch, failure creating initial features");

        // When: request is submitted to NakshaHub Space Storage instance
        final String bodyJson = loadFileOrFail("PatchFeatures/testPatchOneFeatureById/patch_request.json");
        response = nakshaClient
                .patch(
                        "hub/spaces/" + SPACE_ID + "/features/feature-1-to-patch",
                        bodyJson,
                        streamId);

        // Then: Perform assertions
        final String expectedBodyPart = loadFileOrFail("PatchFeatures/testPatchOneFeatureById/response.json");
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Patch Feature response body doesn't match");
    }

    @Test
    void testPatchOneFeatureByIdNotExisting() throws Exception {
        // Test API : PATCH /hub/spaces/{spaceId}/features/{featureId}
        // Given: initial features
        final String streamId = UUID.randomUUID().toString();

        // When: request is submitted to NakshaHub Space Storage instance
        final String bodyJson = loadFileOrFail("PatchFeatures/testPatchOneFeatureByIdNotExisting/patch_request.json");
        HttpResponse<String> response = nakshaClient
                .patch(
                        "hub/spaces/" + SPACE_ID + "/features/feature-2-to-patch",
                        bodyJson,
                        streamId);

        // Then: Perform assertions
        final String expectedBodyPart = loadFileOrFail("PatchFeatures/testPatchOneFeatureByIdNotExisting/response.json");
        assertThat(response)
                .hasStatus(404)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Patch Feature error response body doesn't match");
    }

    @Test
    void testPatchOneFeatureByIdWrongUuid() throws Exception {
        // Test API : PATCH /hub/spaces/{spaceId}/features/{featureId}
        // Given: initial features
        final String streamId = UUID.randomUUID().toString();
        final String createFeaturesJson = loadFileOrFail("PatchFeatures/testPatchOneFeatureByIdWrongUuid/create_features.json");
        HttpResponse<String> response = nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", createFeaturesJson, streamId);
        assertEquals(200, response.statusCode(), "ResCode mismatch, failure creating initial features");

        // When: request is submitted to NakshaHub Space Storage instance
        final String bodyJson = loadFileOrFail("PatchFeatures/testPatchOneFeatureByIdWrongUuid/patch_request.json");
        response = nakshaClient
                .patch(
                        "hub/spaces/" + SPACE_ID + "/features/feature-3-to-patch",
                        bodyJson,
                        streamId);

        // Then: Perform assertions
        final String expectedBodyPart = loadFileOrFail("PatchFeatures/testPatchOneFeatureByIdWrongUuid/response.json");
        assertThat(response)
                .hasStatus(409)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Patch Feature error response body doesn't match");
    }
}
