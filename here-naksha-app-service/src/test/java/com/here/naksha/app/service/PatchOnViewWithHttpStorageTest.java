package com.here.naksha.app.service;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.here.naksha.app.common.CommonApiTestSetup.createHandler;
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

@WireMockTest(httpPort = 9094)
public class PatchOnViewWithHttpStorageTest extends ApiTest {

    private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();
    private static final String PSQL_SPACE_ID = "patch_on_view_test_psql_space";
    private static final String VIEW_SPACE_ID = "patch_on_view_test_view_space";
    private static final String ENDPOINT = "/my_env/my_storage/my_feat_type/features";

    @BeforeAll
    static void setup() throws URISyntaxException, IOException, InterruptedException {
        // Set up Base space - using Http Storage
        setupSpaceAndRelatedResources(nakshaClient, "PatchOnViewWithHttpStorage/setup/http_storage_space");
        // Set up Delta space - using Psql Storage
        createHandler(nakshaClient, "PatchOnViewWithHttpStorage/setup/psql_storage_space/create_sourceId_handler.json");
        setupSpaceAndRelatedResources(nakshaClient, "PatchOnViewWithHttpStorage/setup/psql_storage_space");
        // Set up View space - using above Delta and Base spaces
        setupSpaceAndRelatedResources(nakshaClient, "PatchOnViewWithHttpStorage/setup/view_space");
        // Load some test data in Delta space
        final String initialFeaturesJson = loadFileOrFail("PatchOnViewWithHttpStorage/setup/psql_storage_space/create_features.json");
        final HttpResponse<String> response = nakshaClient.post("hub/spaces/" + PSQL_SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
        assertThat(response).hasStatus(200);
    }

    @Test
    void tc01_testPatchForFeatureOnlyInBase() throws Exception {
        // This test is to validate that for a Patch request against a Feature in Base layer,
        // we are successfully able to read Base layer using Http Storage (and not Psql Storage)

        // Test API : POST /hub/spaces/{spaceId}/features
        // Given: input patch feature request and final expected response body
        final String streamId = UUID.randomUUID().toString();
        final String patchRequestJson = loadFileOrFail("PatchOnViewWithHttpStorage/TC01_patchFeatureOnlyInBase/patch_request.json");
        final String expectedBodyPart = loadFileOrFail("PatchOnViewWithHttpStorage/TC01_patchFeatureOnlyInBase/response_body_part.json");

        // Given: Mock Http Response from Base space
        final String baseMockResponse =
                loadFileOrFail("PatchOnViewWithHttpStorage/TC01_patchFeatureOnlyInBase/base_mock_response.json");
        final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
        stubFor(get(endpointPath)
                .withQueryParam("id" , equalTo("my-custom-id-04"))
                .willReturn(okJson(baseMockResponse)));

        // When: Patch request is submitted on a View space to NakshaHub
        final HttpResponse<String> response = nakshaClient
                .post("hub/spaces/" + VIEW_SPACE_ID + "/features", patchRequestJson, streamId);

        // Then: Validate that the Base feature gets patched successfully
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Patch Feature response body doesn't match");

        verify(1, getRequestedFor(endpointPath));
    }

    @Test
    void tc02_testPatchForFeatureOnlyInDelta() throws Exception {
        // This test is to validate that for a Patch request against a Feature in Delta layer,
        // no-record-found situation from Base layer (using Http Storage) doesn't cause entire request to fail.

        // Test API : PATCH /hub/spaces/{spaceId}/features/{featureId}
        // Given: input patch feature request and final expected response body
        final String streamId = UUID.randomUUID().toString();
        final String featureId = "my-custom-id-01";
        final String patchRequestJson = loadFileOrFail("PatchOnViewWithHttpStorage/TC02_patchFeatureOnlyInDelta/patch_request.json");
        final String expectedBodyPart = loadFileOrFail("PatchOnViewWithHttpStorage/TC02_patchFeatureOnlyInDelta/response_body_part.json");

        // Given: Mock Http Response from Base space (no record found)
        final String baseMockResponse =
                loadFileOrFail("PatchOnViewWithHttpStorage/TC02_patchFeatureOnlyInDelta/base_mock_response.json");
        final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
        stubFor(get(endpointPath)
                .withQueryParam("id" , equalTo(featureId))
                .willReturn(okJson(baseMockResponse)));

        // When: Patch request is submitted on a View space to NakshaHub
        final HttpResponse<String> response = nakshaClient
                .patch("hub/spaces/" + VIEW_SPACE_ID + "/features/"+featureId, patchRequestJson, streamId);

        // Then: Validate that the Delta feature gets patched successfully
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Patch Feature response body doesn't match");
    }

}
