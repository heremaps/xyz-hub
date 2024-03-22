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
import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

@WireMockTest(httpPort = 9090)
public class ViewTypeUnionTest extends ApiTest {

    private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

    private static final String HTTP_SPACE_ID = "base_view_union_test_http_space";
    private static final String PSQL_SPACE_ID = "delta_view_union_test_psql_space";
    private static final String VIEW_SPACE_ID = "view_union_view_space";

    private static final String ENDPOINT = "/view_union/test/bbox";


    @BeforeAll
    static void setup() throws URISyntaxException, IOException, InterruptedException {
        // Set up Http Storage based Space
        setupSpaceAndRelatedResources(nakshaClient, "ViewUnion/setup/http_storage_space");
        // Set up (standard) Psql Storage based Space
        setupSpaceAndRelatedResources(nakshaClient, "ViewUnion/setup/psql_storage_space");
        // Set up View Space over Psql and Http Storage based spaces
        setupSpaceAndRelatedResources(nakshaClient, "ViewUnion/setup/view_space");
        // Load some test data in PsqlStorage based Space
        final String initialFeaturesJson2 = loadFileOrFail("ViewUnion/setup/psql_storage_space/create_features.json");
        final HttpResponse<String> response2 = nakshaClient.post("hub/spaces/" + PSQL_SPACE_ID + "/features", initialFeaturesJson2, UUID.randomUUID().toString());
        assertThat(response2).hasStatus(200);
    }

    @Test
    void testGetByBBoxOnViewSpaceUnion() throws Exception {
        // Test API : GET /hub/spaces/{spaceId}/bbox
        // Validate features returned match with given BBox condition using View space over psql and http storage based spaces

        // Given: Features By BBox request (against view space)
        final String bboxQueryParam = "west=12.79&south=53.59&east=12.82&north=53.62";
        final String httpStorageMockResponse =
                loadFileOrFail("ViewUnion/ByBBox/http_storage_response.json");
        final String expectedViewResponse =
                loadFileOrFail("ViewUnion/ByBBox/feature_response_part.json");
        String streamId = UUID.randomUUID().toString();

        final UrlPattern endpointPath = urlPathEqualTo(ENDPOINT);
        stubFor(get(endpointPath)
                .withQueryParam("west", equalTo("12.79"))
                .withQueryParam("south", equalTo("53.59"))
                .withQueryParam("east", equalTo("12.82"))
                .withQueryParam("north", equalTo("53.62"))
                .willReturn(okJson(httpStorageMockResponse)));

        // When: Get Features By BBox request is submitted to NakshaHub
        HttpResponse<String> response = nakshaClient.get("hub/spaces/" + VIEW_SPACE_ID + "/bbox?" + bboxQueryParam, streamId);

        // Then: Perform assertions
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedViewResponse, "Get Feature response body doesn't match");

        // Then: Verify request reached endpoint once
        verify(1, getRequestedFor(endpointPath));
    }
}
