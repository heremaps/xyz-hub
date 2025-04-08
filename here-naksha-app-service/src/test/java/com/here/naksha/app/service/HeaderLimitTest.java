package com.here.naksha.app.service;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaAppInjection;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.assertions.ResponseAssertions;
import com.here.naksha.lib.hub.NakshaHubConfig;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

import static com.here.naksha.app.common.CommonApiTestSetup.setupSpaceAndRelatedResources;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;

public class HeaderLimitTest extends ApiTest {

    private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();

    private static final String SPACE_ID = "header_limit_test_space";

    @BeforeAll
    static void setup() throws URISyntaxException, IOException, InterruptedException {
        setupSpaceAndRelatedResources(nakshaClient, "HeaderLimit/setup");
        final String initialFeaturesJson = loadFileOrFail("HeaderLimit/setup/create_features.json");
        nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
    }

    @Test
    void tc001_testGetByIdsSuccess(final @NakshaAppInjection NakshaApp nakshaApp) throws Exception {
        // Test API : GET /hub/spaces/{spaceId}/features
        // Validate features getting returned for existing Ids when header size is within configured limit
        // Given: Features By Ids request (against above space)
        final String idsQueryParam = "id=my-custom-id-001" + "&id=my-custom-id-002";
        final String expectedBodyPart =
                loadFileOrFail("HeaderLimit/TC001_GetByIds/feature_response_part.json");
        // When: Header size is just below limit (i.e. 100 bytes below limit)
        final NakshaHubConfig config = nakshaApp.getHub().getConfig();
        final String streamId = StringUtils.repeat("x", (config.requestHeaderLimit * 1024) - 100);

        // When: Get Features request is submitted to NakshaHub Space Storage instance
        HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features?" + idsQueryParam, streamId);

        // Then: Perform assertions that the request is successful
        ResponseAssertions.assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match")
                .hasUuids()
        ;
    }

    @Test
    void tc002_testGetByIdsWithHeaderLimitBreach(final @NakshaAppInjection NakshaApp nakshaApp) throws Exception {
        // Test API : GET /hub/spaces/{spaceId}/features
        // Validate Http error 413 when header size crosses the configured limit
        // Given: Features By Ids request (against above space)
        final String idsQueryParam = "id=my-custom-id-001" + "&id=my-custom-id-002";
        final String expectedBodyPart =
                loadFileOrFail("HeaderLimit/TC001_GetByIds/feature_response_part.json");
        // When: Header size breaches the limit
        final NakshaHubConfig config = nakshaApp.getHub().getConfig();
        final String streamId = StringUtils.repeat("x", config.requestHeaderLimit * 1024);

        // When: Get Features request is submitted to NakshaHub Space Storage instance
        HttpResponse<String> response = getNakshaClient().get("hub/spaces/" + SPACE_ID + "/features?" + idsQueryParam, streamId);

        // Then: Perform assertions that the request fails with header limit breach
        ResponseAssertions.assertThat(response)
                .hasStatus(431);
    }

}
