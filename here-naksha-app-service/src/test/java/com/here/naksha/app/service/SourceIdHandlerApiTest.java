package com.here.naksha.app.service;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

import static com.here.naksha.app.common.CommonApiTestSetup.*;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

public class SourceIdHandlerApiTest extends ApiTest {

    private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();
    private static final String SPACE_ID = "source_id_handler_test_space";

    @BeforeAll
    static void setup() throws URISyntaxException, IOException, InterruptedException {
        createStorage(nakshaClient, "SourceHandlerId/setup/create_storage.json");
        createHandler(nakshaClient, "SourceHandlerId/setup/create_event_handler.json");
        createHandler(nakshaClient, "SourceHandlerId/setup/create_default_event_handler.json");
        createSpace(nakshaClient, "SourceHandlerId/setup/create_space.json");
        String initialFeaturesJson = loadFileOrFail("SourceHandlerId/setup/create_features.json");
        nakshaClient.post("hub/spaces/" + SPACE_ID + "/features", initialFeaturesJson, UUID.randomUUID().toString());
    }


    @Test
    void tc2000_testCreateFeaturesWithSourceIdInMeta() throws Exception {
        // Given:
        final String bodyJson = loadFileOrFail("SourceHandlerId/TC2000_createFeatureWithoutTag/create_feature_without_tag.json");
        final String expectedBodyPart = loadFileOrFail("SourceHandlerId/TC2000_createFeatureWithoutTag/feature_response_part.json");
        String streamId = UUID.randomUUID().toString();

        // When
        HttpResponse<String> response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

        // Then
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
                .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
                .hasInsertedIdsMatchingFeatureIds(null)
                .hasUuids();
    }
    @Test
    void tc2001_testCreateFeaturesWithSourceIdInMetaWithTags() throws Exception {
        // Given:
        final String bodyJson = loadFileOrFail("SourceHandlerId/TC2001_createFeatureWithTag/create_feature_with_tag.json");
        final String expectedBodyPart = loadFileOrFail("SourceHandlerId/TC2001_createFeatureWithTag/feature_response_part.json");
        String streamId = UUID.randomUUID().toString();

        // When
        HttpResponse<String> response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);

        // Then
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
                .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
                .hasInsertedIdsMatchingFeatureIds(null)
                .hasUuids();
    }
    @Test
    void tc2002_testCreateFeaturesWithoutSourceIdButWithSourceIdTag() throws Exception {
        // Given:
        final String bodyJson = loadFileOrFail("SourceHandlerId/TC2002_createFeatureWithoutSourceId/create_feature.json");
        final String expectedBodyPart = loadFileOrFail("SourceHandlerId/TC2002_createFeatureWithoutSourceId/feature_response_part.json");
        String streamId = UUID.randomUUID().toString();

        // When
        HttpResponse<String> response = getNakshaClient().post("hub/spaces/" + SPACE_ID + "/features", bodyJson, streamId);
        // Then
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Create Feature response body doesn't match")
                .hasInsertedCountMatchingWithFeaturesInRequest(bodyJson)
                .hasInsertedIdsMatchingFeatureIds(null)
                .hasUuids();
    }

   @Test
    void tc2003_searchBySourceId() throws Exception {
        //given
        final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
        final String propQueryParam = "%s=eq=%s".formatted(
                urlEncoded("p.@ns:com:here:mom:meta.sourceId"),
                urlEncoded("task_2")
        );

        final String expectedBodyPart =
                loadFileOrFail("SourceHandlerId/TC2003_searchBySourceId/feature_response_part.json");
        String streamId = UUID.randomUUID().toString();

        // When
        HttpResponse<String> response = nakshaClient
                .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam , streamId);

        // Then
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
    }

    @Test
    void tc2004_searchWithoutSourceId() throws Exception {
        //given
        final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
        final String propQueryParam = "p.speedLimit='60'";

        final String expectedBodyPart =
                loadFileOrFail("SourceHandlerId/TC2004_searchWithoutSourceId/feature_response_part.json");
        String streamId = UUID.randomUUID().toString();

        // When
        HttpResponse<String> response = nakshaClient
                .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam , streamId);

        // Then
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
    }

    @Test
    void tc2005_searchBySourceIdAndTags() throws Exception {
        //given
        final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";

        final String propQueryParam = "%s=eq=%s&tags=three".formatted(
                urlEncoded("p.@ns:com:here:mom:meta.sourceId"),
                urlEncoded("task_4")
        );

        final String expectedBodyPart =
                loadFileOrFail("SourceHandlerId/TC2005_searchBySourceIdAndTags/feature_response_part.json");
        String streamId = UUID.randomUUID().toString();

        // When
        HttpResponse<String> response = nakshaClient
                .get("hub/spaces/" + SPACE_ID + "/bbox?" + bboxQueryParam + "&" + propQueryParam , streamId);

        // Then
        assertThat(response)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedBodyPart, "Get Feature response body doesn't match");
    }
}
