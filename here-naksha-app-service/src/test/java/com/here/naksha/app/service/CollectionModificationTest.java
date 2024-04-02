package com.here.naksha.app.service;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.UUID;

import static com.here.naksha.app.common.CommonApiTestSetup.*;
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;

public class CollectionModificationTest extends ApiTest {

    private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();
    private static final String REGULAR_SPACE_ID = "regular_collection_mod_test_space";
    private static final String ACTIVITY_SPACE_ID = "history_collection_mod_test_space";

    @BeforeAll
    static void setup() throws Exception {
        setupSpaceAndRelatedResources(nakshaClient, "CollectionModification/setup/regularSpace");
        createHandler(nakshaClient, "CollectionModification/setup/historySpace/create_event_handler.json");
        createSpace(nakshaClient, "CollectionModification/setup/historySpace/create_space.json");
    }

    @Test
    void tc1400_testActivityLogPerformanceAfterCollectionMod() throws Exception {
        // Given: Test files
        String createFeatureJson = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/create_features.json");
        String updateFeatureJson = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/update_feature.json");
        String createFeatureJson2 = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/create_features2.json");
        String updateFeatureJson2 = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/update_feature2.json");
        String updateSpaceJson1 = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/update_regular_space.json");
        String updateSpaceJson2 = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/update_regular_space2.json");
        String expectedActivityResp = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/get_response.json");
        String expectedActivityResp2 = TestUtil.loadFileOrFail("CollectionModification/TC1400_WriteCollectionSuccess/get_response2.json");
        String streamId = UUID.randomUUID().toString();
        String featureId = "TC1400_feature";
        String featureId2 = "TC1400_feature2";

        // Given: Regular space is updated to allow activity history and deletion backup to be stored
        HttpResponse<String> updateSpaceResp1 = nakshaClient.put("hub/spaces/"+REGULAR_SPACE_ID, updateSpaceJson1, streamId);
        assertThat(updateSpaceResp1).hasStatus(200);

        // When: New feature is created
        HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
        assertThat(createResp).hasStatus(200);

        // And: This feature is updated
        HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, updateFeatureJson,
                streamId);
        assertThat(updateResp).hasStatus(200);

        // And: This feature is deleted
        HttpResponse<String> deleteResp = nakshaClient.delete("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, streamId);
        assertThat(deleteResp).hasStatus(200);

        // And: Client queries activity log space for this feature
        String featureIdNamespaceQuery = urlEncoded("p.@ns:com:here:xyz:log.id") + "=" + featureId;
        HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/search?" + featureIdNamespaceQuery, streamId);

        // And: Expected ActivityLog response matches the response
        assertThat(getResp)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedActivityResp)
        ;

        // When: Regular space is updated to disable activity history and enable autoPurge
        HttpResponse<String> updateSpaceResp2 = nakshaClient.put("hub/spaces/"+REGULAR_SPACE_ID, updateSpaceJson2, streamId);
        assertThat(updateSpaceResp2).hasStatus(200);

        // Then:
        HttpResponse<String> createResp2 = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson2, streamId);
        assertThat(createResp2).hasStatus(200);

        // And: This feature is updated
        HttpResponse<String> updateResp2 = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId2, updateFeatureJson2,
                streamId);
        assertThat(updateResp2).hasStatus(200);

        // And: This feature is deleted
        HttpResponse<String> deleteResp2 = nakshaClient.delete("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId2, streamId);
        assertThat(deleteResp2).hasStatus(200);

        // And: Client queries activity log space for this feature
        String featureIdNamespaceQuery2 = urlEncoded("p.@ns:com:here:xyz:log.id") + "=" + featureId2;
        HttpResponse<String> getResp2 = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/search?" + featureIdNamespaceQuery2, streamId);

        // And: Expected ActivityLog response matches the response
        assertThat(getResp2)
                .hasStatus(200)
                .hasStreamIdHeader(streamId)
                .hasJsonBody(expectedActivityResp2);
    }
}
