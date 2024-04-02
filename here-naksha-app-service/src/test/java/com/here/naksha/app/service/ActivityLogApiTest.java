package com.here.naksha.app.service;

import com.here.naksha.app.common.ApiTest;
import com.here.naksha.app.common.CommonApiTestSetup;
import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestUtil;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import static com.here.naksha.app.common.CommonApiTestSetup.*;
import static com.here.naksha.app.common.TestUtil.urlEncoded;
import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityLogApiTest extends ApiTest {

  private static final NakshaTestWebClient nakshaClient = new NakshaTestWebClient();
  private static final String REGULAR_SPACE_ID = "regular_space_ah";
  private static final String ACTIVITY_SPACE_ID = "activity_history_space";

  @BeforeAll
  static void setup() throws Exception {
    setupSpaceAndRelatedResources(nakshaClient, "ActivityLog/setup/regularSpace");
    createHandler(nakshaClient, "ActivityLog/setup/activityLogSpace/create_event_handler.json");
    createSpace(nakshaClient, "ActivityLog/setup/activityLogSpace/create_space.json");
  }

  @Test
  void tc1300_testActivityLogAfterCreateByUuid() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1300_afterCreateByUuid/create_features.json");
    String expectedGetResponse = TestUtil.loadFileOrFail("ActivityLog/TC1300_afterCreateByUuid/get_response.json");
    String streamId = UUID.randomUUID().toString();

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    assertThat(createResp).hasStatus(200);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());

    // And: Client queries activity log space for this feature
    HttpResponse<String> getActivityResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/features/" + createdFeature.uuid,
        streamId);

    // Then: Activity response is valid and conveys expected data
    assertThat(getActivityResp)
        .hasStreamIdHeader(streamId)
        .hasStatus(200)
        .hasJsonBody(formattedJson(expectedGetResponse, Map.of(
            "${id}", createdFeature.uuid,
            "${activityLogId}", "TC1300_feature",
            "\"${createdAt}\"", createdFeature.createdAt,
            "\"${updatedAt}\"", createdFeature.updatedAt
        )));
  }

  @Test
  void tc1301_testActivityLogAfterCreateByFeatureId() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1301_afterCreateByFeatureId/create_features.json");
    String expectedGetResponse = TestUtil.loadFileOrFail("ActivityLog/TC1301_afterCreateByFeatureId/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String featureId = "TC1301_feature";

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    assertThat(createResp).hasStatus(200);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());

    // And: Client queries activity log space for this feature
    String featureIdNamespaceQuery = urlEncoded("p.@ns:com:here:xyz:log.id") + "=" + featureId;
    HttpResponse<String> getActivityResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/search?" + featureIdNamespaceQuery,
        streamId);

    // Then: Activity response is valid and conveys expected data
    assertThat(getActivityResp)
        .hasStreamIdHeader(streamId)
        .hasStatus(200)
        .hasJsonBody(formattedJson(expectedGetResponse, Map.of(
            "${id}", createdFeature.uuid,
            "${activityLogId}", featureId,
            "\"${createdAt}\"", createdFeature.createdAt,
            "\"${updatedAt}\"", createdFeature.updatedAt
        )));
  }

  @Test
  void tc1302_testActivityLogAfterUpdateByUuid() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1302_afterUpdateByUuid/create_features.json");
    String updateFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1302_afterUpdateByUuid/update_feature.json");
    String expectedActivityResp = TestUtil.loadFileOrFail("ActivityLog/TC1302_afterUpdateByUuid/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String featureId = "TC1302_feature";

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());
    assertThat(createResp).hasStatus(200);

    // And: This feature is updated
    HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, updateFeatureJson,
        streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata updatedFeature = featureMetadataFromFeatureResp(updateResp.body());

    // And: Client queries activity log space for this feature
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/features/" + updatedFeature.uuid, streamId);

    // Then: Expected ActivityLog response matches the response
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(formattedJson(expectedActivityResp, Map.of(
            "${id}", updatedFeature.uuid,
            "${activityLogId}", featureId,
            "${puuid}", createdFeature.uuid,
            "\"${createdAt}\"", updatedFeature.createdAt,
            "\"${updatedAt}\"", updatedFeature.updatedAt
        )));
  }

  @Test
  void tc1303_testActivityLogAfterUpdateByFeatureId() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1303_afterUpdateByFeatureId/create_features.json");
    String updateFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1303_afterUpdateByFeatureId/update_feature.json");
    String expectedActivityResp = TestUtil.loadFileOrFail("ActivityLog/TC1303_afterUpdateByFeatureId/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String featureId = "TC1303_feature";

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    assertThat(createResp).hasStatus(200);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());

    // And: This feature is updated
    HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, updateFeatureJson,
        streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata updatedFeature = featureMetadataFromFeatureResp(updateResp.body());

    // And: Client queries activity log space for this feature
    String featureIdNamespaceQuery = urlEncoded("p.@ns:com:here:xyz:log.id") + "=" + featureId;
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/search?" + featureIdNamespaceQuery, streamId);

    // Then: Expected ActivityLog response matches the response
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(formattedJson(expectedActivityResp, Map.of(
            "${firstId}", updatedFeature.uuid,
            "${firstPuuid}", createdFeature.uuid,
            "\"${firstCreatedAt}\"", updatedFeature.createdAt,
            "\"${firstUpdatedAt}\"", updatedFeature.updatedAt,
            "${secondId}", createdFeature.uuid,
            "${activityLogId}", featureId,
            "\"${secondCreatedAt}\"", createdFeature.createdAt,
            "\"${secondUpdatedAt}\"", createdFeature.updatedAt
        )));
  }

  @Test
  void tc1304_testActivityLogAfterDeleteByUuid() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1304_afterDeleteByUuid/create_features.json");
    String updateFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1304_afterDeleteByUuid/update_feature.json");
    String expectedActivityResp = TestUtil.loadFileOrFail("ActivityLog/TC1304_afterDeleteByUuid/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String featureId = "TC1304_feature";

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    assertThat(createResp).hasStatus(200);

    // And: This feature is updated
    HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, updateFeatureJson,
        streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata updatedFeature = featureMetadataFromFeatureResp(updateResp.body());

    // And: This feature is deleted
    HttpResponse<String> deleteResp = nakshaClient.delete("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, streamId);
    assertThat(deleteResp).hasStatus(200);
    FeatureMetadata deletedFeature = featureMetadataFromFeatureResp(deleteResp.body());

    // And: Client queries activity log space for this feature
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/features/" + deletedFeature.uuid, streamId);

    // Then: Expected ActivityLog response matches the response
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(formattedJson(expectedActivityResp, Map.of(
            "${id}", deletedFeature.uuid,
            "${activityLogId}", featureId,
            "${puuid}", updatedFeature.uuid,
            "\"${createdAt}\"", deletedFeature.createdAt,
            "\"${updatedAt}\"", deletedFeature.updatedAt
        )));
  }

  @Test
  void tc1305_testActivityLogAfterDeleteByFeatureId() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1305_afterDeleteByFeatureId/create_features.json");
    String updateFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1305_afterDeleteByFeatureId/update_feature.json");
    String expectedActivityResp = TestUtil.loadFileOrFail("ActivityLog/TC1305_afterDeleteByFeatureId/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String featureId = "TC1305_feature";

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    assertThat(createResp).hasStatus(200);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());

    // And: This feature is updated
    HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, updateFeatureJson,
        streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata updatedFeature = featureMetadataFromFeatureResp(updateResp.body());

    // And: This feature is deleted
    HttpResponse<String> deleteResp = nakshaClient.delete("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata deletedFeature = featureMetadataFromFeatureResp(deleteResp.body());

    // And: Client queries activity log space for this feature
    String featureIdNamespaceQuery = urlEncoded("p.@ns:com:here:xyz:log.id") + "=" + featureId;
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/search?" + featureIdNamespaceQuery, streamId);

    // Then: Expected ActivityLog response matches the response
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(formattedJson(expectedActivityResp, mapOf(
            "${firstId}", deletedFeature.uuid,
            "${firstPuuid}", updatedFeature.uuid,
            "\"${firstCreatedAt}\"", deletedFeature.createdAt,
            "\"${firstUpdatedAt}\"", deletedFeature.updatedAt,
            "${secondId}", updatedFeature.uuid,
            "${secondPuuid}", createdFeature.uuid,
            "\"${secondCreatedAt}\"", updatedFeature.createdAt,
            "\"${secondUpdatedAt}\"", updatedFeature.updatedAt,
            "${thirdId}", createdFeature.uuid,
            "\"${thirdCreatedAt}\"", createdFeature.createdAt,
            "\"${thirdUpdatedAt}\"", createdFeature.updatedAt,
            "${activityLogId}", featureId
        )));
  }

  @Test
  void tc1306_testActivityLogWithSourceId() throws URISyntaxException, IOException, InterruptedException {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1306_withSourceId/create_features.json");
    String updateFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1306_withSourceId/update_feature.json");
    String expectedActivityResp = TestUtil.loadFileOrFail("ActivityLog/TC1306_withSourceId/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String sourceIdSpace = "source_id_ah_test_space";
    String activityLogSpace = "activity_history_space_source_id_tests";
    String featureId = "TC1306_feature";

    // And: space with sourceId handling
    CommonApiTestSetup.createHandler(nakshaClient, "ActivityLog/TC1306_withSourceId/sourceIdSpace/create_default_handler.json");
    CommonApiTestSetup.createHandler(nakshaClient, "ActivityLog/TC1306_withSourceId/sourceIdSpace/create_source_id_handler.json");
    CommonApiTestSetup.createSpace(nakshaClient, "ActivityLog/TC1306_withSourceId/sourceIdSpace/create_space.json");

    // And: space with activity log that is based on sourceId handling space
    CommonApiTestSetup.createHandler(nakshaClient, "ActivityLog/TC1306_withSourceId/activityLogSpace/create_event_handler.json");
    CommonApiTestSetup.createSpace(nakshaClient, "ActivityLog/TC1306_withSourceId/activityLogSpace/create_space.json");

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + sourceIdSpace + "/features", createFeatureJson, streamId);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());
    assertThat(createResp).hasStatus(200);

    // And: This feature is updated
    HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + sourceIdSpace + "/features/" + featureId, updateFeatureJson,
        streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata updatedFeature = featureMetadataFromFeatureResp(updateResp.body());

    // And: Client queries activity log space for this feature
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + activityLogSpace + "/features/" + updatedFeature.uuid, streamId);

    // Then: Expected ActivityLog response matches the response
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(formattedJson(expectedActivityResp, Map.of(
            "${uuid}", updatedFeature.uuid,
            "${puuid}", createdFeature.uuid,
            "\"${createdAt}\"", updatedFeature.createdAt,
            "\"${updatedAt}\"", updatedFeature.updatedAt
        )));
  }

  @Test
  void tc1307_testActivityLogByBBox() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1307_byBbox/create_features.json");
    String updateFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1307_byBbox/update_feature.json");
    String expectedActivityResp = TestUtil.loadFileOrFail("ActivityLog/TC1307_byBbox/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String featureId = "TC1307_feature";

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());
    assertThat(createResp).hasStatus(200);

    // And: This feature is updated
    HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, updateFeatureJson,
        streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata updatedFeature = featureMetadataFromFeatureResp(updateResp.body());

    // And: Client queries activity log space against new tags and bbox
    final String bboxQueryParam = "west=-180&south=-90&east=180&north=90";
    final String tagsQueryParam = "tags=tc1307+two";
    HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/bbox?" + tagsQueryParam + "&" + bboxQueryParam,
        streamId);

    // Then: Expected ActivityLog response matches the response
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(formattedJson(expectedActivityResp, Map.of(
            "${uuid}", updatedFeature.uuid,
            "${puuid}", createdFeature.uuid,
            "\"${createdAt}\"", updatedFeature.createdAt,
            "\"${updatedAt}\"", updatedFeature.updatedAt
        )));
  }

  @Test
  void tc1308_testActivityLogByTile() throws Exception {
    // Given: Test files
    String createFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1308_byTile/create_features.json");
    String updateFeatureJson = TestUtil.loadFileOrFail("ActivityLog/TC1308_byTile/update_feature.json");
    String expectedActivityResp = TestUtil.loadFileOrFail("ActivityLog/TC1308_byTile/get_response.json");
    String streamId = UUID.randomUUID().toString();
    String featureId = "TC1308_feature";

    // When: New feature is created
    HttpResponse<String> createResp = nakshaClient.post("hub/spaces/" + REGULAR_SPACE_ID + "/features", createFeatureJson, streamId);
    FeatureMetadata createdFeature = featureMetadataFromCollectionResp(createResp.body());
    assertThat(createResp).hasStatus(200);

    // And: This feature is updated
    HttpResponse<String> updateResp = nakshaClient.put("hub/spaces/" + REGULAR_SPACE_ID + "/features/" + featureId, updateFeatureJson,
        streamId);
    assertThat(updateResp).hasStatus(200);
    FeatureMetadata updatedFeature = featureMetadataFromFeatureResp(updateResp.body());

    // And: Client queries activity log space against new tags and bbox
    final String tagsQueryParam = "tags=tc1308+two";
    final HttpResponse<String> getResp = nakshaClient.get("hub/spaces/" + ACTIVITY_SPACE_ID + "/tile/quadkey/1?"+ tagsQueryParam, streamId);

    // Then: Expected ActivityLog response matches the response
    assertThat(getResp)
        .hasStatus(200)
        .hasStreamIdHeader(streamId)
        .hasJsonBody(formattedJson(expectedActivityResp, Map.of(
            "${uuid}", updatedFeature.uuid,
            "${puuid}", createdFeature.uuid,
            "\"${createdAt}\"", updatedFeature.createdAt,
            "\"${updatedAt}\"", updatedFeature.updatedAt
        )));
  }

  private String formattedJson(String json, Map<String, Object> propsToOverride) {
    for (Entry<String, Object> entry : propsToOverride.entrySet()) {
      json = json.replace(entry.getKey(), entry.getValue().toString());
    }
    return json;
  }

  private FeatureMetadata featureMetadataFromFeatureResp(String featureResponse) {
    XyzFeature feature = JsonSerializable.deserialize(featureResponse, XyzFeature.class);
    return FeatureMetadata.from(feature.getProperties().getXyzNamespace());
  }

  private FeatureMetadata featureMetadataFromCollectionResp(String featureCollectionResponseJson) {
    List<FeatureMetadata> featuresMetadata = featuresMetadata(featureCollectionResponseJson);
    assertEquals(1, featuresMetadata.size(), "Expected single contained 0/multiple features");
    return featuresMetadata.get(0);
  }

  private List<FeatureMetadata> featuresMetadata(String featureCollectionResponseJson) {
    return JsonSerializable.deserialize(featureCollectionResponseJson, XyzFeatureCollection.class)
        .getFeatures().stream()
        .map(XyzFeature::getProperties)
        .map(XyzProperties::getXyzNamespace)
        .map(FeatureMetadata::from)
        .toList();
  }

  private static Map mapOf(Object... args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException("Expected even number of args (key-value pairs!)");
    }
    Map map = new HashMap();
    for (int i = 0; i < args.length; i += 2) {
      map.put(args[i], args[i + 1]);
    }
    return map;
  }

  private record FeatureMetadata(String uuid, long createdAt, long updatedAt) {

    static FeatureMetadata from(XyzNamespace xyzNamespace) {
      return new FeatureMetadata(xyzNamespace.getUuid(), xyzNamespace.getCreatedAt(), xyzNamespace.getUpdatedAt());
    }
  }
}
