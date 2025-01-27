package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BBoxTest {

    @BeforeEach
    void rmFeatures() {
        rmAllFeatures();
    }

    @Test
    void bbox_notContains() {
        DataHub.createFeatureFromJsonFile("bbox/feature_1.json");

        String bbox = "bbox?west=-1&north=0&east=0&south=-3";
        Response dhResponse = DataHub.request().get(bbox);
        Response nResponse = Naksha.request().get(bbox);
        assertSameIds(dhResponse, nResponse);

        List<Map> features = dhResponse.body().jsonPath().getList("features", Map.class);
        assertEquals(0, features.size());
    }

    @Test
    void bbox_contains() {
        DataHub.createFeatureFromJsonFile("bbox/feature_1.json");

        String bbox = "bbox?west=-3&north=0&east=0&south=-1";
        Response dhResponse = DataHub.request().get(bbox);
        Response nResponse = Naksha.request().get(bbox);
        assertSameIds(dhResponse, nResponse);

        List<Map> features = dhResponse.body().jsonPath().getList("features", Map.class);
        assertEquals(1, features.size());
    }


}
