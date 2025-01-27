package com.here.naksha.storage.http.connector.integration.utils;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;

import java.util.Base64;

public class DataHub {

    private static final String SPACE = System.getenv("dataHubSpace");
    private static final String TOKEN_BASE_64 = System.getenv("dataHubToken");
    private static final String token = new String(Base64.getDecoder().decode(TOKEN_BASE_64));

    public static void createFeatureFromJsonFile(String pathInIntegrationResources) {
        Commons.createFeatureFromJsonFile(request(), pathInIntegrationResources);
    }

    public static void createFeatureFromJsonTemplateFile(String pathInIntegrationResources, String... args) {
        Commons.createFeatureFromJsonTemplateFile(request(), pathInIntegrationResources, args);
    }

    public static RequestSpecification request() {
        return RestAssured
                .given()
                .header("Authorization", "Bearer " + token)
                .baseUri("https://xyz.api.here.com/hub/spaces/" + SPACE)
                .log().ifValidationFails();
    }
}
