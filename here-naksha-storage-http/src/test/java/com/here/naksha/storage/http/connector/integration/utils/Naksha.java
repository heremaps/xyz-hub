package com.here.naksha.storage.http.connector.integration.utils;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;

public class Naksha {

    private static final String NAKSHA_SPACE = System.getenv("nakshaSpace");

    public static RequestSpecification request() {
        return RestAssured.given().baseUri("http://localhost:8080/hub/spaces/" + NAKSHA_SPACE)
          .log().ifValidationFails();
    }
}
