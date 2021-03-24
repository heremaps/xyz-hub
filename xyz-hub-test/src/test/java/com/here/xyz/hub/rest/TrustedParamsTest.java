package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrustedParamsTest extends TestWithSpaceCleanup {

  static final Map<String, String> exampleHeaders = new HashMap<String, String>() {{
    put("AuthN", "bearer abc");
    put("never", "none");
    put("X-request-ID", "REQ-123-456");
    put("custom_header", "my_custom_header_value");
  }};

  static final Map<String, String> exampleCookies = new HashMap<String, String>() {{
    put("here_access", "my_PRD_jwt_cookie");
    put("here_access_st", "my_DEV_jwt_cookie");
  }};

  @BeforeClass
  public static void beforeClass() {
    cleanUpId = "space-with-js-connector";
  }

  @Before
  public void before() {
    removeSpace(cleanUpId);
  }

  @Test
  public void untrustedConnector() {
    final String code = "if (event.getTrustedParams() == null) throw 'valid';";
    final String body = "{\"id\":\""+cleanUpId+"\",\"title\":\""+cleanUpId+"\",\"storage\":{\"id\":\"evaljs-untrusted\",\"params\":{\"code\":\""+code+"\"}}}";

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .cookies(exampleCookies)
        .headers(exampleHeaders)
        .body(body)
        .when()
        .post("/spaces")
        .then()
        .body("errorMessage", equalTo("valid"));
  }

  @Test
  public void trustedConnector() {
    final String code = "if (event.getTrustedParams() != null && event.getTrustedParams().getCookies().get('here_access') == 'my_PRD_jwt_cookie' && !event.getTrustedParams().getCookies().containsKey('here_access_st')) throw 'valid';";
    final String body = "{\"id\":\""+cleanUpId+"\",\"title\":\""+cleanUpId+"\",\"storage\":{\"id\":\"evaljs-trusted\",\"params\":{\"code\":\""+code+"\"}}}";

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .cookies(exampleCookies)
        .headers(exampleHeaders)
        .body(body)
        .when()
        .post("/spaces")
        .then()
        .body("errorMessage", equalTo("valid"));
  }
}
