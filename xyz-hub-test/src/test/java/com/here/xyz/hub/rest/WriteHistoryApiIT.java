/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.hub.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class WriteHistoryApiIT extends UpdateFeatureApiIT {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    createSpace(true);
    addFeatures();
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void updateObjectWithHeadUUID() throws JsonProcessingException {
    String body = given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .get(getSpacesPath() + "/x-psql-test/search?f.id=Q3495887&force2d=true")
            .getBody().asString();

    FeatureCollection fc = XyzSerializable.deserialize(body);
    Feature f1 = readFeatureFromFC(fc, true);

    FeatureCollection mfc = new FeatureCollection().withFeatures(new ArrayList<Feature>(){{add(f1);}});

    /** UPDATE FEATURE with INSERT UUID  */
    body = given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(mfc.serialize())
            .when()
            .post(getSpacesPath() + "/x-psql-test/features")
            .getBody().asString();

    fc = XyzSerializable.deserialize(body);
    Feature f2 = readFeatureFromFC(fc, false);

    assertEquals(f1.getProperties().getXyzNamespace().getUuid() ,f2.getProperties().getXyzNamespace().getPuuid());
    assertNotNull(f2.getProperties().getXyzNamespace().getUuid());
    assertNull(f2.getProperties().getXyzNamespace().getMuuid());

    mfc = new FeatureCollection().withFeatures(new ArrayList<Feature>(){{add(f2);}});

    /** UPDATE FEATURE with INSERT HEAD-UUID */
    given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(mfc.serialize())
            .when()
            .post(getSpacesPath() + "/x-psql-test/features")
            .then().statusCode(OK.code())
            .body("features[0].properties.@ns:com:here:xyz.uuid", notNullValue())
            .body("features[0].properties.@ns:com:here:xyz.muuid", nullValue())
            .body("features[0].properties.@ns:com:here:xyz.puuid", equalTo(f2.getProperties().getXyzNamespace().getUuid()))
            .body("features[0].id", equalTo("Q3495887"));
  }

  @Test
  public void updateObjectWithOldUUID() throws JsonProcessingException {
    String body = given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .get(getSpacesPath() + "/x-psql-test/search?f.id=Q3495887&force2d=true")
            .getBody().asString();

    Feature f1 = readFeatureFromFC(XyzSerializable.deserialize(body), true);
    FeatureCollection mfc = new FeatureCollection().withFeatures(new ArrayList<Feature>(){{add(f1);}});

    /** UPDATE FEATURE with INSERT UUID*/
    given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(mfc.serialize())
            .when()
            .post(getSpacesPath() + "/x-psql-test/features")
            .then().statusCode(OK.code())
            .body("features[0].properties.@ns:com:here:xyz.uuid", notNullValue())
            .body("features[0].properties.@ns:com:here:xyz.muuid", nullValue())
            .body("features[0].properties.@ns:com:here:xyz.puuid", equalTo(f1.getProperties().getXyzNamespace().getUuid()))
            .body("features[0].id", equalTo("Q3495887"));

    /** UPDATE FEATURE AGAIN with INSERT UUID - now we get a merge */
    given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(mfc.serialize())
            .when()
            .post(getSpacesPath() + "/x-psql-test/features")
            .then().statusCode(OK.code())
            .body("features[0].properties.@ns:com:here:xyz.uuid", notNullValue())
            .body("features[0].properties.@ns:com:here:xyz.muuid", notNullValue())
            .body("features[0].properties.@ns:com:here:xyz.puuid", not(f1.getProperties().getXyzNamespace().getUuid()))
            .body("features[0].id", equalTo("Q3495887"));
  }

  @Test
  public void updateDeletedObjectWithUUID() throws JsonProcessingException {
    String body = given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .get(getSpacesPath() + "/x-psql-test/search?f.id=Q3495887&force2d=true")
            .getBody().asString();

    Feature f1 = readFeatureFromFC(XyzSerializable.deserialize(body), true);
    FeatureCollection mfc = new FeatureCollection().withFeatures(new ArrayList<Feature>(){{add(f1);}});

    /** Delete Feature */
    given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .delete(getSpacesPath() + "/x-psql-test/features?id=Q3495887")
            .then().statusCode(OK.code());

    /** UPDATE FEATURE with INSERT UUID*/
    given()
            .accept(APPLICATION_GEO_JSON)
            .contentType(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(mfc.serialize())
            .when()
            .post(getSpacesPath() + "/x-psql-test/features")
            .then().statusCode(CONFLICT.code());
  }

  private Feature readFeatureFromFC(FeatureCollection fc, boolean puuidShouldBeNull ) throws JsonProcessingException {
    Feature f1 = fc.getFeatures().get(0);

    if(puuidShouldBeNull)
      assertNull(f1.getProperties().getXyzNamespace().getPuuid());
    else
      assertNotNull(f1.getProperties().getXyzNamespace().getPuuid());

    assertNotNull(f1.getProperties().getXyzNamespace().getUuid());
    return f1;
  }

  @Ignore
  public void validateFeature() { }

  @Ignore
  public void patchFeatureWithUUIDnonUUIDSpace() throws Exception {}

  @Ignore
  public void postFeatureWithUUIDnonUUIDSpace() throws Exception {}
}
