/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.restassured.http.ContentType.JSON;

import com.here.xyz.models.geojson.implementation.Feature;
import io.restassured.RestAssured;
import io.restassured.config.EncoderConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class FeatureModificationIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void beforeClass() {
    RestAssured.config = RestAssured.config().encoderConfig(EncoderConfig.encoderConfig()
        .encodeContentTypeAs("application/vnd.here.feature-modification-list", JSON));
  }

  @Before
  public void before() {
    remove();
    createSpace();
  }

  @After
  public void after() {
    remove();
  }

  @Test
  public void invalidIfNotExistsValue() {
    writeFeature(new Feature().withId("F1"), "invalid", "patch", "error")
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void invalidIfExistsValue() {
    writeFeature(new Feature().withId("F1"), "create", "invalid", "error")
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void invalidConflictResolutionValue() {
    writeFeature(new Feature().withId("F1"), "create", "patch", "invalid")
        .statusCode(BAD_REQUEST.code());
  }
}
