/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import static com.jayway.restassured.config.DecoderConfig.decoderConfig;
import static com.jayway.restassured.config.EncoderConfig.encoderConfig;

import com.here.xyz.hub.auth.TestAuthenticator;
import com.jayway.restassured.RestAssured;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class RestAssuredTest extends TestAuthenticator {

    @BeforeClass
    public static void configureRestAssured() {
        RestAssured.baseURI = RestAssuredConfig.config().baseURI;
        RestAssured.port = RestAssuredConfig.config().port;
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
        RestAssured.config = RestAssured.config().httpClient(RestAssured.config().getHttpClientConfig().reuseHttpClientInstance());
        RestAssured.config = RestAssured.config().encoderConfig(encoderConfig().defaultContentCharset("UTF-8"));
        RestAssured.config = RestAssured.config().decoderConfig(decoderConfig().defaultContentCharset("UTF-8"));
    }

    @AfterClass
    public static void unconfigureRestAssured() {
        RestAssured.reset();
    }
}
