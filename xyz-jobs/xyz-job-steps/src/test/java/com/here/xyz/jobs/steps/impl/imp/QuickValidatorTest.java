/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl.imp;

import static org.junit.Assert.assertEquals;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace.Format;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
public class QuickValidatorTest {
    private static String TEST_PREXIF = "validation-test/";
    private static S3Client client;

    @BeforeClass
    public static void setup() throws URISyntaxException {
        new Config();
        Config.instance.JOBS_S3_BUCKET = "test-bucket";
        Config.instance.AWS_REGION = "us-east-1";
        Config.instance.HUB_ENDPOINT = "http://localhost:8080/hub";
        Config.instance.LOCALSTACK_ENDPOINT = new URI("http://localhost:4566");

        client = S3Client.getInstance();
        client.deleteFolder(TEST_PREXIF);
    }

    @Before
    public void cleanUp() {
        client.deleteFolder(TEST_PREXIF);
    }
    private String generateTestS3Key(String name){
        return TEST_PREXIF + name;
    }

    @Test
    public void testQuickValidation() throws BaseHttpServerVerticle.ValidationException {
        /** With no new line at end */
        client.putObject(generateTestS3Key("test_valid_1_jsonwkb.csv"),
                "text/csv",
                "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000".getBytes());

        client.putObject(generateTestS3Key("test_valid_1_geojson.csv"),
                "text/csv",
                "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\"".getBytes());
        client.putObject(generateTestS3Key("test_valid_1_geojson.txt"),
                "application/json",
                "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}".getBytes());

        /** With new line at end */
        client.putObject(generateTestS3Key("test_valid_2_jsonwkb.csv"),
                "text/csv",
                "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000\n".getBytes());
        client.putObject(generateTestS3Key("test_valid_2_geojson.csv"),
                "text/csv",
                "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\"\n".getBytes());
        client.putObject(generateTestS3Key("test_valid_2_geojson.txt"),
                "application/json",
                "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}\n".getBytes());

        /** Should not fail - above are all valid */
      ImportFilesQuickValidator.validate(generateTestS3Key("test_valid_1_jsonwkb.csv"), Format.CSV_JSONWKB, false);
      ImportFilesQuickValidator.validate(generateTestS3Key("test_valid_1_geojson.csv"), Format.CSV_GEOJSON, false);
      ImportFilesQuickValidator.validate(generateTestS3Key("test_valid_1_geojson.txt"), Format.GEOJSON, false);
      ImportFilesQuickValidator.validate(generateTestS3Key("test_valid_2_jsonwkb.csv"), Format.CSV_JSONWKB, false);
      ImportFilesQuickValidator.validate(generateTestS3Key("test_valid_2_geojson.csv"), Format.CSV_GEOJSON, false);
      ImportFilesQuickValidator.validate(generateTestS3Key("test_valid_2_geojson.txt"), Format.GEOJSON, false);
    }

    @Test
    public void testInvalidJson() {
        /** Invalid JSON */
        S3Client.getInstance().putObject(generateTestS3Key("test_invalid_1_jsonwkb.csv"),
                "text/csv",
                "\"{'\"properties'\": {invalid}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000".getBytes());

        S3Client.getInstance().putObject(generateTestS3Key("test_invalid_1_geojson.csv"),
                "text/csv",
                "\"{'\"type'\":'\"Feature'\" invalid }}\"".getBytes());

        S3Client.getInstance().putObject(generateTestS3Key("test_invalid_1_geojson.txt"),
                "application/json",
                "{\"type\":\"Featureinvaid\",\"geometry\":{\"type\":\"Pointinvalid\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}".getBytes());

        try{
            ImportFilesQuickValidator.validate(generateTestS3Key("test_invalid_1_jsonwkb.csv"), Format.CSV_JSONWKB, false);
        }catch (BaseHttpServerVerticle.ValidationException e){
            checkValidationException(e, "Bad JSON encoding! ");
        }
        try{
            ImportFilesQuickValidator.validate(generateTestS3Key("test_invalid_1_geojson.csv"), Format.CSV_GEOJSON, false);
        }catch (BaseHttpServerVerticle.ValidationException e){
            checkValidationException(e, "Bad JSON encoding! ");
        }
        try{
            ImportFilesQuickValidator.validate(generateTestS3Key("test_invalid_1_geojson.txt"), Format.GEOJSON, false);
        }catch (BaseHttpServerVerticle.ValidationException e){
            checkValidationException(e, "Bad JSON encoding! ");
        }
    }

    @Test
    public void testInvalidWKB() {
        /** Invalid JSON */
        S3Client.getInstance().putObject(generateTestS3Key("test_invalid_1_jsonwkb.csv"),
                "text/csv",
                "\"{'\"properties'\": {invalid}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A40000000000000".getBytes());
        try{
            ImportFilesQuickValidator.validate(generateTestS3Key("test_invalid_1_jsonwkb.csv"), Format.CSV_JSONWKB, false);
        }catch (BaseHttpServerVerticle.ValidationException e){
            checkValidationException(e, "Bad WKB encoding! ");
        }
    }

    private static void checkValidationException(BaseHttpServerVerticle.ValidationException e, String message){
        assertEquals(BaseHttpServerVerticle.ValidationException.class, e.getClass());
        assertEquals(message , e.getMessage());
    }
}
