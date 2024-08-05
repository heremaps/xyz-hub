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
import com.here.xyz.jobs.steps.TestSteps;
import com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace.Format;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
public class QuickValidatorTest extends TestSteps {
    private static String TEST_PREFIX = "validation-test/";

    @Before
    public void cleanUp() {
        cleanS3Files(TEST_PREFIX);
    }

    private String generateTestS3Key(String name){
        return TEST_PREFIX + name;
    }

    private void validate(String s3Key, Format format, boolean isCompressed) throws ValidationException {
        ImportFilesQuickValidator.validate(Config.instance.JOBS_S3_BUCKET, s3Key, format, isCompressed);
    }

    private void uploadAndValidateValidFiles(boolean gzip) throws IOException, ValidationException {
        /** With no new line at end */
        uploadFileToS3(generateTestS3Key("test_valid_1_jsonwkb.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000".getBytes(),
                gzip
        );

        uploadFileToS3(generateTestS3Key("test_valid_1_geojson.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\"".getBytes(),
                gzip
        );

        uploadFileToS3(generateTestS3Key("test_valid_1_geojson.txt"),
                S3ContentType.APPLICATION_JSON,
                "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}".getBytes(),
                gzip
        );

        /** With new line at end */
        uploadFileToS3(generateTestS3Key("test_valid_2_jsonwkb.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000\n".getBytes(),
                gzip
        );
        uploadFileToS3(generateTestS3Key("test_valid_2_geojson.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\"\n".getBytes(),
                gzip
        );
        uploadFileToS3(generateTestS3Key("test_valid_2_geojson.txt"),
                S3ContentType.APPLICATION_JSON,
                "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}\n".getBytes(),
                gzip
        );

        /** Should not fail - above are all valid */
        validate(generateTestS3Key("test_valid_1_jsonwkb.csv"), Format.CSV_JSONWKB, gzip);
        validate(generateTestS3Key("test_valid_1_geojson.csv"), Format.CSV_GEOJSON, gzip);
        validate(generateTestS3Key("test_valid_1_geojson.txt"), Format.GEOJSON, gzip);
        validate(generateTestS3Key("test_valid_2_jsonwkb.csv"), Format.CSV_JSONWKB, gzip);
        validate(generateTestS3Key("test_valid_2_geojson.csv"), Format.CSV_GEOJSON, gzip);
        validate(generateTestS3Key("test_valid_2_geojson.txt"), Format.GEOJSON, gzip);
    }

    private void uploadAndValidateFilesWithInvalidJson(boolean gzip) throws IOException {
        /** Invalid JSON */
        uploadFileToS3(generateTestS3Key("test_invalid_1_jsonwkb.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"properties'\": {invalid}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000".getBytes(),
                gzip
        );

        uploadFileToS3(generateTestS3Key("test_invalid_1_geojson.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"type'\":'\"Feature'\" invalid }}\"".getBytes(),
                gzip
        );

        uploadFileToS3(generateTestS3Key("test_invalid_1_geojson.txt"),
                S3ContentType.APPLICATION_JSON,
                "{\"type\":\"Featureinvaid\",\"geometry\":{\"type\":\"Pointinvalid\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}".getBytes(),
                gzip
        );

        try{
            validate(generateTestS3Key("test_invalid_1_jsonwkb.csv"), Format.CSV_JSONWKB, gzip);
        }catch (ValidationException e){
            checkValidationException(e, "Bad JSON encoding! ");
        }
        try{
            validate(generateTestS3Key("test_invalid_1_geojson.csv"), Format.CSV_GEOJSON, gzip);
        }catch (ValidationException e){
            checkValidationException(e, "Bad JSON encoding! ");
        }
        try{
            validate(generateTestS3Key("test_invalid_1_geojson.txt"), Format.GEOJSON, gzip);
        }catch (ValidationException e){
            checkValidationException(e, "Bad JSON encoding! ");
        }
    }
    private void uploadAndValidateFilesWithInvalidWKB(boolean gzip) throws IOException {
        /** Invalid WKB */
        uploadFileToS3(generateTestS3Key("test_invalid_1_jsonwkb.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A40000000000000000".getBytes(),
                gzip);

        uploadFileToS3(generateTestS3Key("test_invalid_2_jsonwkb.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"properties'\": {'\"test'\": 1}}\",invalid".getBytes(),
                gzip);
        try{
            validate(generateTestS3Key("test_invalid_1_jsonwkb.csv"), Format.CSV_JSONWKB, gzip);
        }catch (ValidationException e){
            checkValidationException(e, "Bad WKB encoding! ");
        }

        try{
            validate(generateTestS3Key("test_invalid_2_jsonwkb.csv"), Format.CSV_JSONWKB, gzip);
        }catch (ValidationException e){
            checkValidationException(e, "Bad WKB encoding! ");
        }
    }

    private void uploadAndValidateFilesWithEmptyColumn(boolean gzip) throws IOException {
        /** Invalid WKB */
        uploadFileToS3(generateTestS3Key("test_invalid_3_jsonwkb.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000,".getBytes(),
                gzip
        );

        uploadFileToS3(generateTestS3Key("test_invalid_3_geojson.csv"),
                S3ContentType.TEXT_CSV,
                "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\",".getBytes(),
                gzip
        );

        try{
            validate(generateTestS3Key("test_invalid_3_jsonwkb.csv"), Format.CSV_JSONWKB, gzip);
        }catch (ValidationException e){
            checkValidationException(e, "Empty Column detected!");
        }

        try{
            validate(generateTestS3Key("test_invalid_3_geojson.csv"), Format.CSV_GEOJSON, gzip);
        }catch (ValidationException e){
            checkValidationException(e, "Empty Column detected!");
        }
    }

    @Test
    public void testQuickValidation() throws ValidationException, IOException {
      uploadAndValidateValidFiles(false);
    }

    @Test
    public void testQuickValidationGZipped() throws ValidationException, IOException {
        uploadAndValidateValidFiles(true);
    }

    @Test
    public void testInvalidJson() throws IOException {
        uploadAndValidateFilesWithInvalidJson(false);
    }

    @Test
    public void testInvalidJsonGZipped() throws IOException {
        uploadAndValidateFilesWithInvalidJson(true);
    }

    @Test
    public void testInvalidWKB() throws IOException {
        uploadAndValidateFilesWithInvalidWKB(false);
    }
    @Test
    public void testInvalidWKBGzipped() throws IOException{
        uploadAndValidateFilesWithInvalidWKB(true);
    }

    @Test
    public void testValidateFilesWithEmptyColumn() throws IOException{
        uploadAndValidateFilesWithEmptyColumn(false);
    }
    @Test
    public void testValidateFilesWithEmptyColumnGzipped() throws IOException{
        uploadAndValidateFilesWithEmptyColumn(true);
    }

    private static void checkValidationException(ValidationException e, String message){
        assertEquals(ValidationException.class, e.getClass());
        assertEquals(message , e.getMessage());
    }
}
