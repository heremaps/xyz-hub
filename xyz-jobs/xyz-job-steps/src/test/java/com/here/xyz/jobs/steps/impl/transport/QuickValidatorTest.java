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

package com.here.xyz.jobs.steps.impl.transport;

import static com.here.xyz.jobs.util.test.StepTestBase.S3ContentType.APPLICATION_JSON;
import static com.here.xyz.jobs.util.test.StepTestBase.S3ContentType.TEXT_CSV;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine.Feature;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine.FeatureCollection;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.CSV_GEOJSON;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.CSV_JSON_WKB;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.GEOJSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.impl.transport.tools.ImportFilesQuickValidator;
import com.here.xyz.jobs.util.test.StepTestBase;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class QuickValidatorTest extends StepTestBase {

  private static String TEST_PREFIX = "validation-test/";

  @BeforeEach
  public void cleanUp() {
    cleanS3Files(TEST_PREFIX);
  }

  @Test
  public void testQuickValidation() throws ValidationException, IOException {
    uploadAndValidateValidFiles(false);
  }

  private void uploadAndValidateValidFiles(boolean gzip) throws IOException, ValidationException {
    /** With no new line at end */
    uploadFileToS3(generateTestS3Key("test_valid_1_jsonwkb.csv"),
        TEXT_CSV,
        "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000".getBytes(),
        gzip
    );
    uploadFileToS3(generateTestS3Key("test_valid_1_geojson.csv"),
        TEXT_CSV,
        "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\"".getBytes(),
        gzip
    );
    uploadFileToS3(generateTestS3Key("test_valid_1_geojson.geojson"),
        APPLICATION_JSON,
        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}".getBytes(),
        gzip
    );
    uploadFileToS3(generateTestS3Key("test_valid_1_geojsonfc.geojson"),
        APPLICATION_JSON,
        ("{\"type\":\"FeatureCollection\", \"features\" :[" +
            "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}," +
            "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[9,51]},\"properties\":{\"test\":2}}" +
            "]}").getBytes(),
        gzip
    );

    /** With new line at end */
    uploadFileToS3(generateTestS3Key("test_valid_2_jsonwkb.csv"),
        TEXT_CSV,
        "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000\n".getBytes(),
        gzip
    );
    uploadFileToS3(generateTestS3Key("test_valid_2_geojson.csv"),
        TEXT_CSV,
        "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\"\n".getBytes(),
        gzip
    );
    uploadFileToS3(generateTestS3Key("test_valid_2_geojson.geojson"),
        APPLICATION_JSON,
        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}\n".getBytes(),
        gzip
    );

    uploadFileToS3(generateTestS3Key("test_valid_2_geojsonfc.geojson"),
        APPLICATION_JSON,
        ("{\"type\":\"FeatureCollection\", \"features\" :[" +
            "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}," +
            "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[9,51]},\"properties\":{\"test\":2}}" +
            "]}\n").getBytes(),
        gzip
    );

    /** Should not fail - above are all valid */
    validate(generateTestS3Key("test_valid_1_jsonwkb.csv"), CSV_JSON_WKB, gzip, Feature);
    validate(generateTestS3Key("test_valid_1_geojson.csv"), CSV_GEOJSON, gzip, Feature);
    validate(generateTestS3Key("test_valid_1_geojson.geojson"), GEOJSON, gzip, Feature);
    validate(generateTestS3Key("test_valid_1_geojsonfc.geojson"), GEOJSON, gzip, EntityPerLine.FeatureCollection);
    validate(generateTestS3Key("test_valid_2_jsonwkb.csv"), CSV_JSON_WKB, gzip, Feature);
    validate(generateTestS3Key("test_valid_2_geojson.csv"), CSV_GEOJSON, gzip, Feature);
    validate(generateTestS3Key("test_valid_2_geojson.geojson"), GEOJSON, gzip, Feature);
    validate(generateTestS3Key("test_valid_2_geojsonfc.geojson"), GEOJSON, gzip, EntityPerLine.FeatureCollection);
  }

  private String generateTestS3Key(String name) {
    return TEST_PREFIX + name;
  }

  private void validate(String s3Key, Format format, boolean isCompressed, EntityPerLine entityPerLine) throws ValidationException {
    ImportFilesQuickValidator.validate(new UploadUrl()
        .withS3Bucket(Config.instance.JOBS_S3_BUCKET)
        .withS3Key(s3Key)
        .withCompressed(isCompressed), format, entityPerLine);
  }

  @Test
  public void testQuickValidationGZipped() throws ValidationException, IOException {
    uploadAndValidateValidFiles(true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInvalidJsonWkb(boolean gzip) throws IOException {
    testInvalidJson(
        "\"{'\"properties'\": {invalid}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000",
        TEXT_CSV,
        CSV_JSON_WKB,
        Feature,
        gzip
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInvalidJsonCsv(boolean gzip) throws IOException {
    testInvalidJson(
        "\"{'\"type'\":'\"Feature'\" invalid }}\"",
        TEXT_CSV,
        CSV_GEOJSON,
        Feature,
        gzip
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInvalidGeoJsonFeature(boolean gzip) throws IOException {
    testInvalidJson(
        "{\"type\":\"Featureinvaid\",\"geometry\":{\"type\":\"Pointinvalid\",\"coordinates\":[8,50]},\"properties\":{\"test\":1}}",
        APPLICATION_JSON,
        GEOJSON,
        Feature,
        gzip
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInvalidGeoJsonFeatureCollection(boolean gzip) throws IOException {
    testInvalidJson(
        "{\"type\":\"FeatureCollection\", \"features\":[invalid]}",
        APPLICATION_JSON,
        GEOJSON,
        FeatureCollection,
        gzip
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testValidGeoJsonFeatureCollectionWithoutFeatures(boolean gzip) throws IOException, ValidationException {
    testValidJson(
        "{\"type\":\"FeatureCollection\"}",
        APPLICATION_JSON,
        GEOJSON,
        FeatureCollection,
        gzip
    );
  }

  @Test
  public void testInvalidWKB() throws IOException {
    uploadAndValidateFilesWithInvalidWKB(false);
  }

  private void uploadAndValidateFilesWithInvalidWKB(boolean gzip) throws IOException {
    /** Invalid WKB */
    uploadFileToS3(generateTestS3Key("test_invalid_1_jsonwkb.csv"),
        TEXT_CSV,
        "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A40000000000000000".getBytes(),
        gzip);

    uploadFileToS3(generateTestS3Key("test_invalid_2_jsonwkb.csv"),
        TEXT_CSV,
        "\"{'\"properties'\": {'\"test'\": 1}}\",invalid".getBytes(),
        gzip);
    try {
      validate(generateTestS3Key("test_invalid_1_jsonwkb.csv"), CSV_JSON_WKB, gzip, Feature);
      fail("Exception expected");
    }
    catch (ValidationException e) {
      checkValidationException(e, "Bad WKB encoding! ");
    }

    try {
      validate(generateTestS3Key("test_invalid_2_jsonwkb.csv"), CSV_JSON_WKB, gzip, Feature);
      fail("Exception expected");
    }
    catch (ValidationException e) {
      checkValidationException(e, "Bad WKB encoding! ");
    }
  }

  @Test
  public void testInvalidWKBGzipped() throws IOException {
    uploadAndValidateFilesWithInvalidWKB(true);
  }

  @Test
  public void testValidateFilesWithEmptyColumn() throws IOException {
    uploadAndValidateFilesWithEmptyColumn(false);
  }

  private void uploadAndValidateFilesWithEmptyColumn(boolean gzip) throws IOException {
    /** Invalid WKB */
    uploadFileToS3(generateTestS3Key("test_invalid_3_jsonwkb.csv"),
        TEXT_CSV,
        "\"{'\"properties'\": {'\"test'\": 1}}\",01010000A0E61000007DAD4B8DD0AF07C0BD19355F25B74A400000000000000000,".getBytes(),
        gzip
    );

    uploadFileToS3(generateTestS3Key("test_invalid_3_geojson.csv"),
        TEXT_CSV,
        "\"{'\"type'\":'\"Feature'\",'\"geometry'\":{'\"type'\":'\"Point'\",'\"coordinates'\":[8,50]},'\"properties'\":{'\"test'\":1}}\",".getBytes(),
        gzip
    );

    try {
      validate(generateTestS3Key("test_invalid_3_jsonwkb.csv"), CSV_JSON_WKB, gzip, Feature);
      fail("Exception expected");
    }
    catch (ValidationException e) {
      checkValidationException(e, "Empty Column detected!");
    }

    try {
      validate(generateTestS3Key("test_invalid_3_geojson.csv"), CSV_GEOJSON, gzip, Feature);
      fail("Exception expected");
    }
    catch (ValidationException e) {
      checkValidationException(e, "Empty Column detected!");
    }
  }

  @Test
  public void testValidateFilesWithEmptyColumnGzipped() throws IOException {
    uploadAndValidateFilesWithEmptyColumn(true);
  }

  private void testInvalidJson(String fileContent, S3ContentType contentType, Format format, EntityPerLine entityPerLine, boolean gzip)
      throws IOException {
    try {
      testValidJson(fileContent, contentType, format, entityPerLine, gzip);
      fail("Exception expected");
    }
    catch (ValidationException e) {
      checkValidationException(e, "Bad JSON encoding! ");
    }
  }

  private void testValidJson(String fileContent, S3ContentType contentType, Format format, EntityPerLine entityPerLine, boolean gzip)
      throws IOException, ValidationException {
    uploadFileToS3(generateTestS3Key("someFile"),
        contentType,
        fileContent.getBytes(),
        gzip
    );
    validate(generateTestS3Key("someFile"), format, gzip, entityPerLine);
  }

  private static void checkValidationException(ValidationException e, String message) {
    assertEquals(ValidationException.class, e.getClass());
    assertTrue(e.getMessage().startsWith(message));
  }
}
