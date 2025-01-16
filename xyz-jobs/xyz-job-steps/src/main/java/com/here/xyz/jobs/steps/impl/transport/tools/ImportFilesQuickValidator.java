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

package com.here.xyz.jobs.steps.impl.transport.tools;

import static com.here.xyz.XyzSerializable.Mappers.DEFAULT_MAPPER;

import com.amazonaws.AmazonServiceException;
import com.fasterxml.jackson.core.JacksonException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.S3DataFile;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.EntityPerLine;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class ImportFilesQuickValidator {
  private static final int VALIDATE_LINE_PAGE_SIZE_BYTES = 512 * 1024;
  private static final int VALIDATE_LINE_MAX_LINE_SIZE_BYTES = 4 * 1024 * 1024;
  private static final String RE_UPLOAD_HINT = "\nPlease re-upload the input files in the correct format using the already provided upload-urls!";

  public static void validate(S3DataFile s3File, Format format, EntityPerLine entityPerLine) throws ValidationException {
    try {
      validateFirstCSVLine(s3File, format, "", 0, entityPerLine);
    }
    catch (IOException e) {
      throw new ValidationException("Input could not be read.", e);
    }
  }

  private static void validateFirstCSVLine(S3DataFile s3File, Format format, String line, long fromKB, EntityPerLine entityPerLine)
      throws IOException, ValidationException {
    S3Client client = S3Client.getInstance(s3File.getS3Bucket());
    long toKB = fromKB + VALIDATE_LINE_PAGE_SIZE_BYTES;

    InputStream input = client.streamObjectContent(s3File.getS3Key(), 0, toKB);

    if (s3File.isCompressed())
      input = new GZIPInputStream(input);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
      int val;
      while ((val = reader.read()) != -1) {
        char c = (char) val;
        line += c;

        if (c == '\n' || c == '\r') {
          ImportFilesQuickValidator.validateCSVLine(line, format, entityPerLine);
          return;
        }
      }
    }
    catch (AmazonServiceException e) {
      if (e.getErrorCode().equalsIgnoreCase("InvalidRange")) {
        //Did not find a line break - maybe CSV with 1LOC - try to validate
        ImportFilesQuickValidator.validateCSVLine(line, format, entityPerLine);
        return;
      }
      throw e;
    }

    if (toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
      //Not found a line break till now - search further
      validateFirstCSVLine(s3File, format, line, toKB, entityPerLine);
    }
    else {
      //Not able to find a newline - could be a one-liner
      validateCSVLine(line, format, entityPerLine);
    }
  }

  private static void validateCSVLine(String csvLine, Format format, EntityPerLine entityPerLine) throws ValidationException {
    if (csvLine != null && csvLine.endsWith("\r\n"))
      csvLine = csvLine.substring(0, csvLine.length() - 3);
    else if (csvLine != null && (csvLine.endsWith("\n") || csvLine.endsWith("\r")))
      csvLine = csvLine.substring(0, csvLine.length() - 1);

    if (!format.equals(Format.GEOJSON) && csvLine.endsWith(","))
      throw new ValidationException("Empty Column detected!" + RE_UPLOAD_HINT);

    switch (format) {
      case CSV_GEOJSON -> validateCsvGeoJSON(csvLine, entityPerLine);
      case CSV_JSON_WKB -> validateCsvJSON_WKB(csvLine);
      case GEOJSON -> validateGeoJSON(csvLine, entityPerLine);
      default -> throw new ValidationException("Format is not supported! " + format + RE_UPLOAD_HINT);
    }
  }

  private static void validateGeoJSON(String geoJson, EntityPerLine entityPerLine) throws ValidationException {
    try {
      //Try to deserialize into the target model
      XyzSerializable.deserialize(geoJson, (Class<? extends Typed>) (entityPerLine == EntityPerLine.Feature ? Feature.class : FeatureCollection.class));
    }
    catch (Exception e) {
      transformException(e);
    }
  }

  private static void validateCsvGeoJSON(String csvLine, EntityPerLine entityPerLine) throws ValidationException {
    //Try to deserialize JSON
    csvLine = csvLine.substring(1, csvLine.length()).replaceAll("'\"", "\"");
    validateGeoJSON(csvLine, entityPerLine);
  }

  private static void validateCsvJSON_WKB(String csvLine) throws ValidationException {
    try {
      String json = csvLine.substring(1, csvLine.lastIndexOf(",") - 1).replaceAll("'\"", "\"");
      String wkb = csvLine.substring(csvLine.lastIndexOf(",") + 1);

      byte[] aux = WKBReader.hexToBytes(wkb);
      //Try to read WKB
      new WKBReader().read(aux);
      //Try to serialize JSON
      DEFAULT_MAPPER.get().readTree(json);
    }
    catch (Exception e) {
      transformException(e);
    }
  }

  private static void transformException(Exception e) throws ValidationException {
    Throwable cause = e.getCause();
    if (e instanceof ParseException || e instanceof IllegalArgumentException)
      throw new ValidationException("Bad WKB encoding! " + (cause != null ? cause.toString() : "") + RE_UPLOAD_HINT);
    else if (e instanceof JacksonException)
      throw new ValidationException("Bad JSON encoding! " + (cause != null ? cause.toString() : "") + RE_UPLOAD_HINT);
    else
      throw new ValidationException("Not able to validate! " + (cause != null ? cause.toString() : "") + RE_UPLOAD_HINT);
  }
}
