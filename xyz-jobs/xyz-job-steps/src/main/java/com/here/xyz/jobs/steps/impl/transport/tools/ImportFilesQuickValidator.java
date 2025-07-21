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

import com.amazonaws.util.CountingInputStream;
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
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class ImportFilesQuickValidator {
  private static final Logger logger = LogManager.getLogger();
  private static final int VALIDATE_LINE_MAX_LINE_SIZE_BYTES = 4 * 1024 * 1024;
  private static final String RE_UPLOAD_HINT = "\nPlease re-upload the input files in the correct format using the already provided upload-urls!";

  public static void validate(S3DataFile s3File, Format format, EntityPerLine entityPerLine) throws ValidationException {
    try {
      validateFirstCSVLine(s3File, format, entityPerLine);
    }
    catch (IOException e) {
      throw new ValidationException("Input could not be read.", e);
    }
  }

  private static void validateFirstCSVLine(S3DataFile s3File, Format format, EntityPerLine entityPerLine)
      throws IOException, ValidationException {
    logger.info("Validating first line of file {} in format {}", s3File.getS3Key(), format);
    S3Client client = S3Client.getInstance(s3File.getS3Bucket());

    InputStream rawInput = client.streamObjectContent(s3File.getS3Key(), 0, VALIDATE_LINE_MAX_LINE_SIZE_BYTES);
    if (s3File.isCompressed())
      rawInput = new GZIPInputStream(rawInput);

    CountingInputStream countingStream = new CountingInputStream(rawInput);
    BufferedReader reader = new BufferedReader(new InputStreamReader(countingStream, StandardCharsets.UTF_8));

    StringBuilder line2 = new StringBuilder();
    int ch;

    while ((ch = reader.read()) != -1) {
      line2.append((char) ch);

      if (ch == '\n' || ch == '\r') {
        ImportFilesQuickValidator.validateCSVLine(line2.toString(), format, entityPerLine);
        logger.info("Validation finished {} in format {}", s3File.getS3Key(), format);
        return;
      }

      if (countingStream.getByteCount() >= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
        throw new IllegalStateException("No newline found within 4MB decompressed limit.");
      }
    }

    // Still validate if file ends without a newline
    if (!line2.isEmpty()) {
      //Not able to find a newline - could be a one-liner
      ImportFilesQuickValidator.validateCSVLine(line2.toString(), format, entityPerLine);
      logger.info("Validation finished {} ", s3File.getS3Key());
      return;
    }

    throw new IllegalStateException("No data found in file.");
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
