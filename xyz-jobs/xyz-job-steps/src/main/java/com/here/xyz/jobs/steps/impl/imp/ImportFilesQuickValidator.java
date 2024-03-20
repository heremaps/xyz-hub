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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.impl.imp.ImportFilesToSpace.Format;
import com.here.xyz.jobs.steps.inputs.UploadUrl;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class ImportFilesQuickValidator {
  private static final int VALIDATE_LINE_KB_STEPS = 512 * 1024;
  private static final int VALIDATE_LINE_MAX_LINE_SIZE_BYTES = 4 * 1024 * 1024;

  static void validate(UploadUrl uploadUrl, Format format) throws ValidationException {
    validate(uploadUrl.getS3Key(), format, uploadUrl.isCompressed());
  }

  static void validate(String s3Key, Format format, boolean isCompressed) throws ValidationException {
    S3Client client = S3Client.getInstance();
    try {
      if (isCompressed)
        validateFirstZippedCSVLine(client, s3Key, format, "", 0);
      else
        validateFirstCSVLine(client, s3Key, format, "", 0);
    }
    catch (IOException e) {
      //@TODO: Check how we want to handle those errors (not related to the content)
      /** Unexpected validation error! */
      throw new RuntimeException("Cant validate");
    }
  }

  //TODO: Simplify this method
  private static void validateFirstCSVLine(S3Client client, String s3Key, Format format, String line, int fromKB)
      throws IOException, ValidationException {
    int toKB = fromKB + VALIDATE_LINE_KB_STEPS;

    InputStream s3is = null;
    BufferedReader reader = null;

    try {

      s3is = client.streamObjectContent(s3Key);
      reader = new BufferedReader(new InputStreamReader(s3is, StandardCharsets.UTF_8));

      int val;
      while ((val = reader.read()) != -1) {
        char c = (char) val;
        line += c;

        if (c == '\n' || c == '\r') {
          ImportFilesQuickValidator.validateCSVLine(line, format);
          return;
        }
      }

    }
    catch (AmazonServiceException e) {
      if (e.getErrorCode().equalsIgnoreCase("InvalidRange")) {
        /** Did not find a lineBreak - maybe CSV with 1LOC - try to validate */
        ImportFilesQuickValidator.validateCSVLine(line, format);
        return;
      }
      throw e;
    }
    finally {
      if (s3is != null) {
        s3is.close();
      }
      if (reader != null)
        reader.close();
    }

    /** not found a line break */

    if (toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
      /** not found a line break till now - search further */
      fromKB = toKB + 1;
      validateFirstCSVLine(client, s3Key, format, line, fromKB);
    }
    else {
      /** Not able to find a newline - could be a oneLiner*/
      ImportFilesQuickValidator.validateCSVLine(line, format);
    }
  }

  private static void validateFirstZippedCSVLine(S3Client client, String s3Key, Format format, String line, long toKB)
      throws IOException, ValidationException {
    if (toKB == 0)
      toKB = VALIDATE_LINE_KB_STEPS;

    S3Object o = null;
    InputStream s3is = null;
    BufferedReader reader = null;

    int val;
    //TODO: Deduplicate with method validateFirstCSVLine()
    try {
      s3is = client.streamObjectContent(s3Key, 0, toKB);

      reader = new BufferedReader(new InputStreamReader(
          new GZIPInputStream(s3is)));

      while ((val = reader.read()) != -1) {
        char c = (char) val;
        line += c;
        if (c == '\n' || c == '\r') {
          /** Found complete line */
          ImportFilesQuickValidator.validateCSVLine(line, format);
          return;
        }
      }
    }
    catch (EOFException e) {
      /** Ignore incomplete stream */
    }
    finally {
      if (s3is != null) {
        s3is.close();
      }
      if (o != null)
        o.close();
      if (reader != null)
        reader.close();
    }

    if (toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
      /** not found a line break till now - search further */
      toKB = toKB + VALIDATE_LINE_KB_STEPS;
      validateFirstZippedCSVLine(client, s3Key, format, line, toKB);
    }
    else
      throw new UnsupportedEncodingException("Not able to find EOL!");
  }

  private static void validateCSVLine(String csvLine, Format format) throws ValidationException {

    if (csvLine != null && csvLine.endsWith("\r\n"))
      csvLine = csvLine.substring(0, csvLine.length() - 3);
    else if (csvLine != null && (csvLine.endsWith("\n") || csvLine.endsWith("\r")))
      csvLine = csvLine.substring(0, csvLine.length() - 1);

    switch (format) {
      case CSV_GEOJSON -> validateCsvGeoJSON(csvLine);
      case CSV_JSONWKB -> validateCsvJSON_WKB(csvLine);
      case GEOJSON -> validateGeoJSON(csvLine);
      default -> throw new ValidationException("Format is not supported! " + format);
    }
  }

  private static void validateGeoJSON(String csvLine) throws ValidationException {
    try {
      /** Try to serialize JSON */
      XyzSerializable.deserialize(csvLine, Feature.class);
    }
    catch (Exception e) {
      transformException(e);
    }
  }

  private static void validateCsvGeoJSON(String csvLine) throws ValidationException {
    try {
      /** Try to serialize JSON */
      String geoJson = csvLine.substring(1, csvLine.length()).replaceAll("'\"", "\"");
      XyzSerializable.deserialize(geoJson, Feature.class);
    }
    catch (Exception e) {
      transformException(e);
    }
  }

  private static void validateCsvJSON_WKB(String csvLine) throws ValidationException {
    if (csvLine.lastIndexOf(",") != -1) {
      try {
        String json = csvLine.substring(1, csvLine.lastIndexOf(",") - 1).replaceAll("'\"", "\"");
        String wkb = csvLine.substring(csvLine.lastIndexOf(",") + 1);

        byte[] aux = WKBReader.hexToBytes(wkb);
        /** Try to read WKB */
        new WKBReader().read(aux);
        /** Try to serialize JSON */
        new JSONObject(json);
      }
      catch (Exception e) {
        transformException(e);
      }
    }
  }

  private static void transformException(Exception e) throws ValidationException {
    Throwable cause = e.getCause();
    if (e instanceof ParseException)
      throw new ValidationException("Bad WKB encoding! " + (cause != null ? cause.toString() : ""));
    else if (e instanceof JSONException || e instanceof JsonParseException || e instanceof InvalidTypeIdException)
      throw new ValidationException("Bad JSON encoding! " + (cause != null ? cause.toString() : ""));
    else
      throw new ValidationException("Not able to validate! " + (cause != null ? cause.toString() : ""));
  }
}
