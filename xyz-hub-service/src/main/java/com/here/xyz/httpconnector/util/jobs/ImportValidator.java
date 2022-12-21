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
package com.here.xyz.httpconnector.util.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.hub.Core;
import com.here.xyz.models.geojson.implementation.Feature;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class ImportValidator {
    private static final Logger logger = LogManager.getLogger();

    public static void validateCSVLine(String csvLine, Import.CSVFormat csvFormat) throws UnsupportedEncodingException {

        if(csvLine != null && csvLine.endsWith("\r\n"))
            csvLine = csvLine.substring(0,csvLine.length()-4);
        else if(csvLine != null && (csvLine.endsWith("\n") || csvLine.endsWith("\r")))
            csvLine = csvLine.substring(0,csvLine.length()-2);

        switch (csvFormat){
            case GEOJSON:
                 validateGEOJSON(csvLine);
                break;
            case JSON_WKB:
                validateJSON_WKB(csvLine);
                break;
            case JSON_WKT:
                validateJSON_WKT(csvLine);
        }
    }

    private static void validateGEOJSON(String csvLine) throws UnsupportedEncodingException {
        try {
            /** Try to serialize JSON */
            String geoJson = csvLine.substring(1,csvLine.length()-2).replaceAll("'\"","\"");
            XyzSerializable.deserialize(geoJson, Feature.class);
        } catch (JsonProcessingException e) {
            logger.info("Bad Encoding: ",e);
            throw new UnsupportedEncodingException();
        } catch (Exception e) {
            logger.info("Bad Encoding: ",e);
            throw new UnsupportedEncodingException();
        }
    }

    private static void validateJSON_WKB(String csvLine) throws UnsupportedEncodingException {
        if(csvLine.lastIndexOf(",") != -1) {
            try {
                String json = csvLine.substring(1,csvLine.lastIndexOf(",")-1).replaceAll("'\"","\"");
                String wkb = csvLine.substring(csvLine.lastIndexOf(",")+1);

                byte[] aux = WKBReader.hexToBytes(wkb);
                /** Try to read WKB */
                new WKBReader().read(aux);
                /** Try to serialize JSON */
                new JSONObject(json);
            } catch (ParseException e) {
                logger.info("Bad WKB Encoding: ",e);
                throw new UnsupportedEncodingException();
            } catch (JSONException e) {
                logger.info("Bad JSON Encoding: ",e);
                throw new UnsupportedEncodingException();
            } catch (Exception e) {
                logger.info("Bad Encoding: ",e);
                throw new UnsupportedEncodingException();
            }
        }
    }

    private static void validateJSON_WKT(String csvLine) throws UnsupportedEncodingException {
        throw new NotImplementedException();
    }

    public static void validateImportJob(Import job){
        if(job.getImportObjects().size() == 0){
            job.setErrorDescription(Import.ERROR_DESCRIPTION_UPLOAD_MISSING);
            job.setErrorType(Import.ERROR_TYPE_VALIDATION_FAILED);
        }else {
            try {
                /** scan S3 Path for existing Uploads and validate first line of CSV */
                Map<String, ImportObject> scannedObjects = CService.jobS3Client.scanImportPath(job, job.getCsvFormat());
                if (scannedObjects.size() == 0) {
                    job.setErrorDescription(Import.ERROR_DESCRIPTION_UPLOAD_MISSING);
                    job.setErrorType(Import.ERROR_TYPE_VALIDATION_FAILED);
                } else {
                    boolean foundOneValid = false;
                    for (String key : job.getImportObjects().keySet()) {
                        if (scannedObjects.get(key) != null) {
                            /** S3 Object is found - update job with file metadata */
                            ImportObject scannedFile = scannedObjects.get(key);
                            job.addImportObject(scannedFile);

                            if (!scannedFile.isValid()) {
                                /** If file is invalid fail validation */
                                job.setErrorDescription(Import.ERROR_DESCRIPTION_INVALID_FILE);
                                job.setErrorType(Import.ERROR_TYPE_VALIDATION_FAILED);
                            } else
                                foundOneValid = true;
                        } else {
                            job.getImportObjects().get(key).setFilesize(-1);
                        }
                    }

                    if (!foundOneValid) {
                        /** No Uploads found */
                        job.setErrorDescription(Import.ERROR_DESCRIPTION_NO_VALID_FILES_FOUND);
                        job.setErrorType(Import.ERROR_TYPE_VALIDATION_FAILED);
                    }
                }
            }catch (Exception e){
                logger.warn("[{}] Validation has failed! {}",job.getId(),e);
                job.setStatus(Job.Status.failed);
                job.setErrorType(Import.ERROR_TYPE_VALIDATION_FAILED);
            }
        }

        //ToDo: Decide if we want to proceed partially
        if(job.getErrorType() != null)
            job.setStatus(Job.Status.failed);
        else
            job.setStatus(Job.Status.validated);
    }

    private static void setJobDefaults(Job job){
        job.setCreatedAt(Core.currentTimeMillis() / 1000L);
        job.setUpdatedAt(Core.currentTimeMillis() / 1000L);

        if(job.getErrorType() != null){
            job.setErrorType(null);
        }
        if(job.getErrorDescription() != null){
            job.setErrorDescription(null);
        }
    }

    /** Check mandatory fields */
    public static String validateImportCreation(Import job){
        setJobDefaults(job);
        if(job.getTargetSpaceId() == null){
            return "Please specify 'targetSpaceId'!";
        }
        if(job.getCsvFormat() == null){
            return "Please specify 'csvFormat'!";
        }
        return null;
    }
}
