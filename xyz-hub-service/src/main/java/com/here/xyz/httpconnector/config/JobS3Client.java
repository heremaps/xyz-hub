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
package com.here.xyz.httpconnector.config;

import com.amazonaws.AmazonServiceException;

import com.amazonaws.services.s3.model.*;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.ImportValidator;
import com.here.xyz.httpconnector.util.jobs.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

public class JobS3Client extends AwsS3Client{
    private static final Logger logger = LogManager.getLogger();

    private static final int VALIDATE_LINE_KB_STEPS = 100 * 1024;
    private static final int VALIDATE_LINE_MAX_LINE_SIZE_BYTES = 10 * 1024 * 1024;

    protected static final String IMPORT_MANUAL_UPLOAD_FOLDER = "manual";
    protected static final String IMPORT_UPLOAD_FOLDER = "imports";

    public URL generateUploadURL(String key) throws IOException {
        return generateUploadURL(CService.configuration.JOBS_S3_BUCKET, key);
    }

    public ImportObject generateUploadURL(Import job) throws IOException {
        return generateUploadURL(CService.configuration.JOBS_S3_BUCKET, job);
    }

    public ImportObject generateUploadURL(String bucketName, Import job) throws IOException {
        String extension = "csv";

        int currentPart = job.getImportObjects().size();

        String key = "${uploadFolder}/${jobId}/part_${currentPart}.${extension}"
                .replace("${uploadFolder}",IMPORT_UPLOAD_FOLDER)
                .replace("${jobId}",job.getId())
                .replace("${currentPart}",Integer.toString(currentPart))
                .replace("${extension}",extension);

        URL url = generateUploadURL(bucketName, key);
        return new ImportObject(key,url);
    }

    public Map<String, ImportObject> scanImportPath(Import job, Job.CSVFormat csvFormat){
        /** if we cant find a upload url read from IMPORT_MANUAL_UPLOAD_FOLDER */
        ImportObject io = (ImportObject) job.getImportObjects().values().toArray()[0];
        if(io.getUploadUrl() == null)
           return scanImportPath(IMPORT_MANUAL_UPLOAD_FOLDER +"/"+ IMPORT_UPLOAD_FOLDER +"/"+ job.getId(), CService.configuration.JOBS_S3_BUCKET, csvFormat);
        return scanImportPath(IMPORT_UPLOAD_FOLDER +"/"+ job.getId(), CService.configuration.JOBS_S3_BUCKET, csvFormat);
    }

    public Map<String,ImportObject> scanImportPath(String prefix, String bucketName, Job.CSVFormat csvFormat){
        Map<String, ImportObject> importObjectList = new HashMap<>();
        ListObjectsRequest listObjects = new ListObjectsRequest()
                .withPrefix(prefix)
                .withBucketName(bucketName);

        ObjectListing objectListing = client.listObjects(listObjects);
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            ObjectMetadata objectMetadata = client.getObjectMetadata(bucketName, objectSummary.getKey());
            ImportObject importObject = checkFile(objectSummary, objectMetadata, csvFormat);
            importObjectList.put(importObject.getFilename(), importObject );
        }
        return importObjectList;
    }

    private ImportObject checkFile(S3ObjectSummary s3ObjectSummary, ObjectMetadata objectMetadata, Job.CSVFormat csvFormat){
        ImportObject io = new ImportObject(s3ObjectSummary, objectMetadata);
        try {
            if(objectMetadata.getContentEncoding() != null &&
                    objectMetadata.getContentEncoding().equalsIgnoreCase("gzip")){
                validateFirstZippedCSVLine(io.getS3Key(), s3ObjectSummary.getBucketName(), csvFormat, "", 0);
            }else{
                validateFirstCSVLine(io.getS3Key(), s3ObjectSummary.getBucketName(),csvFormat, "", 0);
            }
            io.setStatus(ImportObject.Status.waiting);
            io.setValid(true);
        } catch (Exception e) {
            if(e instanceof UnsupportedEncodingException){
                logger.info("CSV Format is not valid: {}",io.getS3Key());
            }else if(e instanceof ZipException){
                logger.info("Wrong content-encoding: [}", io.getS3Key());
            }else
                logger.warn("checkFile error {} {}",io.getS3Key(), e);
            io.setValid(false);
        }
        return io;
    }

    private void validateFirstCSVLine(String key_name, String bucket_name, Job.CSVFormat csvFormat, String line, int fromKB) throws AmazonServiceException, IOException {
        int toKB =  fromKB + VALIDATE_LINE_KB_STEPS;

        GetObjectRequest gor = new GetObjectRequest(bucket_name,key_name).withRange(fromKB, toKB );
        S3Object o;

        try {
             o = client.getObject(gor);
        }catch (AmazonServiceException e){
            /** Did not find a lineBreak - maybe CSV with 1LOC */
            if(e.getErrorCode().equalsIgnoreCase("InvalidRange")){
                logger.info("Invalid Range found for s3Key {}",key_name);
                ImportValidator.validateCSVLine(line, csvFormat);
                return;
            }
            throw e;
        }

        S3ObjectInputStream s3is = o.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3is, StandardCharsets.UTF_8));

        int val;
        while((val = reader.read()) != -1) {
            char c = (char)val;
            line += c;

            if(c == '\n' || c == '\r'){
                ImportValidator.validateCSVLine(line, csvFormat);
                return;
            }
        }

        /** not found a line break */
        reader.close();
        s3is.abort();
        s3is.close();

        if(toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
            /** not found a line break till now - search further */
            fromKB = toKB + 1;
            validateFirstCSVLine(key_name, bucket_name, csvFormat, line, fromKB);
        }
        else
            throw new UnsupportedEncodingException("Not able to find EOL!");
    }

    private void validateFirstZippedCSVLine(String key_name, String bucket_name, Job.CSVFormat csvFormat, String line, int toKB) throws AmazonServiceException, IOException {
        if(toKB == 0)
            toKB = VALIDATE_LINE_KB_STEPS;

        GetObjectRequest gor = new GetObjectRequest(bucket_name,key_name).withRange(0, toKB );

        S3Object o = client.getObject(gor);
        S3ObjectInputStream s3is = o.getObjectContent();

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(s3is)));

        int val;

        try {
            while ((val = reader.read()) != -1) {
                char c = (char) val;
                line += c;
                if (c == '\n' || c == '\r') {
                    /** Found complete line */
                    ImportValidator.validateCSVLine(line, csvFormat);
                    return;
                }
            }
        }catch (EOFException e) {
            /** Ignore incomplete stream */
        }

        reader.close();
        s3is.abort();
        s3is.close();

        if(toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
            /** not found a line break till now - search further */
            toKB = toKB + VALIDATE_LINE_KB_STEPS;
            validateFirstZippedCSVLine(key_name, bucket_name, csvFormat, line, toKB);
        }
        else
            throw new UnsupportedEncodingException("Not able to find EOL!");
    }
}
