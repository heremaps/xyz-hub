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
package com.here.xyz.httpconnector.config;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.ExportObject;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.validate.Validator;
import com.here.xyz.jobs.util.S3ClientHelper;
import com.here.xyz.util.service.aws.S3ObjectSummary;
import io.vertx.core.Future;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

public class JobS3Client extends AwsS3Client {
    public static final String EXPORT_DOWNLOAD_FOLDER = "exports";
    public static final String EXPORT_PERSIST_FOLDER = "persistent";
    protected static final String IMPORT_MANUAL_UPLOAD_FOLDER = "manual";
    protected static final String IMPORT_UPLOAD_FOLDER = "imports";
    private static final Logger logger = LogManager.getLogger();
    private static final int VALIDATE_LINE_KB_STEPS = 512 * 1024;
    private static final int VALIDATE_LINE_MAX_LINE_SIZE_BYTES = 4 * 1024 * 1024;
    private static Cache<String, Map<String, ExportObject>> s3ScanningCache = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    public static String getImportPath(String jobId, String part) {
        return IMPORT_UPLOAD_FOLDER + "/" + jobId + "/" + part;
    }

    public ImportObject generateUploadURL(Import job) throws IOException {
        return generateUploadURL(CService.configuration.JOBS_S3_BUCKET, job);
    }

    public ImportObject generateUploadURL(String bucketName, Import job) throws IOException {
        String extension = "csv";

        int currentPart = job.getImportObjects().size();

        String key = "${uploadFolder}/${jobId}/part_${currentPart}.${extension}"
                .replace("${uploadFolder}", IMPORT_UPLOAD_FOLDER)
                .replace("${jobId}", job.getId())
                .replace("${currentPart}", Integer.toString(currentPart))
                .replace("${extension}", extension);

        URL url = generateUploadURL(bucketName, key);
        return new ImportObject(key, url);
    }

    public Map<String, ImportObject> scanImportPath(Import job, Job.CSVFormat csvFormat) {
        /** if we cant find a upload url read from IMPORT_MANUAL_UPLOAD_FOLDER */
        String firstKey = (String) job.getImportObjects().keySet().toArray()[0];
        String path = getS3Path(job);

        /** manual uploaded files are not allowed to be named as part_*.csv */
        if (!firstKey.matches("part_\\d*.csv"))
            path = IMPORT_MANUAL_UPLOAD_FOLDER + "/" + path;

        return scanImportPath(path, CService.configuration.JOBS_S3_BUCKET, csvFormat);
    }

    public Map<String, ImportObject> scanImportPath(String prefix, String bucketName, Job.CSVFormat csvFormat) {
        Map<String, ImportObject> importObjectList = new HashMap<>();

        for (S3ObjectSummary s3ObjectSummary : scanFolder(bucketName, prefix)) {

            HeadObjectResponse metadata = S3ClientHelper.loadMetadata(client, bucketName, s3ObjectSummary.key());
            ImportObject importObject = checkFile(s3ObjectSummary, metadata, csvFormat);
            importObjectList.put(importObject.getFilename(), importObject);
        }

        return importObjectList;
    }

    private ImportObject checkFile(S3ObjectSummary s3ObjectSummary, HeadObjectResponse objectMetadata, Job.CSVFormat csvFormat) {
        //skip validation till refactoring is done.
        ImportObject io = new ImportObject(s3ObjectSummary, objectMetadata);
        io.setStatus(ImportObject.Status.waiting);
        io.setValid(true);
        return io;
    }

    private ImportObject checkFileBak(S3ObjectSummary s3ObjectSummary, HeadObjectResponse objectMetadata, Job.CSVFormat csvFormat) {
        ImportObject io = new ImportObject(s3ObjectSummary, objectMetadata);

        try {
            if (objectMetadata.contentEncoding() != null &&
                    objectMetadata.contentEncoding().equalsIgnoreCase("gzip")) {
                validateFirstZippedCSVLine(io.getS3Key(), s3ObjectSummary.bucket(), csvFormat, "", 0);
            } else {
                validateFirstCSVLine(io.getS3Key(), s3ObjectSummary.bucket(), csvFormat, "", 0);
            }
            io.setStatus(ImportObject.Status.waiting);
            io.setValid(true);
        } catch (Exception e) {
            if (e instanceof UnsupportedEncodingException) {
                logger.info("CSV Format is not valid: {}", io.getS3Key());
            } else if (e instanceof ZipException) {
                logger.info("Wrong content-encoding: [}", io.getS3Key());
            } else
                logger.warn("checkFile error {} {}", io.getS3Key(), e);
            io.setValid(false);
        }

        return io;
    }

    private void validateFirstCSVLine(String keyName, String bucketName, Job.CSVFormat csvFormat, String line, int fromKB) throws IOException {
        int toKB = fromKB + VALIDATE_LINE_KB_STEPS;

        GetObjectRequest gor;
        InputStream is = null;
        BufferedReader reader = null;

        try {
            gor = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .range("bytes=" + fromKB + "-" + toKB)
                    .build();

            ResponseInputStream<GetObjectResponse> objectContent = client.getObject(gor);
            is = objectContent;
            reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

            int val;
            while ((val = reader.read()) != -1) {
                char c = (char) val;
                line += c;

                if (c == '\n' || c == '\r') {
                    Validator.validateCSVLine(line, csvFormat);
                    return;
                }
            }

        } catch (SdkException e) {
            /** Did not find a lineBreak - maybe CSV with 1LOC */
            if (e.getMessage().contains("InvalidRange")) {
                logger.info("Invalid Range found for s3Key {}", keyName);
                Validator.validateCSVLine(line, csvFormat);
                return;
            }
            throw e;
        } finally {
            if (is != null) {
                is.close();
            }
            if (reader != null)
                reader.close();
        }

        /** not found a line break */
        if (toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
            /** not found a line break till now - search further */
            fromKB = toKB + 1;
            validateFirstCSVLine(keyName, bucketName, csvFormat, line, fromKB);
        } else
            throw new UnsupportedEncodingException("Not able to find EOL!");
    }

    private void validateFirstZippedCSVLine(String keyName, String bucketName, Job.CSVFormat csvFormat, String line, int toKB) throws IOException {
        if (toKB == 0)
            toKB = VALIDATE_LINE_KB_STEPS;

        GetObjectRequest gor;
        InputStream is = null;
        BufferedReader reader = null;

        int val;

        try {
            gor = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .range("bytes=0-" + toKB)
                    .build();

            ResponseInputStream<GetObjectResponse> objectContent = client.getObject(gor);
            is = objectContent;

            reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(is)));

            while ((val = reader.read()) != -1) {
                char c = (char) val;
                line += c;
                if (c == '\n' || c == '\r') {
                    /** Found complete line */
                    Validator.validateCSVLine(line, csvFormat);
                    return;
                }
            }
        } catch (EOFException e) {
            /** Ignore incomplete stream */
        } finally {
            if (is != null) {
                is.close();
            }
            if (reader != null)
                reader.close();
        }

        if (toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
            /** not found a line break till now - search further */
            toKB = toKB + VALIDATE_LINE_KB_STEPS;
            validateFirstZippedCSVLine(keyName, bucketName, csvFormat, line, toKB);
        } else
            throw new UnsupportedEncodingException("Not able to find EOL!");
    }

    public Map<String, ExportObject> scanExportPathCached(String prefix) {
        try {
            return s3ScanningCache.get(prefix, () -> scanExportPath(prefix));
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public Map<String, ExportObject> scanExportPath(String prefix) {
        String bucketName = CService.configuration.JOBS_S3_BUCKET;
        Map<String, ExportObject> exportObjects = new HashMap<>();

        for (S3ObjectSummary objectSummary : scanFolder(bucketName, prefix)) {
            //Skip empty files
            if (objectSummary.isEmpty())
                continue;

            ExportObject eo = new ExportObject(objectSummary.key(), objectSummary.size());
            if (eo.getFilename().equalsIgnoreCase("manifest.json"))
                continue;
            ;

            exportObjects.put(eo.getFilename(prefix), eo);
        }

        return exportObjects;
    }

    public String getS3Path(Job job) {
        if (job instanceof Import)
            return IMPORT_UPLOAD_FOLDER + "/" + job.getId();

        //Decide if persistent or not.
        String subFolder = job instanceof Export export && export.readPersistExport()
                ? CService.jobS3Client.EXPORT_PERSIST_FOLDER
                : CService.jobS3Client.EXPORT_DOWNLOAD_FOLDER;

        String jobId = job.getId();

        return String.join("/", new String[]{
                subFolder,
                jobId
        });
    }

    public Future<Job> cleanJobData(Job job) {
        String path = getS3Path(job);

        if (job instanceof Export export && export.getSuperId() != null)
            logger.info("job[{}] data are got produced from {}! Data still present! ", job.getId(), export.getSuperId());

        this.deleteS3Folder(CService.configuration.JOBS_S3_BUCKET, path + "/");
        return Future.succeededFuture(job);
    }
}
