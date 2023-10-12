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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.ExportObject;
import com.here.xyz.httpconnector.util.jobs.Import;
import com.here.xyz.httpconnector.util.jobs.ImportObject;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;
import com.here.xyz.httpconnector.util.jobs.validate.Validator;
import com.here.xyz.util.Hasher;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobS3Client extends AwsS3Client{
    private static final Logger logger = LogManager.getLogger();

    private static final int VALIDATE_LINE_KB_STEPS = 100 * 1024;
    private static final int VALIDATE_LINE_MAX_LINE_SIZE_BYTES = 10 * 1024 * 1024;

    protected static final String IMPORT_MANUAL_UPLOAD_FOLDER = "manual";
    protected static final String IMPORT_UPLOAD_FOLDER = "imports";
    public static final String EXPORT_DOWNLOAD_FOLDER = "exports";
    public static final String EXPORT_PERSIST_FOLDER = "persistent";

    public static String getImportPath(String jobId, String part){
        return IMPORT_UPLOAD_FOLDER +"/"+ jobId+"/"+part;
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
        String firstKey = (String) job.getImportObjects().keySet().toArray()[0];
        String path = getS3Path(job);

        /** manual uploaded files are not allowed to be named as part_*.csv */
        if(!firstKey.matches("part_\\d*.csv"))
            path = IMPORT_MANUAL_UPLOAD_FOLDER +"/"+ path;

        return scanImportPath(path, CService.configuration.JOBS_S3_BUCKET, csvFormat);
    }

    public Map<String,ImportObject> scanImportPath(String prefix, String bucketName, Job.CSVFormat csvFormat){
        Map<String, ImportObject> importObjectList = new HashMap<>();
        ListObjectsRequest listObjects = new ListObjectsRequest()
                .withPrefix(prefix)
                .withBucketName(bucketName);

        ObjectListing objectListing = client.listObjects(listObjects);
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            /** localstack does not set the bucket name */
            if(objectSummary.getBucketName() == null)
                objectSummary.setBucketName(bucketName);
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
                logger.info("CSV Format is not valid: {}", io.getS3Key());
            }else if(e instanceof ZipException){
                logger.info("Wrong content-encoding: [}", io.getS3Key());
            }else
                logger.warn("checkFile error {} {}", io.getS3Key(), e);
            io.setValid(false);
        }

        return io;
    }

    private void validateFirstCSVLine(String key_name, String bucket_name, Job.CSVFormat csvFormat, String line, int fromKB) throws AmazonServiceException, IOException {
        int toKB =  fromKB + VALIDATE_LINE_KB_STEPS;

        GetObjectRequest gor;
        S3Object o = null;
        S3ObjectInputStream s3is = null;
        BufferedReader reader = null;

        try {
            gor = new GetObjectRequest(bucket_name,key_name).withRange(fromKB, toKB );
            o = client.getObject(gor);

            s3is = o.getObjectContent();
            reader = new BufferedReader(new InputStreamReader(s3is, StandardCharsets.UTF_8));

            int val;
            while((val = reader.read()) != -1) {
                char c = (char)val;
                line += c;

                if(c == '\n' || c == '\r'){
                    Validator.validateCSVLine(line, csvFormat);
                    return;
                }
            }

        }catch (AmazonServiceException e){
            /** Did not find a lineBreak - maybe CSV with 1LOC */
            if(e.getErrorCode().equalsIgnoreCase("InvalidRange")){
                logger.info("Invalid Range found for s3Key {}", key_name);
                Validator.validateCSVLine(line, csvFormat);
                return;
            }
            throw e;
        }finally {
            if(s3is !=null) {
                s3is.abort();
                s3is.close();
            }
            if(o != null)
                o.close();
            if(reader != null)
                reader.close();
        }

        /** not found a line break */

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

        GetObjectRequest gor;
        S3Object o = null;
        S3ObjectInputStream s3is = null;
        BufferedReader reader = null;

        int val;

        try {
            gor = new GetObjectRequest(bucket_name,key_name).withRange(0, toKB );

            o = client.getObject(gor);
            s3is = o.getObjectContent();

            reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(s3is)));

            while ((val = reader.read()) != -1) {
                char c = (char) val;
                line += c;
                if (c == '\n' || c == '\r') {
                    /** Found complete line */
                    Validator.validateCSVLine(line, csvFormat);
                    return;
                }
            }
        }catch (EOFException e) {
            /** Ignore incomplete stream */
        }finally {
            if(s3is !=null) {
                s3is.abort();
                s3is.close();
            }
            if(o != null)
                o.close();
            if(reader != null)
                reader.close();
        }

        if(toKB <= VALIDATE_LINE_MAX_LINE_SIZE_BYTES) {
            /** not found a line break till now - search further */
            toKB = toKB + VALIDATE_LINE_KB_STEPS;
            validateFirstZippedCSVLine(key_name, bucket_name, csvFormat, line, toKB);
        }
        else
            throw new UnsupportedEncodingException("Not able to find EOL!");
    }

    public Map<String, ExportObject> scanExportPath(Export job, boolean isSuperPath, boolean createDownloadUrl){
        Map<String, ExportObject> exportObjectMap = new HashMap<>();
        String path = isSuperPath ? job.readParamSuperExportPath() : getS3Path(job);
        exportObjectMap.putAll(scanExportPath(path, CService.configuration.JOBS_S3_BUCKET, createDownloadUrl));
        return exportObjectMap;
    }

    public Map<String,ExportObject> scanExportPath(String prefix, String bucketName, boolean createDownloadUrl) {
        Map<String, ExportObject> exportObjectList = new HashMap<>();
        ListObjectsRequest listObjects = new ListObjectsRequest()
                .withPrefix(prefix)
                .withBucketName(bucketName);

        ObjectListing objectListing = client.listObjects(listObjects);

        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            //Skip empty files
            if(objectSummary.getSize() == 0)
                continue;

            ExportObject eo = new ExportObject(objectSummary.getKey(), objectSummary.getSize());
            if (eo.getFilename().equalsIgnoreCase("manifest.json"))
                continue;;

            exportObjectList.put(eo.getFilename(prefix), eo);
            //if (createDownloadUrl) {
            //    try {
            //        eo.setDownloadUrl(generateDownloadURL(bucketName, eo.getS3Key()));
            //    }
            //    catch (Exception e) {
            //        logger.error("[{}] Cant create download-url! ", prefix, e);
            //    }
            //}
        }

        return exportObjectList;
    }

    public void writeMetaFile(Export job){
        writeMetaFile(job, CService.configuration.JOBS_S3_BUCKET);
    }

    public void writeMetaFile(Export job, String bucketName){
        String path = getS3Path(job) + "/manifest.json";
        writeMetaFile(job, path, bucketName);
    }

    public void writeMetaFile(Export job , String filePath, String bucketName){
        try {
            byte[] meta = new ObjectMapper().writeValueAsBytes(job);

            ObjectMetadata omd = new ObjectMetadata();
            omd.setContentLength(meta.length);
            omd.setContentType("application/json");
            client.putObject(bucketName, filePath, new ByteArrayInputStream(meta), omd);

        } catch (AmazonServiceException | IOException e) {
            logger.error("job[{}] cant write Metafile! ", job.getId(), e);
        }
    }

    public String checkPersistentS3ExportOfSuperLayer(String superLayer, Export sourceJob) {
        String path = getS3Path(superLayer, sourceJob.getTargetLevel());
        Export superExport = readMetaFileFromPath(path);
        if(superExport == null)
            return null;
        return path;
    }

    public Export readMetaFileFromJob(Export job) {
        return readMetaFileFromPath(getS3Path(job));
    }

    public Export readMetaFileFromPath(String path) {
        try {
            path += "/manifest.json";
            S3Object s3Object = client.getObject(CService.configuration.JOBS_S3_BUCKET, path);
            if(s3Object != null)
                return new ObjectMapper().readValue(s3Object.getObjectContent(), Export.class);
        } catch (Exception e) {
            logger.error("Cant read Metafile from path '{}'! ", path, e);
        }
        return null;
    }

    public String getS3Path(String targetSpaceId, String subfolder, boolean subHashed) {
     return String.join("/", new String[]{CService.jobS3Client.EXPORT_PERSIST_FOLDER, Hasher.getHash(targetSpaceId),
         !subHashed ? subfolder : Hasher.getHash(subfolder)});
    }

    public String getS3Path(String targetSpaceId, int targetLevel) {
        return getS3Path(targetSpaceId, "" + targetLevel, false);
    }

    public String getS3Path(Job job) {
        if (job instanceof Import)
            return IMPORT_UPLOAD_FOLDER +"/"+ job.getId();

        String s3Path = CService.jobS3Client.EXPORT_DOWNLOAD_FOLDER + "/" + job.getId();

        if (job instanceof Export && ((Export) job).readParamPersistExport()) {
            Export exportJob = (Export) job;
            if (exportJob.getExportTarget().getType() == Export.ExportTarget.Type.VML && exportJob.getFilters() == null) {
              if (job.getCsvFormat() == CSVFormat.TILEID_FC_B64)
                  s3Path = getS3Path(job.getTargetSpaceId(),exportJob.getTargetLevel());
              else {
                  String folder =  exportJob.getPartitionKey() != null && !"id".equalsIgnoreCase(exportJob.getPartitionKey()) ? exportJob.getPartitionKey() : "id";
                  s3Path = getS3Path(job.getTargetSpaceId(), folder, true);
              }
            }
        }
        return s3Path;
    }

    public void cleanJobData(Job job, boolean force){
        String path = getS3Path(job);

        if(job instanceof Export && ((Export) job).readParamPersistExport() && !force){
            //force is required!
            return;
        }else if(job instanceof Import && !force){
            //force is required!
            return;
        }

        this.deleteS3Folder(CService.configuration.JOBS_S3_BUCKET, path + "/");
    }
}
