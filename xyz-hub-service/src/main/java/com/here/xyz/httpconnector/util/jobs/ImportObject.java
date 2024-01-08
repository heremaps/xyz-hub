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

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.here.xyz.httpconnector.config.JobS3Client;

import java.net.URL;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ImportObject {
    public static int MAX_RETRIES = 2;

    @JsonIgnore
    private String s3Key;
    private URL uploadUrl;
    private boolean compressed;
    private Boolean valid;
    private Status status;
    private String details;
    private int retryCount;

    private long filesize;

    public enum Status {
        waiting, imported, failed;
        public static Status of(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.toLowerCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    public ImportObject(){}

    public ImportObject(String s3Key, URL uploadUrl) {
        this.s3Key = s3Key;
        this.uploadUrl = uploadUrl;

    }

    public ImportObject(S3ObjectSummary s3ObjectSummary, ObjectMetadata objectMetadata) {
        this.s3Key = s3ObjectSummary.getKey();
        this.filesize = s3ObjectSummary.getSize();

        if(objectMetadata.getContentEncoding() != null &&
            objectMetadata.getContentEncoding().equalsIgnoreCase("gzip"))
                this.compressed = true;
    }

    @JsonIgnore
    public String getFilename() {
        if(s3Key == null)
            return null;
        return s3Key.substring(s3Key.lastIndexOf("/")+1);
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public void setUploadUrl(URL uploadUrl) {
        this.uploadUrl = uploadUrl;
    }

    public void setValid(boolean valid){
        this.valid = valid;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public void setFilesize(long filesize){
        if(filesize == -1) {
            this.status = Status.failed;
            this.valid = false;
        }
        this.filesize = filesize;
    }

    public String getS3Key() { return s3Key;}

    public String getS3Key(String jobId, String part){
        return JobS3Client.getImportPath(jobId, part);
    }

    public URL getUploadUrl(){ return uploadUrl;}

    public boolean isCompressed() {
        return compressed;
    }

    public Boolean isValid() {
        return valid;
    }

    public long getFilesize() {
        return filesize;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getDetails() { return details; }

    public void setDetails(String details) {
        this.details = details;
    }

    public int getRetryCount() { return retryCount; }

    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public void increaseRetryCount() { this.retryCount++;}

    @JsonIgnore
    public boolean isRetryPossible() { return this.retryCount < MAX_RETRIES ; }

    @Override
    public String toString() {
        return "ImportObject{" +
                "s3Key='" + s3Key + '\'' +
                ", uploadUrl=" + uploadUrl +
                ", compressed=" + compressed +
                ", valid=" + valid +
                ", status=" + status +
                ", details='" + details + '\'' +
                ", retryCount=" + retryCount +
                ", filesize=" + filesize +
                '}';
    }
}