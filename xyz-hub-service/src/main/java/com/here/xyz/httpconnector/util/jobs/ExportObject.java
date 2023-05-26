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
package com.here.xyz.httpconnector.util.jobs;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.net.URL;

public class ExportObject {
    @JsonIgnore
    private String s3Key;
    private URL downloadUrl;
    private Status status;
    private long filesize;

    public enum Status {
        exported, failed;
        public static ExportObject.Status of(String value) {
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

    public ExportObject(){}

    public ExportObject(String s3Key, Long fileSize){
        this.s3Key = s3Key;
        this.filesize = fileSize;
        this.status = Status.exported;
    }

    @JsonIgnore
    public String getFilename() {
        if(s3Key == null)
            return null;
        return s3Key.substring(s3Key.lastIndexOf("/")+1);
    }

    @JsonIgnore
    public String getFilename(String prefix) {
        if(s3Key == null || prefix == null)
            return null;

        int pos = s3Key.indexOf(prefix);
        if(pos == -1)
            getFilename();

        return s3Key.substring(pos + prefix.length() + 1);
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public URL getDownloadUrl(){
        return downloadUrl;
    }

    public void setDownloadUrl(URL downloadUrl) {
        this.downloadUrl = downloadUrl;
    }

    public long getFilesize() {
        return filesize;
    }

    public void setFilesize(long filesize){
        this.filesize = filesize;
    }

    public String getS3Key() {
        return s3Key;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "ExportObject{" +
                "s3key='" + s3Key + '\'' +
                ", filename='" + getFilename() + '\'' +
                ", downloadUrl=" + downloadUrl +
                ", filesize=" + filesize +
                '}';
    }
}
