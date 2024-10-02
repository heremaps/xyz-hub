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

package com.here.xyz.jobs.steps.outputs;

public class FileStatistics extends ModelBasedOutput {
    private long exportedFeatures;
    private long exportedBytes;
    private int exportedFiles;

    public long getExportedFeatures() {
        return exportedFeatures;
    }

    public void setExportedFeatures(long exportedFeatures) {
        this.exportedFeatures = exportedFeatures;
    }

    public FileStatistics withRowsExported(long rowsExported) {
        setExportedFeatures(rowsExported);
        return this;
    }

    public long getExportedBytes() {
        return exportedBytes;
    }

    public void setExportedBytes(long exportedBytes) {
        this.exportedBytes = exportedBytes;
    }

    public FileStatistics withBytesExported(long bytesExported) {
        setExportedBytes(bytesExported);
        return this;
    }

    public int getExportedFiles() {
        return exportedFiles;
    }

    public void setExportedFiles(int exportedFiles) {
        this.exportedFiles = exportedFiles;
    }

    public FileStatistics withFilesCreated(int filesCreated) {
        setExportedFiles(filesCreated);
        return this;
    }
}
