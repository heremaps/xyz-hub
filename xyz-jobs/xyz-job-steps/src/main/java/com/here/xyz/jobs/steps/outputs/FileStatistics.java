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
    private long rowsExported;
    private long bytesExported;
    private int filesCreated;

    public long getRowsExported() {
        return rowsExported;
    }

    public void setRowsExported(long rowsExported) {
        this.rowsExported = rowsExported;
    }

    public FileStatistics withRowsExported(long rowsExported) {
        setRowsExported(rowsExported);
        return this;
    }

    public long getBytesExported() {
        return bytesExported;
    }

    public void setBytesExported(long bytesExported) {
        this.bytesExported = bytesExported;
    }

    public FileStatistics withBytesExported(long bytesExported) {
        setBytesExported(bytesExported);
        return this;
    }

    public int getFilesCreated() {
        return filesCreated;
    }

    public void setFilesCreated(int filesCreated) {
        this.filesCreated = filesCreated;
    }

    public FileStatistics withFilesCreated(int filesCreated) {
        setFilesCreated(filesCreated);
        return this;
    }
}
