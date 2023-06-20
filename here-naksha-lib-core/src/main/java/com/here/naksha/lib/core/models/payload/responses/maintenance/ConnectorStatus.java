/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.naksha.lib.core.models.payload.responses.maintenance;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import java.util.Map;
import java.util.Set;

/** TBD send back an {@link ErrorResponse}. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ConnectorStatus")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ConnectorStatus extends XyzResponse {
    public static final String AUTO_INDEXING = "AUTO_INDEXING";

    private boolean initialized;

    private String[] extensions;

    @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
    private Map<String, Integer> scriptVersions;

    private Map<String, MaintenanceStatus> maintenanceStatus;

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public ConnectorStatus withInitialized(boolean initialized) {
        setInitialized(initialized);
        return this;
    }

    public String[] getExtensions() {
        return extensions;
    }

    public void setExtensions(String[] extensions) {
        this.extensions = extensions;
    }

    public ConnectorStatus withExtensions(String[] extensions) {
        setExtensions(extensions);
        return this;
    }

    public Map<String, Integer> getScriptVersions() {
        return scriptVersions;
    }

    public void setScriptVersions(Map<String, Integer> scriptVersions) {
        this.scriptVersions = scriptVersions;
    }

    public ConnectorStatus withScriptVersions(Map<String, Integer> scriptVersions) {
        setScriptVersions(scriptVersions);
        return this;
    }

    public Map<String, MaintenanceStatus> getMaintenanceStatus() {
        return maintenanceStatus;
    }

    public void setMaintenanceStatus(Map<String, MaintenanceStatus> maintenanceStatus) {
        this.maintenanceStatus = maintenanceStatus;
    }

    public ConnectorStatus withMaintenanceStatus(Map<String, MaintenanceStatus> maintenanceStatus) {
        setMaintenanceStatus(maintenanceStatus);
        return this;
    }

    public static class MaintenanceStatus {
        private long maintainedAt;
        private Set<String> maintenanceRunning;

        public long getMaintainedAt() {
            return maintainedAt;
        }

        public void setMaintainedAt(long maintainedAt) {
            this.maintainedAt = maintainedAt;
        }

        public Set<String> getMaintenanceRunning() {
            return maintenanceRunning;
        }

        public void setMaintenanceRunning(Set<String> maintenanceRunning) {
            this.maintenanceRunning = maintenanceRunning;
        }
    }
}
