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

package com.here.xyz.models.payload.responses.maintenance;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.models.payload.XyzResponse;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "SpaceStatus")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class SpaceStatus extends XyzResponse {
    private long runts;

    private long count;

    private String spaceId;

    private Boolean idxCreationFinished;

    private Boolean autoIndexing;

    private List<IDXAvailable> idxAvailable;

    private List<IDXProposals> idxProposals;

    private List<PropStat> propStats;

    private IDXManual idxManual;

    public long getRunts() {
        return runts;
    }

    public void setRunTs(long runTs) {
        this.runts = runts;
    }

    public String getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(String spaceId) {
        this.spaceId = spaceId;
    }

    public Boolean isIdxCreationFinished() {
        return idxCreationFinished;
    }

    public void setIdxCreationFinished(Boolean idxCreationFinished) {
        this.idxCreationFinished = idxCreationFinished;
    }

    public Boolean isAutoIndexing() {
        return autoIndexing;
    }

    public void setAutoIndexing(Boolean autoIndexing) {
        this.autoIndexing = autoIndexing;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public List<IDXAvailable> getIdxAvailable() {
        return idxAvailable;
    }

    public void setIdxAvailable(List<IDXAvailable> idxAvailable) {
        this.idxAvailable = idxAvailable;
    }

    public IDXManual getIdxManual() {
        return idxManual;
    }

    public void setIdxManual(IDXManual idxManual) {
        this.idxManual = idxManual;
    }

    public List<IDXProposals> getIdxProposals() {
        return idxProposals;
    }

    public void setIdxProposals(List<IDXProposals> idxProposals) {
        this.idxProposals = idxProposals;
    }

    public List<PropStat> getPropStats() {
        return propStats;
    }

    public void setPropStats(List<PropStat> propStats) {
        this.propStats = propStats;
    }

    public static class IDXAvailable {
        private String src;
        private String property;

        public String getSrc() {
            return src;
        }

        public void setSrc(String src) {
            this.src = src;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }
    }

    public static class IDXManual {
        private Map<String, Boolean> searchableProperties;
        private List<List<Object>> sortableProperties;

        public Map<String, Boolean> getSearchableProperties() {
            return searchableProperties;
        }

        public void setSearchableProperties(Map<String, Boolean> searchableProperties) {
            this.searchableProperties = searchableProperties;
        }

        public List<List<Object>> getSortableProperties() {
            return sortableProperties;
        }

        public void setSortableProperties(List<List<Object>> sortableProperties) {
            this.sortableProperties = sortableProperties;
        }
    }

    public static class PropStat {
        private String key;
        private String datatype;
        private Long count;
        private Boolean searchable;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getDatatype() {
            return datatype;
        }

        public void setDatatype(String datatype) {
            this.datatype = datatype;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public Boolean getSearchable() {
            return searchable;
        }

        public void setSearchable(Boolean searchable) {
            this.searchable = searchable;
        }
    }

    public static class IDXProposals {
        private String type;
        private String property;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }
    }
}
