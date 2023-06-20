/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.psql;


import com.here.mapcreator.ext.naksha.PsqlConfig;
import com.here.xyz.EventHandlerParams;
import com.here.xyz.util.json.JsonSerializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The PostgresQL connector parameters. */
@SuppressWarnings({"unused", "rawtypes", "unchecked"})
public class PsqlHandlerParams extends EventHandlerParams {

    /**
     * The master database configuration ({@link PsqlConfig}), must be provided, is used for read and
     * write.
     */
    public static final String DB_CONFIG = "dbConfig";
    /**
     * An array of {@link PsqlConfig}'s to be used as read-replicas, when read from replica is okay
     * for the client.
     */
    public static final String DB_REPLICAS = "dbReplicas";

    // TODO: Document the parameters!
    public static final String PROPERTY_SEARCH = "propertySearch";
    public static final String MVT_SUPPORT = "mvtSupport";
    public static final String AUTO_INDEXING = "autoIndexing";
    public static final String ENABLE_HASHED_SPACEID = "enableHashedSpaceId";
    public static final String COMPACT_HISTORY = "compactHistory";
    public static final String ON_DEMAND_IDX_LIMIT = "onDemandIdxLimit";
    public static final String HRN_SHORTENING = "hrnShortening";
    public static final String IGNORE_CREATE_MSE = "ignoreCreateMse";

    private final boolean propertySearch;
    private final boolean mvtSupport;
    private final boolean autoIndexing;
    private final boolean enableHashedSpaceId;
    private final boolean compactHistory;
    private final int onDemandIdxLimit;
    private final boolean hrnShortening;
    private final boolean ignoreCreateMse;

    private final @NotNull PsqlConfig dbConfig;
    private final @NotNull List<@NotNull PsqlConfig> dbReplicas;

    /**
     * Parse the given connector params into this type-safe class.
     *
     * @param connectorParams the connector parameters.
     * @throws NullPointerException if a value is {@code null} that must not be null.
     * @throws IllegalArgumentException if a value has an invalid type, for example a map expected,
     *     and a string found.
     */
    public PsqlHandlerParams(@NotNull Map<@NotNull String, @Nullable Object> connectorParams)
            throws NullPointerException {
        Object raw = connectorParams.get(DB_CONFIG);
        if (raw instanceof Map params) {
            this.dbConfig = JsonSerializable.fromAnyMap(params, PsqlConfig.class);
        } else {
            throw new IllegalArgumentException(DB_CONFIG);
        }
        raw = connectorParams.get(DB_REPLICAS);
        final ArrayList<@NotNull PsqlConfig> replicas;
        if (raw instanceof List list) {
            final int SIZE = list.size();
            replicas = new ArrayList<>(SIZE);
            for (final Object o : list) {
                if (o instanceof Map m) {
                    replicas.add(JsonSerializable.fromAnyMap(m, PsqlConfig.class));
                }
            }
        } else {
            replicas = new ArrayList<>();
        }
        dbReplicas = replicas;

        autoIndexing = parseValueWithDefault(connectorParams, AUTO_INDEXING, false);
        propertySearch = parseValueWithDefault(connectorParams, PROPERTY_SEARCH, false);
        mvtSupport = parseValueWithDefault(connectorParams, MVT_SUPPORT, false);
        enableHashedSpaceId = parseValueWithDefault(connectorParams, ENABLE_HASHED_SPACEID, false);
        compactHistory = parseValueWithDefault(connectorParams, COMPACT_HISTORY, true);
        onDemandIdxLimit = parseValueWithDefault(connectorParams, ON_DEMAND_IDX_LIMIT, 4);
        hrnShortening = parseValueWithDefault(connectorParams, HRN_SHORTENING, false);
        ignoreCreateMse = parseValueWithDefault(connectorParams, IGNORE_CREATE_MSE, false);
    }

    public boolean isPropertySearch() {
        return propertySearch;
    }

    public boolean isMvtSupport() {
        return mvtSupport;
    }

    public boolean isAutoIndexing() {
        return autoIndexing;
    }

    public boolean isEnableHashedSpaceId() {
        return enableHashedSpaceId;
    }

    public boolean isCompactHistory() {
        return compactHistory;
    }

    public int getOnDemandIdxLimit() {
        return onDemandIdxLimit;
    }

    public boolean isHrnShortening() {
        return hrnShortening;
    }

    public boolean isIgnoreCreateMse() {
        return ignoreCreateMse;
    }

    /**
     * Returns the master database configuration.
     *
     * @return the master database configuration.
     */
    public @NotNull PsqlConfig getDbConfig() {
        return dbConfig;
    }

    /**
     * Returns all configured replicas; the returned list may be empty.
     *
     * @return all configured replicas; the returned list may be empty.
     */
    public @NotNull List<@NotNull PsqlConfig> getDbReplicas() {
        return dbReplicas;
    }

    @Override
    public String toString() {
        // TODO: Add replicas?
        // TODO: Do we need all these parameters?
        return "ConnectorParameters{"
                + "propertySearch="
                + propertySearch
                + ", mvtSupport="
                + mvtSupport
                + ", autoIndexing="
                + autoIndexing
                + ", enableHashedSpaceId="
                + enableHashedSpaceId
                + ", compactHistory="
                + compactHistory
                + ", onDemandIdxLimit="
                + onDemandIdxLimit
                + ", dbConfig="
                + dbConfig
                +
                // ", dbReplicas=" + dbReplicas +
                '}';
    }
}
