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

package com.here.xyz.psql.config;

import com.here.xyz.connectors.AbstractConnectorHandler.TraceItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Map;

public class ConnectorParameters {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Paramters
     */
    public final static String CONNECTOR_ID = "connectorId";
    public final static String PROPERTY_SEARCH = "propertySearch";
    public final static String MVT_SUPPORT = "mvtSupport";
    public final static String AUTO_INDEXING = "autoIndexing";
    public final static String ENABLE_HASHED_SPACEID = "enableHashedSpaceId";
    public final static String COMPACT_HISTORY = "compactHistory";
    public final static String ON_DEMAND_IDX_LIMIT = "onDemandIdxLimit";
    public final static String HRN_SHORTENING = "hrnShortening";
    public final static String IGNORE_CREATE_MSE = "ignoreCreateMse";

    public final static String DB_INITIAL_POOL_SIZE = "dbInitialPoolSize";
    public final static String DB_MIN_POOL_SIZE = "dbMinPoolSize";
    public final static String DB_MAX_POOL_SIZE = "dbMaxPoolSize";
    public final static String DB_ACQUIRE_INCREMENT = "dbAcquireIncrement";
    public final static String DB_ACQUIRE_RETRY_ATTEMPTS = "dbAcquireRetryAttempts";
    public final static String DB_CHECKOUT_TIMEOUT = "dbCheckoutTimeout";
    public final static String DB_TEST_CONNECTION_ON_CHECKOUT = "dbTestConnectionOnCheckout";
    public final static String DB_MAX_IDLE_TIME = "dbMaxIdleTime";

    /**
     * Connector Settings defaults
     */
    private String connectorId;
    private boolean propertySearch = false;
    private boolean mvtSupport = false;
    private boolean autoIndexing = false;
    private boolean enableHashedSpaceId = false;
    private boolean compactHistory = true;
    private int onDemandIdxLimit = 4;
    private boolean hrnShortening = false;
    private boolean ignoreCreateMse = false;
    private String ecps;

    /**
     * Connection Pool defaults
     */
    private int dbInitialPoolSize = 1;
    private int dbMinPoolSize = 1;
    private int dbMaxPoolSize = 1;
    private int dbAcquireIncrement = 1;
    private int dbAcquireRetryAttempts = 5;
    private int dbCheckoutTimeout = 7;
    private boolean dbTestConnectionOnCheckout = true;
    private Integer dbMaxIdleTime = null;

    private TraceItem TraceItem;

    public ConnectorParameters(Map<String, Object> connectorParams, TraceItem TraceItem){
        this.TraceItem = TraceItem;

        if (connectorParams != null) {
            this.connectorId = parseValue(connectorParams, String.class, connectorId, CONNECTOR_ID);
            this.autoIndexing = parseValue(connectorParams, Boolean.class, autoIndexing, AUTO_INDEXING);
            this.propertySearch = parseValue(connectorParams, Boolean.class, propertySearch, PROPERTY_SEARCH);
            this.mvtSupport = parseValue(connectorParams, Boolean.class, mvtSupport, MVT_SUPPORT);
            this.enableHashedSpaceId = parseValue(connectorParams, Boolean.class, enableHashedSpaceId, ENABLE_HASHED_SPACEID);
            this.compactHistory = parseValue(connectorParams, Boolean.class, compactHistory, COMPACT_HISTORY);
            this.onDemandIdxLimit = parseValue(connectorParams, Integer.class, onDemandIdxLimit, ON_DEMAND_IDX_LIMIT);
            hrnShortening = parseValue(connectorParams, Boolean.class, hrnShortening, HRN_SHORTENING);
            ignoreCreateMse = parseValue(connectorParams, Boolean.class, ignoreCreateMse, IGNORE_CREATE_MSE);

            this.dbInitialPoolSize = parseValue(connectorParams, Integer.class, dbInitialPoolSize, DB_INITIAL_POOL_SIZE);
            this.dbMinPoolSize = parseValue(connectorParams, Integer.class, dbMinPoolSize, DB_MIN_POOL_SIZE);
            this.dbMaxPoolSize = parseValue(connectorParams, Integer.class, dbMaxPoolSize, DB_MAX_POOL_SIZE);
            this.dbAcquireIncrement = parseValue(connectorParams, Integer.class, dbAcquireIncrement, DB_ACQUIRE_INCREMENT);
            this.dbAcquireRetryAttempts = parseValue(connectorParams, Integer.class, dbAcquireRetryAttempts, DB_ACQUIRE_RETRY_ATTEMPTS);
            this.dbCheckoutTimeout = parseValue(connectorParams, Integer.class, dbCheckoutTimeout, DB_CHECKOUT_TIMEOUT);
            this.dbTestConnectionOnCheckout = parseValue(connectorParams, Boolean.class, dbTestConnectionOnCheckout, DB_TEST_CONNECTION_ON_CHECKOUT);
            this.dbMaxIdleTime = parseValue(connectorParams, Integer.class, dbMaxIdleTime, DB_MAX_IDLE_TIME);

            this.ecps = parseValue(connectorParams, String.class, null, "ecps");
        }
    }

    private <T> T parseValue(Map<String, Object> connectorParams, Class<T> type, Object defaultValue, String parameter){
        Object value = connectorParams.get(parameter);

        if(value == null) {
            if(defaultValue == null)
                return null;
            return (T) defaultValue;
        }
        if(value.getClass() != type) {
            logger.warn("{} Cannot set value {}:{}. Load default '{}'", TraceItem, parameter, value, defaultValue);
            return (T) defaultValue;
        }

        try{
            if (value instanceof String)
                return (T) (String)value;
            else if (value instanceof Integer)
                return (T) (Integer)value;
            else if (value instanceof Boolean)
                return (T) (Boolean)value;
            else
                throw new Exception("Unknown - Take default");
        }catch (Exception e){
            logger.warn("{} Cannot set value {}:{}. Load default '{}'", TraceItem, parameter, value, defaultValue);
            return (T) defaultValue;
        }
    }

    public String getConnectorId() { return connectorId; }

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

    public int getDbInitialPoolSize() {
        return dbInitialPoolSize;
    }

    public int getDbMinPoolSize() {
        return dbMinPoolSize;
    }

    public int getDbMaxPoolSize() {
        return dbMaxPoolSize;
    }

    public int getDbAcquireIncrement() {
        return dbAcquireIncrement;
    }

    public int getDbAcquireRetryAttempts() {
        return dbAcquireRetryAttempts;
    }

    public int getDbCheckoutTimeout() {
        return dbCheckoutTimeout;
    }

    public boolean isDbTestConnectionOnCheckout() { return dbTestConnectionOnCheckout; }

    public Integer getDbMaxIdleTime() {
        return dbMaxIdleTime;
    }

    public String getEcps() {
        return ecps;
    }

    public void setDbMaxPoolSize(int dbMaxPoolSize) {
        this.dbMaxPoolSize = dbMaxPoolSize;
    }

    @Override
    public String toString() {
        return "ConnectorParameters{" +
                "propertySearch=" + propertySearch +
                ", mvtSuppoert=" + mvtSupport +
                ", autoIndexing=" + autoIndexing +
                ", enableHashedSpaceId=" + enableHashedSpaceId +
                ", compactHistory=" + compactHistory +
                ", onDemandIdxLimit=" + onDemandIdxLimit +
                ", dbInitialPoolSize=" + dbInitialPoolSize +
                ", dbMinPoolSize=" + dbMinPoolSize +
                ", dbMaxPoolSize=" + dbMaxPoolSize +
                ", dbAcquireIncrement=" + dbAcquireIncrement +
                ", dbAcquireRetryAttempts=" + dbAcquireRetryAttempts +
                ", dbCheckoutTimeout=" + dbCheckoutTimeout +
                ", dbTestConnectionOnCheckout=" + dbTestConnectionOnCheckout +
                ", dbMaxIdleTime=" + dbMaxIdleTime +
                ", ecps='" + ecps + '\'' +
                '}';
    }
}