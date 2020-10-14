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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Map;

public class ConnectorParameters {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Default Limit of on possible Demand-Indices
     */
    public final int ON_DEMAND_IDX_LIM_DEFAULT = 4;

    /**
     * Connection Pool defaults
     */
    private final int DB_INITIAL_POOL_SIZE_DEFAULT = 1;
    private final int DB_MIN_POOL_SIZE_DEFAULT = 1;
    /** Can get overwritten through an Env-Variable */
    private final int DB_MAX_POOL_SIZE_DEFAULT = 1;
    private final int DB_ACQUIRE_INCREMENT_DEFAULT = 1;
    private final int DB_ACQUIRE_RETRY_ATTEMPTS_DEFAULT = 5;
    private final int DB_CHECKOUT_TIMEOUT_DEFAULT = 7;
    private final boolean DB_TEST_CONNECTION_ON_CHECKOUT_DEFAULT = false;
    /** If null - Idle Time does not get used */
    private final Integer DB_MAX_IDLE_TIME_DEFAULT = null;

    private boolean propertySearch;
    private boolean autoIndexing;
    private boolean enableHashedSpaceId;
    private boolean compactHistory;
    private int onDemandIdxLimit;

    private int dbInitialPoolSize;
    private int dbMinPoolSize;
    private int dbMaxPoolSize;
    private int dbAcquireIncrement;
    private int dbAcquireRetryAttempts;
    private int dbCheckoutTimeout;
    private boolean dbTestConnectionOnCheckout;
    private Integer dbMaxIdleTime;
    private String ecps;
    private String streamId;

    public ConnectorParameters(Map<String, Object> connectorParams, String streamId){
        this.streamId = streamId;

        if (connectorParams != null) {
            this.autoIndexing = parseValue(connectorParams, Boolean.class, Boolean.FALSE, "autoIndexing");
            this.propertySearch = parseValue(connectorParams, Boolean.class, Boolean.FALSE, "propertySearch");
            this.enableHashedSpaceId = parseValue(connectorParams, Boolean.class, Boolean.FALSE, "enableHashedSpaceId");
            this.compactHistory = parseValue(connectorParams, Boolean.class, Boolean.TRUE, "compactHistory");
            this.onDemandIdxLimit = parseValue(connectorParams, Integer.class, ON_DEMAND_IDX_LIM_DEFAULT, "onDemandIdxLimit");

            this.dbInitialPoolSize = parseValue(connectorParams, Integer.class, DB_INITIAL_POOL_SIZE_DEFAULT, "dbInitialPoolSize");
            this.dbMinPoolSize = parseValue(connectorParams, Integer.class, DB_MIN_POOL_SIZE_DEFAULT, "dbMinPoolSize");
            this.dbMaxPoolSize = parseValue(connectorParams, Integer.class, DB_MAX_POOL_SIZE_DEFAULT, "dbMaxPoolSize");
            this.dbAcquireIncrement = parseValue(connectorParams, Integer.class, DB_ACQUIRE_RETRY_ATTEMPTS_DEFAULT, "dbAcquireIncrement");
            this.dbAcquireRetryAttempts = parseValue(connectorParams, Integer.class, DB_ACQUIRE_RETRY_ATTEMPTS_DEFAULT, "dbAcquireRetryAttemptsPoolSize");
            this.dbCheckoutTimeout = parseValue(connectorParams, Integer.class, DB_CHECKOUT_TIMEOUT_DEFAULT, "dbCheckoutTimeout");
            this.dbTestConnectionOnCheckout = parseValue(connectorParams, Boolean.class, DB_TEST_CONNECTION_ON_CHECKOUT_DEFAULT, "dbTestConnectionOnCheckout");
            this.dbMaxIdleTime = parseValue(connectorParams, Integer.class, DB_MAX_IDLE_TIME_DEFAULT, "dbMaxIdleTime");

            this.ecps = parseValue(connectorParams, String.class, null, "ecps");
        }else{
            this.autoIndexing = Boolean.FALSE;
            this.propertySearch = Boolean.FALSE;
            this.enableHashedSpaceId = Boolean.FALSE;
            this.compactHistory = Boolean.TRUE;

            this.onDemandIdxLimit = ON_DEMAND_IDX_LIM_DEFAULT;

            this.dbInitialPoolSize = DB_INITIAL_POOL_SIZE_DEFAULT;
            this.dbMinPoolSize = DB_MIN_POOL_SIZE_DEFAULT;
            this.dbMaxPoolSize = DB_MAX_POOL_SIZE_DEFAULT;
            this.dbAcquireIncrement = DB_ACQUIRE_INCREMENT_DEFAULT;
            this.dbAcquireRetryAttempts = DB_ACQUIRE_RETRY_ATTEMPTS_DEFAULT;
            this.dbCheckoutTimeout = DB_CHECKOUT_TIMEOUT_DEFAULT;
            this.dbTestConnectionOnCheckout = DB_TEST_CONNECTION_ON_CHECKOUT_DEFAULT;
            this.dbMaxIdleTime = DB_MAX_IDLE_TIME_DEFAULT;
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
            logger.warn("{} - Cannot set value {}:{}. Load default '{}'", streamId, parameter, value, defaultValue);
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
            logger.warn("{} - Cannot set value {}:{}. Load default '{}'", streamId, parameter, value, defaultValue);
            return (T) defaultValue;
        }
    }

    public boolean isPropertySearch() {
        return propertySearch;
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
