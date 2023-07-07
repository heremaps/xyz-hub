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

package com.here.xyz.psql.config;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//TODO: Use Jackson for mapping the incoming connectorParams into an instance of ConnectorParameters
public class ConnectorParameters {
  private static final Logger logger = LogManager.getLogger();

  private String connectorId;
  private String ecps;

  /**
   * Connector Settings defaults
   */
  private boolean propertySearch = false;
  private boolean mvtSupport = false;
  private boolean autoIndexing = false;
  private boolean enableHashedSpaceId = false;
  private int onDemandIdxLimit = 4;
  private boolean ignoreCreateMse = false;

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
  private int statementTimeoutSeconds = 23;

  public ConnectorParameters(Map<String, Object> connectorParams) {
    if (connectorParams != null) {
      Map<String, Field> parameters = Arrays.stream(getClass().getDeclaredFields()).collect(Collectors.toMap(Field::getName, Function.identity()));
      for (Entry<String, Object> param : connectorParams.entrySet())
        try {
          if (!parameters.containsKey(param.getKey())) {
            //TODO: Re-activate once the db-settings / connector-params were organized properly
//            logger.warn("Cannot set value {} = {}. No such field exists.", param.getKey(), param.getValue());
          }
          else
            try {
              assignValue(parameters.get(param.getKey()), param.getValue());
            }
            catch (ClassCastException e) {
              logger.warn("Cannot set value {} = {}. Using default {} instead.", param.getKey(), param.getValue(),
                  parameters.get(param.getKey()).get(this), e);
            }
        }
        catch (IllegalAccessException e) {
          logger.warn("Cannot set value {} = {}. Using default instead.", param.getKey(), param.getValue(), e);
        }
    }
  }

  private void assignValue(Field field, Object value) throws IllegalAccessException, ClassCastException {
    if (field.getType().isAssignableFrom(value.getClass())
        || field.getType().isPrimitive() != value.getClass().isPrimitive() && primitiveTypesMatch(field.getType(), value.getClass()))
      field.set(this, value);
    else
      throw new ClassCastException("Can not cast value of type " + value.getClass().getName() + " to " + field.getType().getName());
  }

  private static boolean primitiveTypesMatch(Class<?> type1, Class<?> type2) {
    if (type1 == type2)
      return true;
    if (type1 == int.class && type2 == Integer.class
        || type1 == Integer.class && type2 == int.class
        || type1 == char.class && type2 == Character.class
        || type1 == Character.class && type2 == char.class)
      return true;
    return type1.getSimpleName().toLowerCase().equals(type2.getSimpleName().toLowerCase());
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

  public int getOnDemandIdxLimit() {
    return onDemandIdxLimit;
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

  public boolean isDbTestConnectionOnCheckout() {
    return dbTestConnectionOnCheckout;
  }

  public Integer getDbMaxIdleTime() {
    return dbMaxIdleTime;
  }

  public String getEcps() {
    return ecps;
  }

  public void setDbMaxPoolSize(int dbMaxPoolSize) {
    this.dbMaxPoolSize = dbMaxPoolSize;
  }

  public int getStatementTimeoutSeconds() {
    return statementTimeoutSeconds;
  }

  @Override
  public String toString() {
    return "ConnectorParameters{" +
            "propertySearch=" + propertySearch +
            ", mvtSuppoert=" + mvtSupport +
            ", autoIndexing=" + autoIndexing +
            ", enableHashedSpaceId=" + enableHashedSpaceId +
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