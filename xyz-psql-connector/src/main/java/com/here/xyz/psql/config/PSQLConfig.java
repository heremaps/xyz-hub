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

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.subtle.AesGcmJce;
import com.here.xyz.connectors.AbstractConnectorHandler.TraceItem;
import com.here.xyz.connectors.SimulatedContext;
import com.here.xyz.events.Event;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.util.Hasher;
import com.here.xyz.psql.tools.DhString;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;

import static com.here.xyz.psql.config.DatabaseSettings.*;

public class PSQLConfig {
  private static final Logger logger = LogManager.getLogger();
  public static final String ECPS_PHRASE = "ECPS_PHRASE";
  public static final String MAINTENANCE_ENDPOINT = "MAINTENANCE_SERVICE_ENDPOINT";

  private DataSource dataSource;
  private DataSource readDataSource;
  private DatabaseMaintainer databaseMaintainer;
  private String maintenanceServiceEndpoint;

  public void addDataSource(DataSource dataSource){
    this.dataSource = dataSource;
  }
  public void addReadDataSource(DataSource readDataSource){
    this.readDataSource = readDataSource;
  }

  public void addDatabaseMaintainer(DatabaseMaintainer databaseMaintainer){
    this.databaseMaintainer = databaseMaintainer;
  }

  public DatabaseMaintainer getDatabaseMaintainer() {
    return databaseMaintainer;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public DataSource getReadDataSource() {
    if(readDataSource == null)
      return this.dataSource;
    return this.readDataSource;
  }

  private final ConnectorParameters connectorParams;
  private final DatabaseSettings databaseSettings;
  private final String ecps;
  private Map<String, Object> decodedECPSDatabaseSettings;

  private String applicationName;

  public PSQLConfig(Event event, String schema) {
      ecps = null;
      databaseSettings = new DatabaseSettings(schema);
      connectorParams = new ConnectorParameters(event.getConnectorParams(), null);
  }

  public PSQLConfig(Event event, Context context, TraceItem traceItem) {
    this.connectorParams = event == null ? new ConnectorParameters(null, traceItem) : new ConnectorParameters(event.getConnectorParams(), traceItem);
    this.applicationName = context.getFunctionName();
    this.ecps = connectorParams.getEcps() ;

    /** Stored in env variable */
    String ecpsPhrase = readFromEnvVars(ECPS_PHRASE, context);
    maintenanceServiceEndpoint = readFromEnvVars(MAINTENANCE_ENDPOINT, context);
    databaseSettings = new DatabaseSettings(context);

    /** Fallback: support old env-variable */
    if(databaseSettings.getMaxConnections() != null)
      connectorParams.setDbMaxPoolSize(databaseSettings.getMaxConnections());

    /** If there exists an ECPS String override the databaseSetting with the decoded content */
    if(ecps != null){
      /** Decrypt ECPS String */
      this.decodedECPSDatabaseSettings = decryptECPS(connectorParams.getEcps(), ecpsPhrase);

      for (String key : decodedECPSDatabaseSettings.keySet()){
        switch (key){
          case DatabaseSettings.PSQL_DB: databaseSettings.setDb((String) decodedECPSDatabaseSettings.get(PSQL_DB));
            break;
          case PSQL_HOST: databaseSettings.setHost((String) decodedECPSDatabaseSettings.get(PSQL_HOST));
            break;
          case PSQL_PASSWORD: databaseSettings.setPassword((String) decodedECPSDatabaseSettings.get(PSQL_PASSWORD));
            break;
          case PSQL_PORT: databaseSettings.setPort(Integer.parseInt((String)decodedECPSDatabaseSettings.get(PSQL_PORT)));
            break;
          case PSQL_REPLICA_HOST: databaseSettings.setReplicaHost((String) decodedECPSDatabaseSettings.get(PSQL_REPLICA_HOST));
            break;
          case PSQL_SCHEMA: databaseSettings.setSchema((String) decodedECPSDatabaseSettings.get(PSQL_SCHEMA));
            break;
          case PSQL_USER: databaseSettings.setUser((String) decodedECPSDatabaseSettings.get(PSQL_USER));
            break;
        }
      }
    }
  }

  public String getMaintenanceEndpoint(){ return maintenanceServiceEndpoint;}

  public String getEcps(){ return ecps; }

  public ConnectorParameters getConnectorParams() {
    return connectorParams;
  }

  public DatabaseSettings getDatabaseSettings(){
    return databaseSettings;
  }

  public String getConfigValuesAsString(){
    return ""+
            databaseSettings.getHost()+
            databaseSettings.getReplicaHost()+
            databaseSettings.getDb()+
            databaseSettings.getUser()+
            databaseSettings.getSchema()+
            databaseSettings.getPort()+

            connectorParams.getDbCheckoutTimeout()+
            connectorParams.getDbAcquireIncrement()+
            connectorParams.getDbAcquireRetryAttempts()+
            connectorParams.getDbAcquireRetryAttempts()+
            connectorParams.getDbInitialPoolSize()+
            connectorParams.getDbMinPoolSize()+
            connectorParams.getDbMaxPoolSize()+
            connectorParams.getDbMaxIdleTime()+
            connectorParams.isDbTestConnectionOnCheckout()+
            connectorParams.isEnableHashedSpaceId()+
            connectorParams.getOnDemandIdxLimit()+
            connectorParams.isPropertySearch()+
            connectorParams.isMvtSupport()+
            connectorParams.isAutoIndexing() +
            connectorParams.getStatementTimeoutSeconds();
  }

  public String applicationName() {
    String ecpsSnippet;

    if(connectorParams.getEcps() != null)
      ecpsSnippet = connectorParams.getEcps();
    else
      ecpsSnippet = (databaseSettings.getHost().substring(0,2) + databaseSettings.getUser().substring(0,2) + databaseSettings.getDb().substring(0,2) + (String.valueOf(databaseSettings.getPort()).substring(0,2)));

    ecpsSnippet = ecpsSnippet.length() < 10 ? ecpsSnippet : ecpsSnippet.substring(0,9);

    return DhString.format("%s[%s]", applicationName, ecpsSnippet );
  }

  public String readTableFromEvent(Event event) {
    if (event != null && event.getParams() != null) {
      final String TABLE_NAME = "tableName";
      Object tableName = event.getParams().get(TABLE_NAME);
      if (tableName instanceof String && ((String) tableName).length() > 0)
        return (String) tableName;
    }
    String spaceId = null;
    if (event != null && event.getSpace() != null && event.getSpace().length() > 0)
      spaceId = event.getSpace();
    return getTableNameForSpaceId(spaceId);
  }

  public String getTableNameForSpaceId(String spaceId) {
    if (spaceId != null && spaceId.length() > 0) {
      if (connectorParams.isHrnShortening()) {
        String[] splitHrn = spaceId.split(":");
        if (splitHrn.length > 0)
          return splitHrn[splitHrn.length - 1];
      }
      else if (connectorParams.isEnableHashedSpaceId())
        return Hasher.getHash(spaceId);
      else
        return spaceId;
    }

    return null;
  }

  protected static String readFromEnvVars(String name, Context context) {
    if (context instanceof SimulatedContext) {
      return ((SimulatedContext) context).getEnv(name);
    }
    return System.getenv(name);
  }

  /**
   * Encodes the connector ecps.
   */
  @SuppressWarnings("unused")
  public static String encryptECPS(String connectorParams, String phrase) throws Exception {
    return new PSQLConfig.AESGCMHelper(phrase).encrypt(connectorParams);
  }

  /**
   * Decodes the connector ecps.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> decryptECPS(String ecps, String phrase) {
    try {
      return new ObjectMapper().readValue(AESGCMHelper.getInstance(phrase).decrypt(ecps), Map.class);
    } catch (Exception e) {
      logger.error("Unable to read the encrypted connector parameter settings.");
      throw new RuntimeException(e);
    }
  }

  /**
   * Encrypt and decrypt ECPS Strings by using AesGcm
   */
  public static class AESGCMHelper {
    private static Map<String, AESGCMHelper> helpers = new HashMap<>();
    private AesGcmJce key;

    {
      try {
        AeadConfig.register();
      }catch (Exception e){
        logger.error("Cant register AeadConfig",e);
      }
    }

    /**
     * Returns an instance helper for this passphrase.
     * @param passphrase The passphrase from which to derive a key.
     */
    @SuppressWarnings("WeakerAccess")
    public static AESGCMHelper getInstance(String passphrase) throws GeneralSecurityException {
      if (helpers.get(passphrase) == null) {
        helpers.put(passphrase, new AESGCMHelper(passphrase));
      }
      return helpers.get(passphrase);
    }

    public AESGCMHelper(String passphrase) throws GeneralSecurityException {
      /** If required - adjust passphrase to 128bit length */
      this.key = new AesGcmJce( Arrays.copyOfRange((passphrase == null ? "" : passphrase).getBytes(), 0, 16) );
    }

    /**
     * Decrypts the given string.
     * @param data The Base 64 encoded string representation of the encrypted bytes.
     */
    public String decrypt(String data) throws GeneralSecurityException {
      byte[] decrypted = key.decrypt(Base64.getDecoder().decode(data), null);
      return new String(decrypted);
    }

    /**
     * Encrypts the provided string.
     * @param data The string to encode
     * @return A Base 64 encoded string, which represents the encoded bytes.
     */
    public String encrypt(String data) throws GeneralSecurityException {
      byte[] encrypted = key.encrypt(data.getBytes(), null);
      return new String(Base64.getEncoder().encode(encrypted));
    }
  }
}
