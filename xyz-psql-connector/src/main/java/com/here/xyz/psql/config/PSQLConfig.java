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

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.subtle.AesGcmJce;
import com.here.xyz.connectors.AbstractConnectorHandler.TraceItem;
import com.here.xyz.events.Event;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.util.Hasher;
import java.io.UnsupportedEncodingException;
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

    private DataSource dataSource;
  private DataSource readDataSource;
  private DatabaseMaintainer databaseMaintainer;

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
  private Map<String, Object> decodedECPSDatabaseSettings;

  private final Context context;
  private String applicationName;

  public PSQLConfig(Event event, Context context, TraceItem traceItem) {
    this.context = context;
    this.connectorParams = event == null ? new ConnectorParameters(null, traceItem) : new ConnectorParameters(event.getConnectorParams(), traceItem);
    this.applicationName = context.getFunctionName();

    /** Stored in env variable */
    String ecpsPhrase = DatabaseSettings.readFromEnvVars(ECPS_PHRASE, context);
    databaseSettings = new DatabaseSettings(context);

    /** Fallback: support old env-variable */
    if(databaseSettings.getMaxConnections() != null)
      connectorParams.setDbMaxPoolSize(databaseSettings.getMaxConnections());

    /** If there exists an ECPS String override the databaseSetting with the decoded content */
    if(connectorParams.getEcps() != null){
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
            connectorParams.isCompactHistory()+
            connectorParams.isPropertySearch()+
            connectorParams.isAutoIndexing();
  }

  public String applicationName() {
    String ecpsSnippet;

    if(connectorParams.getEcps() != null)
      ecpsSnippet = connectorParams.getEcps();
    else
      ecpsSnippet = (databaseSettings.getHost().substring(0,2) + databaseSettings.getUser().substring(0,2) + databaseSettings.getDb().substring(0,2) + (String.valueOf(databaseSettings.getPort()).substring(0,2)));

    ecpsSnippet = ecpsSnippet.length() < 10 ? ecpsSnippet : ecpsSnippet.substring(0,9);

    return String.format("%s[%s]", applicationName, ecpsSnippet );
  }

  public String readTableFromEvent(Event event) {
    if (event != null && event.getSpace() != null && event.getSpace().length() > 0) {
      if (connectorParams.isEnableHashedSpaceId()) {
        return Hasher.getHash(event.getSpace());
      }
      else if (connectorParams.isHrnShortening()) {
        String[] splitHrn = event.getSpace().split(":");
        if (splitHrn.length > 0)
          return splitHrn[splitHrn.length - 1];
      }
      else {
        return event.getSpace();
      }
    }

    return null;
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
  private static Map<String, Object> decryptECPS(String ecps, String phrase) {
    try {
      return new ObjectMapper().readValue(AESGCMHelper.getInstance(phrase).decrypt(ecps), Map.class);
    } catch (Exception e) {
      logger.error("Unable to read the encrypted connector parameter settings.");
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  public static String encrypt(String plaintext, String phrase) throws Exception { return encryptECPS( plaintext, phrase ); }  

  @SuppressWarnings("unused")
  public static String decrypt(String encryptedtext, String phrase) throws Exception { return AESGCMHelper.getInstance(phrase).decrypt(encryptedtext); }

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
    protected static AESGCMHelper getInstance(String passphrase) throws GeneralSecurityException {
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
    public String encrypt(String data) throws UnsupportedEncodingException, GeneralSecurityException {
      byte[] encrypted = key.encrypt(data.getBytes(), null);
      return new String(Base64.getEncoder().encode(encrypted));
    }
  }
}
