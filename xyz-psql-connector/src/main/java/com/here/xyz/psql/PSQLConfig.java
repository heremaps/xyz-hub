/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.subtle.AesGcmJce;
import com.here.xyz.connectors.SimulatedContext;
import com.here.xyz.events.Event;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class PSQLConfig {

  private static final Logger logger = LogManager.getLogger();

  /**
   * A constant that is normally used as environment variable name for the host.
   */
  protected static final String PSQL_HOST = "PSQL_HOST";

  /**
   * A constant that is normally used as environment variable name for the port.
   */
  protected static final String PSQL_PORT = "PSQL_PORT";

  /**
   * A constant that is normally used as environment variable name for the host.
   */
  private static final String PSQL_REPLICA_HOST = "PSQL_REPLICA_HOST";

  /**
   * A constant that is normally used as environment variable name for the database.
   */
  private static final String PSQL_DB = "PSQL_DB";

  /**
   * A constant that is normally used as environment variable name for the user.
   */
  protected static final String PSQL_USER = "PSQL_USER";

  /**
   * A constant that is normally used as environment variable name for the password.
   */
  protected static final String PSQL_PASSWORD = "PSQL_PASSWORD";

  /**
   * A constant that is normally used as environment variable name for the schema.
   */
  private static final String PSQL_SCHEMA = "PSQL_SCHEMA";

  protected static final String ECPS_PHRASE = "ECPS_PHRASE";

  protected final static String DEFAULT_ECPS = "default";

  /**
   * The maximal amount of concurrent connections, default is one, normally only increased for embedded lambda.
   */
  private final static String PSQL_MAX_CONN = "PSQL_MAX_CONN";

  /**
   * The encrypted connector parameters.
   */
  private final Map<String, Object> connectorParams;

  private Context context;
  private boolean propertySearch;
  private boolean autoIndexing;

  private Map<String, Object> readECPS(String ecps) {
    if (DEFAULT_ECPS.equals(ecps)) {
      return null;
    }

    return decryptECPS(ecps, readEnv(ECPS_PHRASE));
  }

  /**
   * Decodes the connector parameters.
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

  /**
   * A method to decode connector parameters.
   */
  @SuppressWarnings("unused")
  protected static String encryptCPS(String connectorParams, String phrase) throws Exception {
    return new AESGCMHelper(phrase).encrypt(connectorParams);
  }

  protected static String getECPS(Event event) {
    if (event == null || event.getConnectorParams() == null || event.getConnectorParams().get("ecps") == null) {
      return DEFAULT_ECPS;
    }
    return (String) event.getConnectorParams().get("ecps");
  }


  /**
   * Returns the maximal amount of concurrent connections to be used.
   *
   * @return the maximal amount of concurrent connections to be used.
   */
  protected int maxPostgreSQLConnections() {
    try {
      return Integer.parseInt(readEnv(PSQL_MAX_CONN), 10);
    } catch (Exception e) {
      return 1;
    }
  }

  /**
   * Returns the host of the PostgreSQL service.
   *
   * @return the host of the PostgreSQL service.
   */
  protected String host() {
    final String host = readEnv(PSQL_HOST);
    if (host != null) {
      return host;
    }
    return "localhost";
  }

  /**
   * Returns the host of the PostgreSQL service.
   *
   * @return the host of the PostgreSQL service.
   */
  protected String replica() {
    return readEnv(PSQL_REPLICA_HOST);
  }

  /**
   * Returns the port of the PostgreSQL service.
   *
   * @return the port of the PostgreSQL service.
   */
  protected int port() {
    final String portText = readEnv(PSQL_PORT);
    if (portText != null) {
      try {
        int port = Integer.parseInt(portText, 10);
        if (port > 0 && port < 65536) {
          return port;
        }
      } catch (NumberFormatException ignored) {
      }
    }
    return 5432;
  }


  /**
   * Returns the database to connect to.
   *
   * @return the database to connect to.
   */
  protected String database() {
    final String db = readEnv(PSQL_DB);
    if (db != null) {
      return db;
    }
    return "postgres";
  }

  /**
   * Returns the user to connect with.
   *
   * @return the user to connect with.
   */
  protected String user() {
    final String user = readEnv(PSQL_USER);
    if (user != null) {
      return user;
    }
    return "postgres";
  }

  /**
   * Returns the password to connect with.
   *
   * @return the password to connect with.
   */
  protected String password() {
    final String password = readEnv(PSQL_PASSWORD);
    if (password != null) {
      return password;
    }
    return "postgres";
  }


  static String INCLUDE_OLD_STATES = "includeOldStates"; // read from event params

  private String applicationName;

  public PSQLConfig(Event event, Context context) {
    this.context = context;
    this.connectorParams = readECPS(getECPS(event));
    this.applicationName = context.getFunctionName();

    if(event.getConnectorParams() != null){
      if(event.getConnectorParams().get("autoIndexing") == Boolean.TRUE)
        this.autoIndexing = true;
      if(event.getConnectorParams().get("propertySearch") == Boolean.TRUE)
        this.propertySearch = true;
    }
  }

  private String readEnv(String name) {
    //The ecps phrase, is in the context.
    if (ECPS_PHRASE.equals(name)) {
      return readEnvFromContext(name);
    }

    if (connectorParams != null) {
      return (String) connectorParams.get(name);
    }

    if (context instanceof SimulatedContext) {
      return ((SimulatedContext) context).getEnv(name);
    }
    return System.getenv(name);
  }

  private String readEnvFromContext(String name) {
    if (context instanceof SimulatedContext) {
      return ((SimulatedContext) context).getEnv(name);
    }
    return System.getenv(name);
  }


  protected boolean isReadOnly() {
    String READ_ONLY = "READ_ONLY";
    return "true".equals(readEnv(READ_ONLY));
  }

  protected String schema() {
    final String schema = readEnv(PSQL_SCHEMA);
    if (schema != null) {
      return schema;
    }

    return "public";
  }

  protected String table(Event event) {
    if (event != null && event.getSpace() != null && event.getSpace().length() > 0) {
      return event.getSpace();
    }

    return null;
  }

  protected String applicationName() {
    return applicationName;
  }

  protected boolean isPropertySearchActivated(){
    return propertySearch;
  }

  protected boolean isAutoIndexingActivated(){
    return autoIndexing;
  }

  protected Integer onDemandLimit(){
    if(connectorParams != null && connectorParams.get("onDemandIdxLimit") != null)
      return  (Integer) connectorParams.get("onDemandIdxLimit");
    return null;
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
    protected static AESGCMHelper getInstance(String passphrase) throws GeneralSecurityException {
      if (helpers.get(passphrase) == null) {
        helpers.put(passphrase, new AESGCMHelper(passphrase));
      }
      return helpers.get(passphrase);
    }

    protected AESGCMHelper(String passphrase) throws GeneralSecurityException {
      /** If required - adjust passphrase to 128bit length */
      this.key = new AesGcmJce( Arrays.copyOfRange((passphrase == null ? "" : passphrase).getBytes(), 0, 16) );
    }

    /**
     * Decrypts the given string.
     * @param data The Base 64 encoded string representation of the encrypted bytes.
     */
    protected String decrypt(String data) throws GeneralSecurityException {
      byte[] decrypted = key.decrypt(Base64.getDecoder().decode(data), null);
      return new String(decrypted);
    }

    /**
     * Encrypts the provided string.
     * @param data The string to encode
     * @return A Base 64 encoded string, which represents the encoded bytes.
     */
    protected String encrypt(String data) throws UnsupportedEncodingException, GeneralSecurityException {
      byte[] encrypted = key.encrypt(data.getBytes(), null);
      return new String(Base64.getEncoder().encode(encrypted));
    }
  }
}
