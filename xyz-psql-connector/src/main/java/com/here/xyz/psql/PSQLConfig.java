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
import com.google.common.hash.Hashing;
import com.here.xyz.connectors.SimulatedContext;
import com.here.xyz.events.Event;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.SecretKeySpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class PSQLConfig {

  private static final Logger logger = LogManager.getLogger();

  /**
   * A constant that is normally used as environment variable name for the host.
   */
  static final String PSQL_HOST = "PSQL_HOST";

  /**
   * A constant that is normally used as environment variable name for the port.
   */
  static final String PSQL_PORT = "PSQL_PORT";

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
  static final String PSQL_USER = "PSQL_USER";

  /**
   * A constant that is normally used as environment variable name for the password.
   */
  static final String PSQL_PASSWORD = "PSQL_PASSWORD";

  /**
   * A constant that is normally used as environment variable name for the schema.
   */
  private static final String PSQL_SCHEMA = "PSQL_SCHEMA";

  static final String ECPS_PHRASE = "ECPS_PHRASE";

  private final static String DEFAULT_ECPS = "default";

  /**
   * The maximal amount of concurrent connections, default is one, normally only increased for embedded lambda.
   */
  private final static String PSQL_MAX_CONN = "PSQL_MAX_CONN";

  /**
   * The encrypted connector parameters.
   */
  private final Map<String, Object> connectorParams;

  private Context context;

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
  static Map<String, Object> decryptECPS(String ecps, String phrase) {
    try {
      return new ObjectMapper().readValue(AESHelper.getInstance(phrase).decrypt(ecps), Map.class);
    } catch (Exception e) {
      logger.error("Unable to read the encrypted connector parameter settings.");
      throw new RuntimeException(e);
    }
  }

  /**
   * A method to decode connector parameters.
   */
  @SuppressWarnings("unused")
  static String encryptCPS(String connectorParams, String phrase) throws Exception {
    return new AESHelper(phrase).encrypt(connectorParams);
  }

  static String getECPS(Event event) {
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
  int maxPostgreSQLConnections() {
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
  String host() {
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
  String replica() {
    return readEnv(PSQL_REPLICA_HOST);
  }

  /**
   * Returns the port of the PostgreSQL service.
   *
   * @return the port of the PostgreSQL service.
   */
  int port() {
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
  String database() {
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
  String user() {
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
  String password() {
    final String password = readEnv(PSQL_PASSWORD);
    if (password != null) {
      return password;
    }
    return "postgres";
  }


  static String INCLUDE_OLD_STATES = "includeOldStates"; // read from event params

  private String applicationName;

  PSQLConfig(Event event, Context context) {
    this.context = context;
    this.connectorParams = readECPS(getECPS(event));
    this.applicationName = context.getFunctionName();
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


  boolean isReadOnly() {
    String READ_ONLY = "READ_ONLY";
    return "true".equals(readEnv(READ_ONLY));
  }

  String schema() {
    final String schema = readEnv(PSQL_SCHEMA);
    if (schema != null) {
      return schema;
    }

    return "public";
  }

  String table(Event event) {
    if (event != null && event.getSpace() != null && event.getSpace().length() > 0) {
      return event.getSpace();
    }

    return null;
  }

  String applicationName() {
    return applicationName;
  }

  public static class AESHelper {

    private static Map<String, AESHelper> helpers = new HashMap<>();
    public byte[] key;

    /**
     * Returns an instance helper for this passphrase.
     *
     * @param passphrase The passphrase from which to derive a key.
     */
    @SuppressWarnings("WeakerAccess")
    public static AESHelper getInstance(String passphrase) {
      if (helpers.get(passphrase) == null) {
        helpers.put(passphrase, new AESHelper(passphrase));
      }
      return helpers.get(passphrase);
    }


    public AESHelper(String passphrase) {
      //noinspection UnstableApiUsage
      this.key = Arrays.copyOf(Hashing.sha256().newHasher().putBytes(passphrase.getBytes()).hash().asBytes(), 16);
    }

    /**
     * Decrypts the given string.
     *
     * @param data The Base 64 encoded string representation of the encrypted bytes.
     */
    String decrypt(String data) throws IllegalBlockSizeException, BadPaddingException {
      return new String(decrypt(Base64.getDecoder().decode(data)), StandardCharsets.UTF_8);
    }

    /**
     * Decrypts the given bytes.
     */
    byte[] decrypt(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
      return decryptCipher.get().doFinal(data);
    }

    /**
     * Encrypts the provided string.
     *
     * @param data The string to encode
     * @return A Base 64 encoded string, which represents the encoded bytes.
     */
    String encrypt(String data) throws IllegalBlockSizeException, BadPaddingException {
      return new String(Base64.getEncoder().encode(encrypt(data.getBytes())));
    }

    /**
     * Encrypts the provided byte array.
     */
    byte[] encrypt(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
      final Cipher ec = encryptCipher.get();
      return ec.doFinal(data);
    }

    private final ThreadLocal<Cipher> decryptCipher = ThreadLocal.withInitial(() -> {
      try {
        Cipher dc = Cipher.getInstance("AES/ECB/PKCS5Padding");
        dc.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"));
        return dc;
      } catch (Exception e) {
        logger.error("Exception when initializing the decrypt cypher.", e);
        return null;
      }
    });

    private final ThreadLocal<Cipher> encryptCipher = ThreadLocal.withInitial(() -> {
      try {
        Cipher ec = Cipher.getInstance("AES/ECB/PKCS5Padding");
        ec.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"));
        return ec;
      } catch (Exception e) {
        logger.error("Exception when initializing the decrypt cypher.", e);
        return null;
      }
    });
  }
}
