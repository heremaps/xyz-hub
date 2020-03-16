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

package com.here.xyz.connectors.decryptors;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PrivateKeyEventDecryptor extends EventDecryptor {

  /**
   * Logger for the class
   */
  private static final Logger logger = LogManager.getLogger();

  /**
   * Environment variable for the file path to the private key in PKCS#8 PEM format.
   */
  public static final String ENV_PRIVATE_KEY = "PRIVATE_KEY_FILE";

  /**
   * The optional (but recommended) passphrase to decode the private key.
   */
  public static final String ENV_PRIVATE_KEY_PASSPHRASE = "PRIVATE_KEY_PASSPHRASE";

  /**
   * The algorithm used for decrypting the secrets.
   */
  public static final String ALGORITHM = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
  /**
   * The algorithm for creating the private key.
   */
  public static final String RSA = "RSA";

  final private PrivateKey privateKey;

  /**
   * Default constructor to create a new EventDecryptor that takes
   * a private key for decrypting the secrets.
   *
   * For security reasons should the private key be encrypted
   * using PKCS#12 scheme.
   * This line converts an openssl private key to an usable, encrypted
   * PKCS#8 key:
   * openssl pkcs8 -topk8 -in private_key.pem  -v1 PBE-SHA1-3DES -out private_pkcs8.pem
   *
   * Following environment variables need to be set to use this class:
   * - {@link com.here.xyz.connectors.AbstractConnectorHandler#ENV_DECRYPTOR}
   * - {@link #ENV_PRIVATE_KEY}
   * - {@link #ENV_PRIVATE_KEY_PASSPHRASE}
   */
  PrivateKeyEventDecryptor() {
    PrivateKey tmp;
    try {
      tmp = loadPrivateKey(System.getenv(ENV_PRIVATE_KEY), System.getenv(ENV_PRIVATE_KEY_PASSPHRASE));
    } catch (Exception e) {
      tmp = null;
      logger.error("Could not load the private key. Params will not be decrypted!", e);
    }
    privateKey = tmp;
  }

  @Override
  public String decryptAsymmetric(final String encoded) {
    if (privateKey == null) {
      return encoded;
    }
    try {
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      cipher.init(Cipher.DECRYPT_MODE, privateKey);

      byte[] decryptedBytes = cipher.doFinal(Base64.getDecoder().decode(encoded));
      return new String(decryptedBytes, UTF_8);
    } catch(Exception e) {
      logger.warn("Could not decrypt string.", e);
      return encoded;
    }
  }

  /**
   * Load the private key.
   *
   * @param filePath The file path to the private key in PKCS#8 PEM format
   * @param passphrase The optional passphrase to decode the private key (recommended).
   * @return Returns the {@link PrivateKey} or null if there a problem.
   */
  public static PrivateKey loadPrivateKey(final String filePath, final String passphrase) {
    if (filePath == null) {
      return null;
    }
    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(filePath));
    } catch (IOException e) {
      logger.error("Could not load private key from file path '" + filePath + "'", e);
      return null;
    }
    String[] lines = reader.lines().toArray(String[]::new);
    if ("-----BEGIN PRIVATE KEY-----".equals(lines[0])) {
      // not encrypted
      String privateKeyPEM = Arrays.stream(lines)
          .filter(s -> !"-----BEGIN PRIVATE KEY-----".equals(s) && !"-----END PRIVATE KEY-----".equals(s))
          .collect(Collectors.joining(""));
      return createPrivateKey(privateKeyPEM);
    } else if("-----BEGIN ENCRYPTED PRIVATE KEY-----".equals(lines[0])) {
      // encrypted
      String privateKeyPEM = Arrays.stream(lines)
          .filter(s -> !"-----BEGIN ENCRYPTED PRIVATE KEY-----".equals(s) && !"-----END ENCRYPTED PRIVATE KEY-----".equals(s))
          .collect(Collectors.joining(""));
      return decryptPrivateKey(privateKeyPEM, passphrase);
    } else {
      logger.error("Unknown key format. Params will not be decrypted.");
      return null;
    }
  }

  /**
   * This method decrypts the private key that was encrypted using PKCS#12 scheme.
   *
   * @param pkcs8Data The private key in PEM format without header and footer.
   * @param passphrase The passphrase for decrypting the private key.
   * @return Returns the {@link PrivateKey} or null if there a problem.
   */
  public static PrivateKey decryptPrivateKey(final String pkcs8Data, final String passphrase) {
    if (passphrase == null || pkcs8Data == null) {
      logger.error("Could not create private key because passphrase or key is null");
      return null;
    }
    try {
      PBEKeySpec pbeSpec = new PBEKeySpec(passphrase.toCharArray());
      EncryptedPrivateKeyInfo pkinfo = new EncryptedPrivateKeyInfo(Base64.getDecoder().decode(pkcs8Data.getBytes(UTF_8)));
      SecretKeyFactory skf = SecretKeyFactory.getInstance(pkinfo.getAlgName());
      Key secret = skf.generateSecret(pbeSpec);
      PKCS8EncodedKeySpec keySpec = pkinfo.getKeySpec(secret);
      KeyFactory keyFactory = KeyFactory.getInstance(RSA);
      return keyFactory.generatePrivate(keySpec);
    } catch (Exception e) {
      logger.error("Could not create encrypted private key from environment variable", e);
      return null;
    }
  }

  /**
   * Try to create a RSA private key from a PKCS#8 PEM without header and footer.
   *
   * @param pkcs8Data The private key in PKCS#8 PEM format without header and footer.
   * @return Returns the {@link PrivateKey} or null if there was a problem.
   */
  public static PrivateKey createPrivateKey(final String pkcs8Data) {
    try {
      KeyFactory keyFactory = KeyFactory.getInstance(RSA);
      return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pkcs8Data.getBytes(UTF_8))));
    } catch (Exception e) {
      logger.error("Could not create unencrypted private key from environment variable", e);
      return null;
    }
  }
}
