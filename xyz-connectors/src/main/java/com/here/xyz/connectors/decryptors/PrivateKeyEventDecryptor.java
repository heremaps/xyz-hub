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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.MGF1ParameterSpec;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Iterator;
import java.util.stream.Stream;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PSource.PSpecified;
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
   * Environment variable for the file path to the public key in X509 PEM format.
   */
  public static final String ENV_PUBLIC_KEY = "PUBLIC_KEY_FILE";
  /**
   * The optional (but recommended) passphrase to decode the private key.
   */
  public static final String ENV_PRIVATE_KEY_PASSPHRASE = "PRIVATE_KEY_PASSPHRASE";
  /**
   * The algorithm used for decrypting the secrets.
   */
  public static final String ALGORITHM = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
  /**
   * The algorithm for creating the public and private key.
   */
  public static final String RSA = "RSA";
  /**
   * Prefix for loading files from classpath.
   */
  private static final String CLASSPATH_PREFIX = "classpath:";
  /**
   * OAEP Padding specs for decrypting secrets.
   */
  private static final OAEPParameterSpec OAEP_PARAMS
      = new OAEPParameterSpec("SHA-256", "MGF1", new MGF1ParameterSpec("SHA-256"), PSpecified.DEFAULT);
  /**
   * The private key for decrypting secrets.
   */
  final private PrivateKey privateKey;
  /**
   * The public key for encrypting secrets.
   */
  final private PublicKey publicKey;

  /**
   * Default constructor to create a new EventDecryptor that takes
   * a private key for decrypting the secrets and an optional
   * public key for encrypting.
   *
   * For security reasons should the private key be encrypted
   * using PKCS#8 scheme.
   * This line converts an openssl private key to an usable, encrypted
   * PKCS#8 key:
   * openssl pkcs8 -topk8 -in private_key.pem  -v1 PBE-SHA1-3DES -out private_pkcs8.pem
   *
   * This is how you can encrypt a secret using openssl and the public key:
   * openssl pkeyutl -encrypt \
   *                 -inkey public_key.pem \
   *                 -pubin \
   *                 -in secret.txt \
   *                 -pkeyopt rsa_padding_mode:oaep \
   *                 -pkeyopt rsa_oaep_md:sha256 \
   *                 -pkeyopt rsa_mgf1_md:sha256
   *
   * Following environment variables need to be set to use this class:
   * - {@link com.here.xyz.connectors.AbstractConnectorHandler#ENV_DECRYPTOR}
   * - {@link #ENV_PRIVATE_KEY}
   * - {@link #ENV_PRIVATE_KEY_PASSPHRASE}
   */
  PrivateKeyEventDecryptor() {
    PrivateKey tmpPrivateKey;
    PublicKey tmpPublicKey;
    try {
      tmpPrivateKey = loadPrivateKey(System.getenv(ENV_PRIVATE_KEY), System.getenv(ENV_PRIVATE_KEY_PASSPHRASE));
    } catch (Exception e) {
      tmpPrivateKey = null;
      logger.error("Could not load the private key. Params will not be decrypted!", e);
    }
    try {
      // TODO validate input
      tmpPublicKey = loadPublicKey(System.getenv(ENV_PUBLIC_KEY));
    } catch (Exception e) {
      tmpPublicKey = null;
      logger.error("Could not load the public key. Params will not be encrypted!", e);
    }
    privateKey = tmpPrivateKey;
    publicKey = tmpPublicKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String encryptAsymmetric(final String secret) {
    // we cannot encrypt the secret if no public key is set.
    if (publicKey == null) {
      return secret;
    }

    String tmp = secret;
    if (isToBeEncrypted(secret)) {
      tmp = secret.substring(2, secret.length() - 2);
    }

    try {
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      cipher.init(Cipher.ENCRYPT_MODE, publicKey, OAEP_PARAMS);
      byte[] encryptedBytes = cipher.doFinal(tmp.getBytes());
      return TO_DECRYPT_PREFIX + Base64.getEncoder().encodeToString(encryptedBytes) + TO_DECRYPT_POSTFIX;
    } catch(Exception e) {
      logger.warn("Could not encrypt string.", e);
    }

    return secret;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String decryptAsymmetric(final String encrypted) {
    // we cannot decode the secret if no private key is set.
    if (privateKey == null) {
      return encrypted;
    }

    if (!isEncrypted(encrypted)) {
      return encrypted;
    }

    String tmp = encrypted.substring(2, encrypted.length() - 2);

    try {
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      cipher.init(Cipher.DECRYPT_MODE, privateKey, OAEP_PARAMS);

      byte[] decryptedBytes = cipher.doFinal(Base64.getDecoder().decode(tmp));
      return new String(decryptedBytes, UTF_8);
    } catch (IllegalArgumentException e) {
      logger.warn("Could not Base64 decode value", e);
    } catch(Exception e) {
      logger.warn("Could not decrypt string.", e);
    }

    return encrypted;
  }

  /**
   * Load the private key.
   *
   * @param filePath The file path to the private key in PKCS#8 PEM format
   * @param passphrase The optional passphrase to decode the private key (recommended).
   * @return Returns the {@link PrivateKey} or null if there a problem.
   */
  public static PrivateKey loadPrivateKey(final String filePath, final String passphrase) {
    if (filePath == null || filePath.isEmpty()) {
      return null;
    }

    Stream<String> lines;
    if (filePath.startsWith(CLASSPATH_PREFIX)) {
      InputStream is = PrivateKeyEventDecryptor.class.getResourceAsStream(filePath.substring(CLASSPATH_PREFIX.length()));
      if (is == null) {
        logger.error("Could not load private key from path '" + filePath + "'");
        return null;
      }
      lines = new LineNumberReader(new InputStreamReader(is)).lines();
    } else {
      logger.error("Loading key from filesystem is forbidden.");
      return null;
    }

    final StringBuilder privateKeyPEM = new StringBuilder();
    final Iterator<String> i = lines.iterator();
    if (i.hasNext()) {
      String header = i.next();
      if ("-----BEGIN PRIVATE KEY-----".equals(header)) {
        i.forEachRemaining(s -> {
          if (!"-----END PRIVATE KEY-----".equals(s)) {
            privateKeyPEM.append(s);
          }
        });
        return createPrivateKey(privateKeyPEM.toString());
      } else if("-----BEGIN ENCRYPTED PRIVATE KEY-----".equals(header)) {
        i.forEachRemaining(s -> {
          if (!"-----END ENCRYPTED PRIVATE KEY-----".equals(s)) {
            privateKeyPEM.append(s);
          }
        });
        return decryptPrivateKey(privateKeyPEM.toString(), passphrase);
      } else{
        logger.error("Unknown key format.");
        return null;
      }
    }
    logger.error("Empty key file.");
    return null;
  }

  /**
   * Load the public key.
   *
   * @param filePath The file path to the public key in X509 PEM format
   * @return Returns the {@link PublicKey} or null if there a problem.
   */
  public static PublicKey loadPublicKey(final String filePath) {
    if (filePath == null || filePath.isEmpty()) {
      return null;
    }

    Stream<String> lines;
    if (filePath.startsWith(CLASSPATH_PREFIX)) {
      InputStream is = PrivateKeyEventDecryptor.class.getResourceAsStream(filePath.substring(CLASSPATH_PREFIX.length()));
      if (is == null) {
        logger.error("Could not load public key from path '" + filePath + "'");
        return null;
      }
      lines = new LineNumberReader(new InputStreamReader(is)).lines();
    } else {
      logger.error("Loading key from filesystem is forbidden.");
      return null;
    }

    final StringBuilder publicKeyPEM = new StringBuilder();
    final Iterator<String> i = lines.iterator();
    if (i.hasNext()) {
      String header = i.next();
      if ("-----BEGIN PUBLIC KEY-----".equals(header)) {
        i.forEachRemaining(s -> {
          if (!"-----END PUBLIC KEY-----".equals(s)) {
            publicKeyPEM.append(s);
          }
        });
        return createPublicKey(publicKeyPEM.toString());
      } else{
        logger.error("Unknown key format.");
        return null;
      }
    }
    logger.error("Empty key file.");
    return null;
  }

  /**
   * This method decrypts the private key that was encrypted using PKCS#8 scheme.
   *
   * @param pkcs8Data The private key in PEM format without header and footer.
   * @param passphrase The passphrase for decrypting the private key.
   * @return Returns the {@link PrivateKey} or null if there a problem.
   */
  public static PrivateKey decryptPrivateKey(final String pkcs8Data, final String passphrase) {
    if (passphrase == null || pkcs8Data == null) {
      logger.error("Could not create private key because passphrase or key is null.");
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
      logger.error("Could not create encrypted private key from environment variable.", e);
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
    } catch (IllegalArgumentException | NoSuchAlgorithmException | InvalidKeySpecException e) {
      logger.error("Could not create unencrypted private key from environment variable.", e);
      return null;
    }
  }

  /**
   * Try to create a RSA public key from X509 PEM without header and footer.
   *
   * @param data The public key in X509 PEM format without header and footer.
   * @return Returns the {@link PublicKey} or null if there was a problem.
   */
  public static PublicKey createPublicKey(final String data) {
    try {
      KeyFactory keyFactory = KeyFactory.getInstance(RSA);
      return keyFactory.generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(data.getBytes(UTF_8))));
    } catch (IllegalArgumentException | NoSuchAlgorithmException | InvalidKeySpecException e) {
      logger.error("Could not create public key from environment variable.", e);
      return null;
    }
  }
}
