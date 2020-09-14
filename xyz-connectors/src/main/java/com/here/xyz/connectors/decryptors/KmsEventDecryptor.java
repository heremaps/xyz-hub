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

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptionAlgorithmSpec;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Helper class to en-/decryptPrivateKey secrets in Events, Token Metadata, and Space configurations.
 */
public class KmsEventDecryptor extends EventDecryptor {
  /**
   * Logger for the class
   */
  private static final Logger logger = LogManager.getLogger();

  /**
   * Algorithm used for asymmetric (private/public key) encryption.
   */
  private static final EncryptionAlgorithmSpec ASYMMETRIC_ALGORITHM = EncryptionAlgorithmSpec.RSAES_OAEP_SHA_256;

  /**
   * Environment variable for the KMS key ARN for decrypting space secrets.
   */
  public static String ENV_KMS_KEY_ARN = "KMS_KEY_ARN";

  /**
   * ARN of the KMS key to use.
   */
  public final String kmsKeyArn;

  /**
   * Regexp to check if the KMS Key ARN is valid.
   */
  public final static Pattern kmsKeyArnPattern = Pattern.compile("arn:aws:kms:\\w+-\\w+-\\d+:\\d{12}:key/\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}");

  /**
   * AWS KMS client for getting the private key to decryptPrivateKey space secrets.
   */
  private final AWSKMS kmsClient;

  /**
   * Default constructor to create a new EventDecryptor that uses
   * AWS KMS to decryptPrivateKey secrets.
   *
   * Following environment variables need to be set to use this class:
   * - {@link com.here.xyz.connectors.AbstractConnectorHandler#ENV_DECRYPTOR}
   * - {@link #ENV_KMS_KEY_ARN}
   */
  KmsEventDecryptor() {
    kmsClient = AWSKMSClientBuilder.defaultClient();
    // validate KMS Key ARN
    String keyArn = System.getenv(ENV_KMS_KEY_ARN);
    if (keyArn != null && kmsKeyArnPattern.matcher(keyArn).matches()) {
      kmsKeyArn = keyArn;
    } else {
      logger.error("KMS Key ARN has wrong format. Automatic de- and encryption disabled!");
      kmsKeyArn = null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String encryptAsymmetric(final String secret) {
    if (isEncrypted(secret)) {
      return secret;
    }

    if (secret.length() > 230) {
      return secret;
    }

    // we cannot decrypt the secret if no KMS key ARN is set.
    if (kmsKeyArn == null) {
      return secret;
    }

    String tmp = secret;
    if (isToBeEncrypted(secret)) {
      tmp = secret.substring(2, secret.length() - 2);
    }

    try {
      EncryptRequest req = new EncryptRequest()
          .withKeyId(kmsKeyArn)
          .withEncryptionAlgorithm(ASYMMETRIC_ALGORITHM)
          .withPlaintext(ByteBuffer.wrap(tmp.getBytes()));
      ByteBuffer plainText = kmsClient.encrypt(req).getCiphertextBlob();
      return TO_DECRYPT_PREFIX + Base64.getEncoder().encodeToString(plainText.array()) + TO_DECRYPT_POSTFIX;
    } catch (RuntimeException e) {
      logger.error("Error when trying to encrypt using asymmetric key. Please check the following:\n"
          + "\t- Does the application use an IAM role?\n"
          + "\t- Does the application's role have the permission to use the CMK the value was encrypted with?\n"
          + "More information on that topic: https://confluence.in.here.com/display/CMECMCPDOWS/Encryption+of+secrets", e);
    }

    return secret;
  }

  /**
   * {@inheritDoc}
   */
  public String decryptAsymmetric(String encrypted) {
    if (!isEncrypted(encrypted)) {
      return encrypted;
    }

    // we cannot decrypt the secret if no KMS key ARN is set.
    if (kmsKeyArn == null) {
      return encrypted;
    }

    String tmp = encrypted.substring(2, encrypted.length() - 2);

    try {
      ByteBuffer cipherText = ByteBuffer.wrap(Base64.getDecoder().decode(tmp.getBytes()));
      DecryptRequest req = new DecryptRequest()
          .withKeyId(kmsKeyArn)
          .withEncryptionAlgorithm(ASYMMETRIC_ALGORITHM)
          .withCiphertextBlob(cipherText);
      ByteBuffer plainText = kmsClient.decrypt(req).getPlaintext();
      return new String(plainText.array());
    } catch (IllegalArgumentException e) {
      logger.warn("Could not Base64 decode value", e);
    } catch (RuntimeException e) {
      logger.error("Error when trying to decrypt with asymmetric key. Please check the following:\n"
          + "\t- Does the application use an IAM role?\n"
          + "\t- Does the application's role have the permission to use the CMK the value was encrypted with?\n"
          + "More information on that topic: https://confluence.in.here.com/display/CMECMCPDOWS/Encryption+of+secrets", e);
    }

    return encrypted;
  }
}
