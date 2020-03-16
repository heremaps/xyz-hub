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
import com.amazonaws.services.kms.model.EncryptionAlgorithmSpec;
import java.nio.ByteBuffer;
import java.util.Base64;
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

  public final String kmsKeyArn;
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
    kmsKeyArn = System.getenv(ENV_KMS_KEY_ARN);
  }

  /**
   * {@inheritDoc}
   */
  public String decryptAsymmetric(String encoded) {
    if (encoded == null || encoded.equals("")) {
      return encoded;
    }

    // we cannot decode the secret if no KMS key ARN is set.
    if (kmsKeyArn == null) {
      return encoded;
    }

    ByteBuffer cipherText = ByteBuffer.wrap(Base64.getDecoder().decode(encoded.getBytes()));
    DecryptRequest req = new DecryptRequest()
        .withKeyId(kmsKeyArn)
        .withEncryptionAlgorithm(ASYMMETRIC_ALGORITHM)
        .withCiphertextBlob(cipherText);
    try {
      ByteBuffer plainText = kmsClient.decrypt(req).getPlaintext();
      return new String(plainText.array());
    } catch (RuntimeException e) {
      logger.error("Error when trying to decryptPrivateKey asymmetric key. Please check the following:\n"
          + "\t- Does the application use an IAM role?\n"
          + "\t- Does the application's role have the permission to use the CMK the value was encrypted with?\n"
          + "More information on that topic: https://confluence.in.here.com/display/CMECMCPDOWS/Encryption+of+secrets", e);
    }

    return encoded;
  }
}
