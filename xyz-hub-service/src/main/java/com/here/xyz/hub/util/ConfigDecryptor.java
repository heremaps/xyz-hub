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

package com.here.xyz.hub.util;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import io.vertx.core.json.JsonObject;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class ConfigDecryptor {

  private static AWSKMS kmsClient;
  private static final String encPrefix = "{{";
  private static final String encPostfix = "}}";
  private static final String SYMMETRIC_ALGORITHM = "AES/CTR/NoPadding";

  public static String decryptSecret(String secretValue) throws CryptoException {
    if (secretValue == null || secretValue == "") {
      return null;
    }

    //Accept both: wrapped values and un-wrapped value
    if (secretValue.startsWith(encPrefix)) {
      secretValue = secretValue.substring(2);
    }
    if (secretValue.endsWith(encPostfix)) {
      secretValue = secretValue.substring(0, secretValue.length() - 2);
    }

    return decryptValue(secretValue);
  }

  public static boolean isEncrypted(String str) {
    if (str == null || str.length() == 0) {
      return false;
    }
    return str.startsWith(encPrefix) && str.endsWith(encPostfix);
  }

  public static String decryptValue(String toDecrypt) throws CryptoException {
    try {
      JsonObject encrypted = new JsonObject(new String(Base64.getDecoder().decode(toDecrypt)));
      String encryptedKey = encrypted.getString("key");
      String encryptedData = encrypted.getString("data");
      String plainKey = decryptSymmetricKey(encryptedKey);

      return decryptSymmetric(plainKey, encryptedData);
    } catch (RuntimeException e) {
      throw new CryptoException("Error when reading value to decrypt. Is the value really an encrypted value?", e);
    }
  }

  private static String decryptSymmetricKey(String encryptedKey) throws CryptoException {
    ByteBuffer cipherTextBlob = ByteBuffer.wrap(Base64.getDecoder().decode(encryptedKey));
    DecryptRequest req = new DecryptRequest().withCiphertextBlob(cipherTextBlob);
    try {
      ByteBuffer plainTextBytes = getKmsClient().decrypt(req).getPlaintext();
      return new String(Base64.getEncoder().encode(plainTextBytes.array()));
    } catch (RuntimeException e) {
      throw new CryptoException("Error when trying to decrypt symmetric key. Please check the following:\n"
          + "\t- Does the application use an IAM role?\n"
          + "\t- Does the application's role have the permission to use the CMK the value was encrypted with?\n"
          + "More information on that topic: https://confluence.in.here.com/display/CMECMCPDOWS/Encryption+of+secrets");
    }
  }

  private static String decryptSymmetric(String key, String toDecrypt) throws CryptoException {
    if (toDecrypt == null) {
      throw new NullPointerException("Value to decrypt must not be null");
    }

    IvParameterSpec iv = new IvParameterSpec(hexStringToByteArray(toDecrypt.substring(0, 32)));
    String encrypted = toDecrypt.substring(32);
    try {
      Cipher decipher = Cipher.getInstance(SYMMETRIC_ALGORITHM);
      decipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(hashSyncKey(key), "AES"), iv);
      return new String(decipher.doFinal(Base64.getDecoder().decode(encrypted)));
    } catch (Exception e) {
      throw new CryptoException("Error when performing symmetric decryption of the payload", e);
    }
  }

  private static byte[] hashSyncKey(String key) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    return digest.digest(key.getBytes());
  }

  public static class CryptoException extends Exception {

    public CryptoException(String message) {
      super(message);
    }

    public CryptoException(String message, Throwable cause) {
      super(message, cause);
    }

  }

  private static AWSKMS getKmsClient() {
    if (kmsClient == null) {
       kmsClient = AWSKMSClientBuilder.defaultClient();
    }
    return kmsClient;
  }

  private static byte[] hexStringToByteArray(String s) {
    byte[] b = new byte[s.length() / 2];
    for (int i = 0; i < b.length; i++) {
      int index = i * 2;
      int v = Integer.parseInt(s.substring(index, index + 2), 16);
      b[i] = (byte) v;
    }
    return b;
  }


}
