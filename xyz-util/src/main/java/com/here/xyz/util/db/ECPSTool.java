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

package com.here.xyz.util.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This tool can be used to prepare a new secret ECPS string for the connectorParams of the PSQL storage connector.
 * Please escape quotes if you want to encode a String.
 * e.g: java ECPSTool encrypt secret "{\"foo\":\"bar\"}"
 */
public class ECPSTool {
  private static final Logger logger = LogManager.getLogger();
  public static final String USAGE = "java ECPSTool encrypt|decrypt <ecps_phrase> <data>";

  public static void main(String[] args) throws GeneralSecurityException, UnsupportedEncodingException {
    String action = args[0];
    String phrase = args[1];
    String data = args[2];

    switch (action) {
      case "encrypt":
        System.out.println(encrypt(phrase, data));
        break;
      case "decrypt":
        System.out.println(decrypt(phrase, data));
        break;
      default:
        System.err.println("ERROR: Invalid action provided.\n\n" + USAGE);
    }
  }

  public static String encrypt(String phrase, String data) throws GeneralSecurityException {
    return encrypt(phrase, data, false);
  }

  public static String decrypt(String phrase, String data) throws GeneralSecurityException {
    return decrypt(phrase, data, false);
  }

  public static String encrypt(String phrase, String data, boolean cacheable) throws GeneralSecurityException {
    return cacheable
        ? AESCacheableHelper.encrypt(data, phrase)
        : AESGCMHelper.getInstance(phrase).encrypt(data);
  }

  public static String decrypt(String phrase, String data, boolean cacheable) throws GeneralSecurityException {
    return cacheable
        ? AESCacheableHelper.decrypt(data, phrase)
        : AESGCMHelper.getInstance(phrase).decrypt(data);
  }

  /**
   * Decrypts data and tries to read it to a {@link Map}.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> decryptToMap(String phrase, String data) {
    try {
      return new ObjectMapper().readValue(decrypt(phrase, data), Map.class);
    }
    catch (Exception e) {
      logger.error("Unable to decrypt data to map.");
      throw new RuntimeException(e);
    }
  }

  private static class AESCacheableHelper {
    private static SecretKeySpec padKey(String secretKey) throws NoSuchAlgorithmException {
      int keyLength = secretKey.length() <= 16 ? 16 : secretKey.length() <= 24 ? 24 : 32;

      MessageDigest sha = MessageDigest.getInstance("SHA-512");
      byte[] keyBytes = sha.digest(secretKey.getBytes());

      return new SecretKeySpec(keyBytes, 0, keyLength, "AES");
    }

    public static String encrypt(String plainText, String secretKey) throws GeneralSecurityException {
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.ENCRYPT_MODE, padKey(secretKey));

      byte[] encryptedBytes = cipher.doFinal(plainText.getBytes());
      return BaseEncoding.base16().encode(encryptedBytes);
    }

    public static String decrypt(String encryptedText, String secretKey) throws GeneralSecurityException {
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.DECRYPT_MODE, padKey(secretKey));

      byte[] decryptedBytes = cipher.doFinal(BaseEncoding.base16().decode(encryptedText));
      return new String(decryptedBytes);
    }
  }

  /**
   * Encrypt and decrypt ECPS Strings by using AesGcm
   */
  private static class AESGCMHelper {
    public static final int TAG_SIZE = 16;
    public static final int IV_SIZE = 12;
    public static final String ALGORITHM = "AES/GCM/NoPadding";
    private static Map<String, AESGCMHelper> helpers = new HashMap<>();
    private SecretKey key;

    private AESGCMHelper(String passphrase) throws GeneralSecurityException {
      //If required - adjust passphrase to 128bit length
      key = new SecretKeySpec(Arrays.copyOfRange((passphrase == null ? "" : passphrase).getBytes(), 0, TAG_SIZE), "AES");
    }

    /**
     * Returns an instance helper for this passphrase.
     * @param passphrase The passphrase from which to derive a key.
     */
    @SuppressWarnings("WeakerAccess")
    public static AESGCMHelper getInstance(String passphrase) throws GeneralSecurityException {
      if (helpers.get(passphrase) == null)
        helpers.put(passphrase, new AESGCMHelper(passphrase));
      return helpers.get(passphrase);
    }

    /**
     * Decrypts the given string.
     * @param data The Base 64 encoded string representation of the encrypted bytes.
     */
    public String decrypt(String data) throws GeneralSecurityException {
      byte[] encrypted = Base64.getDecoder().decode(data);
      final Cipher cipher = getCipher(Cipher.DECRYPT_MODE, encrypted);
      return new String(cipher.doFinal(encrypted, IV_SIZE, encrypted.length - IV_SIZE));
    }

    /**
     * Encrypts the provided string.
     * @param data The string to encode
     * @return A Base 64 encoded string, which represents the encoded bytes.
     */
    public String encrypt(String data) throws GeneralSecurityException {
      byte[] plain = data.getBytes();
      byte[] encrypted = new byte[IV_SIZE + plain.length + TAG_SIZE];
      byte[] iv = getIv();
      System.arraycopy(iv, 0, encrypted, 0, IV_SIZE);
      Cipher cipher = getCipher(Cipher.ENCRYPT_MODE, iv);
      cipher.doFinal(plain, 0, plain.length, encrypted, IV_SIZE);
      return new String(Base64.getEncoder().encode(encrypted));
    }

    private byte[] getIv() {
      byte[] iv = new byte[IV_SIZE];
      new SecureRandom().nextBytes(iv);
      return iv;
    }

    private AlgorithmParameterSpec getParams(byte[] encryptedBuffer) {
      return new GCMParameterSpec(8 * TAG_SIZE, encryptedBuffer, 0, IV_SIZE);
    }

    private Cipher getCipher(int mode, byte[] buffer)
        throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException {
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      cipher.init(mode, key, getParams(buffer));
      return cipher;
    }
  }
}
