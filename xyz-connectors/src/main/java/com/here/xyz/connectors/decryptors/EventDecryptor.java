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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

/**
 * Base class for all EventDecryptor implementations. Takes care of caching the decrypted secrets.
 *
 */
public abstract class EventDecryptor {
  /**
   * Enum with the existing {@link EventDecryptor} implementations
   */
  public enum Decryptors {KMS, PRIVATE_KEY, TEST, DUMMY}
  /**
   * Prefix for secrets that should be encrypted.
   */
  public final static String TO_ENCRYPT_PREFIX = ">>";
  /**
   * Postfix for secrets that should be encrypted.
   */
  public final static String TO_ENCRYPT_POSTFIX = "<<";
  /**
   * Prefix for asymmetric encrypted secrets.
   */
  public final static String TO_DECRYPT_PREFIX = "<<";
  /**
   * Postfix for asymmetric encrypted secrets.
   */
  public final static String TO_DECRYPT_POSTFIX = ">>";
  /**
   * Map with already created instances.
   */
  private final static Map<Decryptors, EventDecryptor> instances = new HashMap<>();

  /**
   * Cache for storing the decrypted secrets for 30s.
   */
  protected final ExpiringMap<String, String> secretsCache = ExpiringMap.builder()
      .maxSize(1024)
      .variableExpiration()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(30, TimeUnit.SECONDS)
      .build();

  /**
   * This method returns an instance of the known EventDecryptor implementations.

   * @param decryptor The implementation to return {@link Decryptors}.
   * @return Returns a singleton of the implementation.
   */
  public static synchronized EventDecryptor getInstance(final Decryptors decryptor) {
    if (instances.get(decryptor) == null) {
      switch (decryptor) {
        case KMS:
          instances.put(decryptor, new KmsEventDecryptor());
          break;
        case PRIVATE_KEY:
          instances.put(decryptor, new PrivateKeyEventDecryptor());
          break;
        case TEST:
          instances.put(decryptor, new TestDecryptor());
          break;
        case DUMMY:
        default:
          instances.put(decryptor, new DummyDecryptor());
      }
    }
    return instances.get(decryptor);
  }

  /**
   * This method decrypts the given parameters.
   *
   * @param params The parameters that should be decrypted.
   * @return Returns the decrypted parameters or the original if no values are encrypted or could decrypted.
   */
  public Map<String, Object> decryptParams(final Map<String, Object> params) {
    if (params == null) {
      return null;
    }
    params.forEach((key, value) -> params.put(key, decryptValue(value)));
    return params;
  }

  /**
   * This method encrypts the given parameters.
   *
   * @param params The parameters that should be encrypted.
   * @param fieldToEncrypt A set of field names that should be encrypted.
   * @return Returns the encrypted parameters or the original if no values should be encrypted or could encrypted.
   */
  public Map<String, Object> encryptParams(final Map<String, Object> params, final Set<String> fieldToEncrypt) {
    if (params == null) {
      return null;
    }
    params.forEach(((key, value) -> params.put(key, encryptValue(key, value, fieldToEncrypt))));
    return params;
  }

  @SuppressWarnings("unchecked")
  private Object decryptValue(final Object value) {
    if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      map.forEach((k,v) -> map.put(k, decryptValue(v)));
      return map;
    } else if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      return list.stream().map(this::decryptValue).collect(Collectors.toList());
    } else if (value instanceof String) {
      String encrypted = (String) value;
      if (isEncrypted(encrypted)) {
        return secretsCache.computeIfAbsent(encrypted, this::decryptAsymmetric);
      } else {
        return value;
      }
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  private Object encryptValue(final String key, final Object value, final Set<String> fieldToEncrypt) {
    if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      map.forEach((k,v) -> map.put(k, encryptValue(key + "." + k, v, fieldToEncrypt)));
      return map;
    } else if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      return list.stream().map(v -> encryptValue(key + "[]", v, fieldToEncrypt)).collect(Collectors.toList());
    } else if (value instanceof String) {
      if (isToBeEncrypted((String) value) || (fieldToEncrypt.contains(key) && !isEncrypted((String) value))) {
        return encryptAsymmetric((String) value);
      } else {
        return value;
      }
    } else {
      return value;
    }
  }

  /**
   * This methods checks whether the string is encrypted or not.
   *
   * @param str The string that should be checked.
   * @return Returns true if the string is encrypted, otherwise false.
   */
  public boolean isEncrypted(final String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    return str.startsWith(TO_DECRYPT_PREFIX) && str.endsWith(TO_DECRYPT_POSTFIX);
  }

  /**
   * This methods checks whether the string should be encrypted or not.
   *
   * @param str The string that should be checked.
   * @return Returns true if the string should be encrypted, otherwise false.
   */
  public boolean isToBeEncrypted(final String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    return str.startsWith(TO_ENCRYPT_PREFIX) && str.endsWith(TO_ENCRYPT_POSTFIX);
  }

  /**
   * This method encrypts a single string.
   *
   * @param secret The string to encrypt.
   * @return Returns the encrypted string or the original value if the string cannot be encrypted.
   */
  public abstract String encryptAsymmetric(String secret);

  /**
   * This method decrypts a single string.
   *
   * @param encrypted The string to decrypt.
   * @return Returns the decrypted string or the original value if the string is not encrypted or could not be decrypted.
   */
  public abstract String decryptAsymmetric(String encrypted);
}
