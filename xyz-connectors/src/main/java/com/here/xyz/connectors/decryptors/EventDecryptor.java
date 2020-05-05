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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

/**
 * Base class for all EventDecryptor implementations. Takes care of caching the decoded secrets.
 *
 */
public abstract class EventDecryptor {
  /**
   * Enum with the existing {@link EventDecryptor} implementations
   */
  public enum Decryptors {KMS, PRIVATE_KEY, DUMMY}
  /**
   * Prefix for asymmetric encrypted secrets.
   */
  public String encPrefix = "<<";
  /**
   * Postfix for asymmetric encrypted secrets.
   */
  public String encPostfix = ">>";
  /**
   * Map with already created instances.
   */
  private static Map<Decryptors, EventDecryptor> instances = new HashMap<>();

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
  public Map<String, Object> decodeParams(final Map<String, Object> params) {
    if (params == null) {
      return null;
    }
    params.forEach((key, value) -> params.put(key, decodeValue(value)));
    return params;
  }

  private Object decodeValue(Object value) {
    if (value instanceof Map) {
      Map<String, Object> map = (Map) value;
      map.forEach((k,v) -> map.put(k, decodeValue(v)));
      return map;
    } else if (value instanceof List) {
      List<Object> list = (List) value;
      return list.stream().map(this::decodeValue).collect(Collectors.toList());
    } else if (value instanceof String) {
      String encoded = (String) value;
      if (isEncrypted(encoded)) {
        return secretsCache.computeIfAbsent(
                encoded,
                k -> decryptAsymmetric(k.substring(2, k.length() - 2)));
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
    if (str == null || str.length() == 0) {
      return false;
    }
    return str.startsWith(encPrefix) && str.endsWith(encPostfix);
  }

  /**
   * This method decodes a single string.
   *
   * @param encoded The string to decode.
   * @return Returns the decoded string or the original value if the string is not encoded or could not be decoded.
   */
  public abstract String decryptAsymmetric(String encoded);
}
