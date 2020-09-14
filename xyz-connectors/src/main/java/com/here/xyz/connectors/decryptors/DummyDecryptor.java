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

import java.util.Map;
import java.util.Set;

/**
 * Dummy implementation that is used when no custom Decryptor is configured.
 */
public class DummyDecryptor extends EventDecryptor {

  /**
   * Default constructor with Default access so that only {@link EventDecryptor} can
   * construct this instance.
   *
   */
  DummyDecryptor() { }

  /**
   * Dummy implementation. Just return the original map.
   *
   * @param params The parameters that should be decrypted.
   * @return Returns the not-decrypted map.
   */
  @Override
  public Map<String, Object> decryptParams(final Map<String, Object> params) {
    return params;
  }

  /**
   * Dummy implementation. Just return the original map.
   *
   * @param params The parameters that should be encrypted.
   * @param fieldsToEncrypt A set with the fields that should be encrypted.
   * @return Returns the not-encrypted map.
   */
  @Override
  public Map<String, Object> encryptParams(final Map<String, Object> params, final Set<String> fieldsToEncrypt) {
    return params;
  }

  /**
   * Dummy implementation. Just return the original string.
   *
   * @param secret The string that should be encrypted.
   * @return Returns the not-encrypted string.
   */
  @Override
  public String encryptAsymmetric(final String secret) {
    return secret;
  }

  /**
   * Dummy implementation. Just return the original string.
   *
   * @param encrypted The string that should be decrypted.
   * @return Returns the not-decrypted string.
   */
  @Override
  public String decryptAsymmetric(final String encrypted) {
    return encrypted;
  }
}
