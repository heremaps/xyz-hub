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

/**
 * Test implementation for Unit tests.
 */
public class TestDecryptor extends EventDecryptor {

  /**
   * Default constructor with Default access so that only {@link EventDecryptor} can
   * construct this instance.
   *
   */
  TestDecryptor() { }

  /**
   * Test implementation. Adds or replaces pre- and suffix only.
   *
   * @param secret The string that should be "encrypted".
   * @return Returns the "encrypted" string.
   */
  @Override
  public String encryptAsymmetric(final String secret) {
    if (isEncrypted(secret)) {
      // already encrypted, to nothing
      return secret;
    }
    if (isToBeEncrypted(secret)) {
      // should be encrypted, replace pre- and suffix
      return TO_DECRYPT_PREFIX + secret.substring(2, secret.length() - 2) + TO_DECRYPT_PREFIX;
    }
    // simple plaintext, add pre- and suffix
    return TO_DECRYPT_PREFIX + secret + TO_DECRYPT_POSTFIX;
  }

  /**
   * Test implementation. Replace pre- and postfix markers.
   *
   * @param encrypted The string that should be "decrypted".
   * @return Returns the "decrypted" string.
   */
  @Override
  public String decryptAsymmetric(final String encrypted) {
    if (isEncrypted(encrypted)) {
      return TO_ENCRYPT_PREFIX + encrypted.substring(2, encrypted.length() - 2) + TO_ENCRYPT_POSTFIX;
    }
    return encrypted;
  }
}
