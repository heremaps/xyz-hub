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
  public Map<String, Object> decodeParams(final Map<String, Object> params) {
    return params;
  }

  /**
   * Dummy implementation. Just return the original string.
   *
   * @param encoded The string that should be decrypted.
   * @return Returns the not-decrypted string.
   */
  @Override
  public String decryptAsymmetric(final String encoded) {
    return encoded;
  }
}
