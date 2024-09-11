/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.util.db.ECPSTool;
import java.security.GeneralSecurityException;
import org.junit.jupiter.api.Test;

public class EcpsTest {

  public static final String TEST_SECRET = "someSecret";
  public static final String TEST_DATA = "someData";
  public static final String ENCRYPTED_TEST_DATA = "6HqMa3/CHP+JlmYAmizoflgpYpv1PAa7WVVrivAztAwH9CJR";

  @Test
  public void decrypt() throws GeneralSecurityException {
    assertEquals(TEST_DATA, ECPSTool.decrypt(TEST_SECRET, ENCRYPTED_TEST_DATA));
  }

  @Test
  public void encryptAndDecrypt() throws GeneralSecurityException {
    assertEquals(TEST_DATA, ECPSTool.decrypt(TEST_SECRET, ECPSTool.encrypt(TEST_SECRET, TEST_DATA)));
  }
}
