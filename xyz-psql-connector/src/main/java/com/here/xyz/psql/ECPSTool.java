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

package com.here.xyz.psql;

import com.here.xyz.psql.PSQLConfig.AESGCMHelper;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;

/**
 * This tool can be used to prepare a new secret ECPS string for the connectorParams of the PSQL storage connector.
 * Please escape quotes if you want to encode a String.
 * e.g: java ECPSTool encrypt secret "{\"foo\":\"bar\"}"
 */
public class ECPSTool {
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

  public static String encrypt(String phrase, String data) throws GeneralSecurityException, UnsupportedEncodingException {
    return new AESGCMHelper(phrase).encrypt(data);
  }

  public static String decrypt(String phrase, String data) throws GeneralSecurityException {
    return new AESGCMHelper(phrase).decrypt(data);
  }
}
