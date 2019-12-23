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

package com.here.xyz.psql;

import com.here.xyz.psql.PSQLConfig.AESHelper;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

/**
 * This tool can be used to prepare a new secret ECPS string for the connectorParams of the PSQL storage connector.
 */
public class ECPSTool {
  public static final String USAGE = "java ECPSTool encrypt|decrypt <ecps_phrase> <data>";

  public static void main(String[] args) throws BadPaddingException, IllegalBlockSizeException {
    String action = args[0];
    String phrase = args[1];
    String data = args[2];

    switch (action) {
      case "encrypt":
        System.out.println(new AESHelper(phrase).encrypt(data));
        break;
      case "decrypt":
        System.out.println(new AESHelper(phrase).decrypt(data));
        break;
      default:
        System.err.println("ERROR: Invalid action provided.\n\n" + USAGE);
    }
  }
}
