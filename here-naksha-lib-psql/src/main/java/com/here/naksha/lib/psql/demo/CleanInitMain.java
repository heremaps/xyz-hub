/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.psql.demo;

import com.here.naksha.lib.psql.*;

public class CleanInitMain {

  /**
   * To run add argument with url to your DB i.e. "jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=plv_test"
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println("Missing argument, please call with JDBC connect string, example: ");
      System.err.println(
          "    jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test&id=test&app=test");
      // TEST_ADMIN_DB=jdbc:postgresql://naksha-moderation-db-perftest.ccmrakfzvsi3.us-east-1.rds.amazonaws.com/unimap?user=postgres&password=vDiGjAIRWFhh7KXzQy6v&schema=alweber_junit&id=alweber_junit&app=alweber
      System.exit(1);
      return;
    }
    try (final PsqlStorage storage = new PsqlStorage(args[0])) {
      // Connect and initialize the database.
      storage.dropSchema();
      storage.initStorage();
    }
    System.out.println("DONE");
    System.out.flush();
    System.err.flush();
  }
}
