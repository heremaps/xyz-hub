/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.util.db.pg;

import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;

import java.sql.SQLException;

/**
 * The empty-space template is needed to read a lazy space without a table, but it is missing or not identical.
 * Both messages begin with a fixed phrase, which monitoring alerts on.
 */
public class TemplateUnavailableException extends SQLException {
  public static final String INTERNAL_ERROR = "XX000";
  public static final String MISSING = "Empty-space template missing";
  public static final String NOT_IDENTICAL = "Empty-space template not identical";

  private TemplateUnavailableException(String reason) {
    super(reason, INTERNAL_ERROR);
  }

  /**
   * @param schema The schema in which the template was expected
   */
  public static TemplateUnavailableException missing(String schema) {
    return new TemplateUnavailableException(MISSING + ": " + TEMPLATE_TABLE + " in schema " + schema);
  }

  /**
   * @param schema The schema in which the template was found not identical
   */
  public static TemplateUnavailableException notIdentical(String schema) {
    return new TemplateUnavailableException(NOT_IDENTICAL + ": " + TEMPLATE_TABLE + " in schema " + schema);
  }
}
