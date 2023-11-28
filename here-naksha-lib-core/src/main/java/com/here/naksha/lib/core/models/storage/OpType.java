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
package com.here.naksha.lib.core.models.storage;

import com.here.naksha.lib.core.util.json.JsonEnum;

/**
 * The basic operation type.
 */
public class OpType extends JsonEnum {

  /**
   * Combine all children via logical AND operator.
   */
  public static final OpType AND = defIgnoreCase(OpType.class, "and");

  /**
   * Combine all children via logical OR operator.
   */
  public static final OpType OR = defIgnoreCase(OpType.class, "or");

  /**
   * Negate the logical state of the child operation, requires exactly one child.
   */
  public static final OpType NOT = defIgnoreCase(OpType.class, "not");

  @Override
  protected void init() {
    register(OpType.class);
    register(POpType.class);
    register(SOpType.class);
  }
}
