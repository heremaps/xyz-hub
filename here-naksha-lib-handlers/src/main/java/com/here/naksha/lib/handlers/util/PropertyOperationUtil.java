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
package com.here.naksha.lib.handlers.util;

import com.here.naksha.lib.core.models.storage.POp;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

public class PropertyOperationUtil {

  private PropertyOperationUtil() {}

  public static void transformPropertyInPropertyOperationTree(
      POp rootPropertyOperation, Function<POp, Optional<POp>> transformingFunction) {
    replacePropertyInPropertyOperationTree(
        rootPropertyOperation, Collections.emptyList(), -1, transformingFunction);
  }

  private static void replacePropertyInPropertyOperationTree(
      POp propertyOperation,
      List<POp> parentCollection,
      int index,
      Function<POp, Optional<POp>> transformingFunction) {

    if (propertyOperation.getPropertyRef() == null
        && propertyOperation.children() != null
        && !propertyOperation.children().isEmpty()) {

      List<@NotNull POp> children = propertyOperation.children();

      for (int i = 0; i < children.size(); i++) {
        replacePropertyInPropertyOperationTree(children.get(i), children, i, transformingFunction);
      }

    } else {
      transformingFunction.apply(propertyOperation).ifPresent(pOp -> parentCollection.set(index, pOp));
    }
  }
}
