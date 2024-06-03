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
package com.here.naksha.lib.psql.sql;

import com.here.naksha.lib.core.models.storage.transformation.BufferTransformation;
import com.here.naksha.lib.core.models.storage.transformation.GeographyTransformation;
import com.here.naksha.lib.core.models.storage.transformation.GeometryTransformation;
import com.here.naksha.lib.psql.SQL;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlGeometryTransformationResolver {

  public static SQL addTransformation(
      @Nullable GeometryTransformation transformation, @NotNull String variablePlaceholder) {
    SQL variableSql = new SQL(variablePlaceholder);
    if (transformation == null) {
      return variableSql;
    }
    if (transformation.hasChildTransformation()) {
      variableSql = addTransformation(transformation.getChildTransformation(), variablePlaceholder);
    }
    SQL sql = new SQL();
    if (transformation instanceof BufferTransformation) {
      BufferTransformation bufferT = (BufferTransformation) transformation;
      sql.add(" ST_Buffer(")
          .add(variableSql)
          .add(",")
          .add(bufferT.getDistance())
          .add(",")
          .addLiteral(bufferT.getProperties())
          .add(") ");
    } else if (transformation instanceof GeographyTransformation) {
      sql.add(variableSql).add("::geography ");
    } else {
      throw new UnsupportedOperationException("add missing transformation");
    }
    return sql;
  }
}
