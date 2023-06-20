/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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


import com.here.mapcreator.ext.naksha.sql.SQLQuery;
import com.here.xyz.models.payload.events.feature.QueryEvent;
import com.here.xyz.psql.query.GetFeatures;

/**
 * A struct like object that contains the string for a prepared statement and the respective
 * parameters for replacement.
 */
@Deprecated
public class SQLQueryExt extends SQLQuery {

  @Deprecated
  public SQLQueryExt() {}

  @Deprecated
  public SQLQueryExt(String text) {
    super(text);
  }

  @Deprecated
  public SQLQueryExt(String text, Object... parameters) {
    super(text, parameters);
  }

  @SuppressWarnings("rawtypes")
  @Deprecated
  public static SQLQuery selectJson(QueryEvent event) {
    return GetFeatures.buildSelectionFragmentBWC(event);
  }
}
