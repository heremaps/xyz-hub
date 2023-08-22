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

package com.here.xyz.psql.datasource;

import javax.sql.DataSource;

public abstract class DataSourceProvider {

  static DataSourceProvider defaultProvider;

  public abstract DataSource getReader();

  public abstract DataSource getWriter();

  public boolean hasReader() {
    return getReader() != null && getReader() != getWriter();
  }

  public static DataSourceProvider getDefaultProvider() {
    return defaultProvider;
  }

  public static void setDefaultProvider(DataSourceProvider provider) {
    defaultProvider = provider;
  }

}