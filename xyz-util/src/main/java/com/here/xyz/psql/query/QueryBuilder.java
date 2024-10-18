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

package com.here.xyz.psql.query;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;

public abstract class QueryBuilder<I> {
  private DataSourceProvider dataSourceProvider;

  protected abstract SQLQuery buildQuery(I input) throws QueryBuildingException;

  public DataSourceProvider getDataSourceProvider() {
    if (dataSourceProvider == null)
      //Fail quickly in this case
      throw new NullPointerException("No dataSourceProvider was defined for this QueryRunner.");
    return dataSourceProvider;
  }

  public void setDataSourceProvider(DataSourceProvider dataSourceProvider) {
    this.dataSourceProvider = dataSourceProvider;
  }

  public <T extends QueryBuilder> T withDataSourceProvider(DataSourceProvider dataSourceProvider) {
    setDataSourceProvider(dataSourceProvider);
    return (T) this;
  }

  private DatabaseSettings getDbSettings() {
    if (getDataSourceProvider().getDatabaseSettings() != null)
      return getDataSourceProvider().getDatabaseSettings();
    throw new IllegalStateException("The DataSourceProvider does not provide database settings.");
  }

  protected String getSchema() {
    return getDbSettings().getSchema();
  }

  public static class QueryBuildingException extends Exception {

    public QueryBuildingException(String message, Throwable cause) {
      super(message, cause);
    }

    public QueryBuildingException(String message) {
      super(message);
    }

    public QueryBuildingException(Throwable cause) {
      super(cause);
    }
  }
}
