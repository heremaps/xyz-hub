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

package com.here.xyz.jobs.steps.impl.transport;

import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import java.util.List;

/**
 * Provides the data sources used by the tests around the express import writer.
 *
 * <p>The FeatureWriter test framework in {@code com.here.xyz.test.sql.base.SQLITBase} only installs the
 * scripts of the {@code /sql} resource folder. The express writer however lives in {@code /jobs/transport.sql},
 * so a data source configured by {@code SQLITBase} would not know
 * {@code execute_express_import_batch} at all. Therefore the tests use their own {@link DatabaseSettings}
 * which installs both resource folders, using the same schema prefix as
 * {@code com.here.xyz.jobs.steps.execution.db.Database} does in production.</p>
 *
 * <p>Installing {@code /jobs} additionally requires the helper functions of {@code /sql/ext.sql}
 * ({@code max_bigint}, {@code xyz_random_string}, {@code xyz_reduce_precision} and
 * {@code xyz_create_history_partition}), which are provided by the xyz-psql-connector dependency of this
 * module. Note that {@code DatabaseSettings.checkScripts} only logs installation failures instead of
 * failing, so a missing script resource surfaces later as a confusing "function does not exist" error.</p>
 *
 * <p>All space tables are created in the {@code public} schema, which is also the schema the read-back
 * queries of {@code com.here.xyz.test.featurewriter.SpaceWriter} use. That way the express writer and the
 * FeatureWriter operate on exactly the same tables, even though they use different data sources.</p>
 */
final class ExpressWriterDataSources {

  /**
   * The schema the space tables live in. It matches the default schema of {@link DatabaseSettings},
   * which is the schema the inherited read-back queries of the FeatureWriter test framework use.
   */
  static final String SCHEMA = "public";

  //NOTE: Same connection coordinates as used by StepTestBase and SQLITBase (both keep them private).
  private static final String PG_HOST = System.getProperty("pg.host", "localhost");
  private static final String PG_DB = "postgres";
  private static final String PG_USER = "postgres";
  private static final String PG_PW = "password";

  private static final DatabaseSettings DB_SETTINGS = new DatabaseSettings("testExpressWriter")
      .withApplicationName(ExpressWriterDataSources.class.getSimpleName())
      .withHost(PG_HOST)
      .withDb(PG_DB)
      .withUser(PG_USER)
      .withPassword(PG_PW)
      .withDbMaxPoolSize(2)
      .withScriptResourcePaths(List.of(
          new ScriptResourcePath("/sql", "jobs", "common"),
          new ScriptResourcePath("/jobs", "jobs")
      ));

  private ExpressWriterDataSources() {}

  static DataSourceProvider getDataSourceProvider() {
    return new PooledDataSources(DB_SETTINGS);
  }

  /**
   * @return The fully qualified and quoted name of the given table, usable as {@code REGCLASS} input.
   */
  static String qualified(String table) {
    return SCHEMA + ".\"" + table + "\"";
  }
}
