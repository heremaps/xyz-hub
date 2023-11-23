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
package com.here.naksha.lib.hub;

import static com.here.naksha.lib.core.util.storage.RequestHelper.createFeatureRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.IfConflict;
import com.here.naksha.lib.core.models.storage.IfExists;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.storage.ResultHelper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.core.view.ViewSerialize;
import com.here.naksha.lib.hub.util.ConfigUtil;
import com.here.naksha.lib.psql.PsqlInstanceConfig;
import com.here.naksha.lib.psql.PsqlInstanceConfigBuilder;
import com.here.naksha.lib.psql.PsqlStorage;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

class DisabledNakshaHubTest {

  // TODO : This test to be entirely removed once NakshaHubWiringTest matures sufficiently, as large part of this
  // already gets tested using REST API tests

  static final String TEST_DATA_FOLDER = "src/test/resources/unused_test_data/";
  static INaksha hub = null;
  static NakshaHubConfig config = null;

  @BeforeAll
  static void prepare() throws Exception {
    String dbUrl = System.getenv("TEST_NAKSHA_PSQL_URL");
    String password = System.getenv("TEST_NAKSHA_PSQL_PASS");
    if (password == null) {
      password = "password";
    }
    if (dbUrl == null) {
      dbUrl = "jdbc:postgresql://localhost/postgres?user=postgres&password=" + password
          + "&schema=naksha_test_maint_hub";
    }

    final PsqlInstanceConfig psqlInstanceCfg =
        new PsqlInstanceConfigBuilder().parseUrl(dbUrl).build();
    final NakshaHubConfig customCfg = ConfigUtil.readConfigFile("mock-config");
    hub = NakshaHubFactory.getInstance(NakshaHubConfig.defaultAppName(), psqlInstanceCfg, customCfg, null);
    config = hub.getConfig();
  }

  private String readTestFile(final String filePath) throws Exception {
    return new String(Files.readAllBytes(Paths.get(TEST_DATA_FOLDER + filePath)));
  }

  private <T> T parseJson(final @NotNull String jsonStr, final @NotNull Class<T> type) throws Exception {
    T obj = null;
    try (final Json json = Json.get()) {
      obj = json.reader(ViewDeserialize.Storage.class).forType(type).readValue(jsonStr);
    }
    return obj;
  }

  private String toJson(final @NotNull Object obj) throws Exception {
    String jsonStr = null;
    try (final Json json = Json.get()) {
      jsonStr = json.writer(ViewSerialize.Storage.class).writeValueAsString(obj);
    }
    return jsonStr;
  }

  // TODO HP : Need to relook at tests just to validate wiring part
  // @Test
  // @Order(1)
  void tc0001_testCreateStorage() throws Exception {
    // 1. Load test data
    final String requestJson = readTestFile("TC0001_createStorage/create_storage.json");
    final String expectedBodyPart = readTestFile("TC0001_createStorage/result_part.json");
    final Storage storage = parseJson(requestJson, Storage.class);
    // Create new NakshaContext
    final NakshaContext ctx = new NakshaContext().withAppId(config.appId);
    ctx.attachToCurrentThread();
    // Create WriteFeature Request to add new storage
    try (final IWriteSession admin = hub.getSpaceStorage().newWriteSession(ctx, true)) {
      final Result wrResult = admin.execute(createFeatureRequest(
          NakshaAdminCollection.STORAGES, storage, IfExists.REPLACE, IfConflict.REPLACE));
      // check result
      if (wrResult instanceof SuccessResult) {
        ForwardCursor<XyzFeature, XyzFeatureCodec> resultCursor = wrResult.getXyzFeatureCursor();
        assertTrue(resultCursor.hasNext());
        resultCursor.next();
        Storage resultStorage = (Storage) resultCursor.getFeature();
        assertFalse(resultCursor.hasNext());
        JSONAssert.assertEquals(
            "Mismatch in Storage WriteResult",
            expectedBodyPart,
            toJson(resultStorage),
            JSONCompareMode.LENIENT);
      } else {
        admin.rollback(true);
        fail("Unexpected result while creating storage!" + wrResult);
      }
      admin.commit(true);
    }
  }

  // @Test
  // @Order(2)
  void tc0002_testCreateDuplicateStorage() throws Exception {
    // 1. Load test data
    final String requestJson = readTestFile("TC0001_createStorage/create_storage.json");
    final Storage storage = parseJson(requestJson, Storage.class);
    // Create new NakshaContext
    final NakshaContext ctx = new NakshaContext().withAppId(config.appId);
    ctx.attachToCurrentThread();
    // Create WriteFeature Request to add new storage
    try (final IWriteSession admin = hub.getSpaceStorage().newWriteSession(ctx, true)) {
      final Result wrResult = admin.execute(createFeatureRequest(
          NakshaAdminCollection.STORAGES, storage, IfExists.REPLACE, IfConflict.REPLACE));
      // we expect exception
      admin.rollback(true);
      if (wrResult instanceof ErrorResult er) {
        assertEquals(
            XyzError.CONFLICT.value(), er.reason.value(), "Expecting conflict error on duplicate storage!");
        return;
      }
      fail("Received different result while creating duplicate storage! " + wrResult);
    }
  }

  // @Test
  // @Order(3)
  void tc0003_testGetStorages() throws Exception {
    // 1. Load test data
    final String expectedBodyPart = readTestFile("TC0003_getStorages/result_part.json");
    // Create new NakshaContext
    final NakshaContext ctx = new NakshaContext().withAppId(config.appId);
    ctx.attachToCurrentThread();
    // Create ReadFeatures Request to read all storages from Admin DB
    final ReadFeatures readFeaturesReq = new ReadFeatures(NakshaAdminCollection.STORAGES);
    // Submit request to NH Space Storage
    try (final IReadSession reader = hub.getSpaceStorage().newReadSession(ctx, false)) {
      final Result result = reader.execute(readFeaturesReq);
      if (result == null) {
        fail("Storage read result is null!");
      } else if (result instanceof ErrorResult er) {
        fail("Exception reading storages " + er);
      } else {
        try {
          // Read all available storages (upto a max limit, e.g. 10)
          final List<Storage> storages = ResultHelper.readFeaturesFromResult(result, Storage.class);
          // convert storage list to JSON string before comparison
          final String storagesJson = toJson(storages);
          JSONAssert.assertEquals(
              "Expecting default psql Storage", expectedBodyPart, storagesJson, JSONCompareMode.LENIENT);
        } catch (Exception e) {
          fail("Unexpected result while reading storages : " + result.getClass() + "/n" + e);
        }
      }
    }
  }

  @AfterAll
  static void close() throws InterruptedException {
    if (hub != null) {
      // drop schema after test execution
      if (hub.getAdminStorage() instanceof PsqlStorage psqlStorage) {
        psqlStorage.dropSchema();
      }
      // TODO: Find a way to gracefully shutdown the hub
    }
  }
}
