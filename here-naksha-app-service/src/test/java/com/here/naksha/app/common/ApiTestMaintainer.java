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
package com.here.naksha.app.common;

import static com.here.naksha.app.common.TestNakshaAppInitializer.localPsqlBasedNakshaApp;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import com.here.naksha.app.service.NakshaApp;
import com.here.naksha.lib.psql.PsqlStorage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for once-per suite infrastructure build-up and tear-down.
 *
 * See <a href="https://stackoverflow.com/a/51556718/7033439">this SO answer</a> for some context
 */
public class ApiTestMaintainer implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

  private static final Logger log = LoggerFactory.getLogger(ApiTestMaintainer.class);

  private static final String API_TEST_MAINTAINER_CONTEXT = "api_test_maintainer";
  private static final AtomicReference<NakshaApp> initializedNaksha = new AtomicReference<>(null);

  @Override
  public void beforeAll(ExtensionContext context) {
    if (initializedNaksha.get() == null) {
      prepareNaksha();
      registerCloseCallback(context);
    }
  }

  @Override
  public void close() {
    log.info("Tearing down NakshaApp...");
    NakshaApp app = initializedNaksha.get();
    if (app != null) {
      app.stopInstance();
      log.info("NakshaApp torn down");
    } else {
      log.info("Unable to find running NakshaApp, nothing to tear down");
    }
  }

  /**
   * This method ensures that `close` method will be called once all maintained tests are done
   */
  private void registerCloseCallback(ExtensionContext context) {
    context.getRoot().getStore(GLOBAL).put(API_TEST_MAINTAINER_CONTEXT, this);
  }

  public static NakshaApp nakshaApp() {
    return initializedNaksha.get();
  }

  private static void prepareNaksha() {
    log.info("Initializing NakshaApp...");
    TestNakshaAppInitializer nakshaAppInitializer =
        localPsqlBasedNakshaApp(); // to use mock, call NakshaAppInitializer.mockedNakshaApp()
    cleanUpDb(nakshaAppInitializer.testDbUrl);
    NakshaApp app = nakshaAppInitializer.initNaksha();
    app.start();
    try {
      Thread.sleep(5000); // wait for server to come up
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    log.info("Initialized NakshaApp");
    initializedNaksha.set(app);
  }

  private static void cleanUpDb(String testUrl) {
    log.info("Cleaning up schema for url: {}", testUrl);
    if (testUrl != null && !testUrl.isBlank()) {
      try (PsqlStorage psqlStorage = new PsqlStorage(testUrl)) {
        psqlStorage.dropSchema();
      }
    }
  }
}
