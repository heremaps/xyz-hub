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

import static com.here.naksha.app.init.context.TestContextEntrypoint.loadTestContext;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import com.here.naksha.app.init.context.ContainerTestContext;
import com.here.naksha.app.init.context.TestContext;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for once-per suite infrastructure build-up and tear-down.
 * <p>
 * See <a href="https://stackoverflow.com/a/51556718/7033439">this SO answer</a> for some context
 */
public class ApiTestMaintainer implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

  private static final Logger log = LoggerFactory.getLogger(ApiTestMaintainer.class);

  private static final String API_TEST_MAINTAINER_CONTEXT = "api_test_maintainer";

  private static final TestContext TEST_CONTEXT = loadTestContext();

  @Override
  public void beforeAll(ExtensionContext context) {
    if (TEST_CONTEXT.isNotStarted()) {
      log.info("Starting test context for ApiTest");
      registerCloseCallback(context);
      TEST_CONTEXT.start();
    }
  }

  @Override
  public void close() {
    log.info("Stopping test context for ApiTest");
    TEST_CONTEXT.stop();
  }

  /**
   * This method ensures that `close` method will be called once all maintained tests are done
   */
  private void registerCloseCallback(ExtensionContext context) {
    context.getRoot().getStore(GLOBAL).put(API_TEST_MAINTAINER_CONTEXT, this);
  }
}
