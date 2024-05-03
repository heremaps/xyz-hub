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

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for all API-related tests. Extending this class ensures that NakshaApp & all required storages are running
 */
@ExtendWith({ApiTestMaintainer.class})
public abstract class ApiTest {

  private final NakshaTestWebClient nakshaClient;

  public ApiTest() {
    this(new NakshaTestWebClient());
  }

  public ApiTest(NakshaTestWebClient nakshaClient) {
    this.nakshaClient = nakshaClient;
  }

  public NakshaTestWebClient getNakshaClient() {
    return nakshaClient;
  }
}
