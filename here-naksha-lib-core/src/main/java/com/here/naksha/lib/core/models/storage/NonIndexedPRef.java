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
package com.here.naksha.lib.core.models.storage;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Any property search, be extremely careful as it allows to search through not indexed properties.
 * Always use it as a last "where" condition and always together with indexed conditions that will drastically decrease
 * result set first.
 */
public class NonIndexedPRef extends PRef {

  private static final Logger log = LoggerFactory.getLogger(NonIndexedPRef.class);

  public NonIndexedPRef(@NotNull String... path) {
    super(path);
    log.atInfo().setMessage("NonIndexedPRef: {}").addArgument(path).log();
  }
}
