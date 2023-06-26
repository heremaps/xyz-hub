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
package com.here.naksha.lib.core.exceptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

/**
 * A wrapper class to throw an {@link JsonProcessingException} or an {@link IOException} as runtime exception.
 */
public class JsonProcessingFailed extends RuntimeException {

  public JsonProcessingFailed(@NotNull IOException e) {
    super(e);
  }
}
