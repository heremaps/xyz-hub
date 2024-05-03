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
package com.here.naksha.test.common;
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

import static com.here.naksha.test.common.JsonUtil.parseJson;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;

public class FileUtil {

  private static final String TEST_DATA_FOLDER = "src/test/resources/unit_test_data/";

  private FileUtil() {}

  public static String loadFileOrFail(final @NotNull String rootPath, final @NotNull String fileName) {
    final String filePath = rootPath + fileName;
    try {
      String json = new String(Files.readAllBytes(Paths.get(filePath)));
      return json;
    } catch (IOException e) {
      Assertions.fail("Unable to read test file " + filePath, e);
      return null;
    }
  }

  public static String loadFileOrFail(final @NotNull String fileName) {
    return loadFileOrFail(TEST_DATA_FOLDER, fileName);
  }

  public static <T> T parseJsonFileOrFail(final @NotNull String fileName, final @NotNull Class<T> type) {
    return parseJson(loadFileOrFail(fileName), type);
  }

  public static <T> T parseJsonFileOrFail(
      final @NotNull String rootPath, final @NotNull String fileName, final @NotNull Class<T> type) {
    return parseJson(loadFileOrFail(rootPath, fileName), type);
  }
}
