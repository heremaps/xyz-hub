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
package com.here.naksha.lib.extmanager.helpers;

import com.here.naksha.lib.extmanager.FileClient;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class FileHelper implements FileClient {
  @Override
  public File getFile(String path) throws IOException {
    Path filePath = toPath(path);
    if (!Files.exists(filePath)) {
      throw new IOException("Local file not found: " + filePath.toAbsolutePath());
    }
    return filePath.toFile();
  }

  @Override
  public String getFileContent(String path) throws IOException {
    Path filePath = toPath(path);
    return Files.readAllLines(filePath).stream().collect(Collectors.joining());
  }

  private Path toPath(String path) {
    if (path.startsWith("file://")) {
      return Paths.get(URI.create(path));
    }
    return Paths.get(path);
  }
}
