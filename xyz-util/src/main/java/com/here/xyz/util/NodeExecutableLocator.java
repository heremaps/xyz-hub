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

package com.here.xyz.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;

/**
 * This utility is only used for debugging purposes.
 */
public class NodeExecutableLocator {

  // ---------------- Main for testing ----------------
  public static void main(String[] args) throws Exception {
    String node = findNode();
    System.out.println("Node executable found at: " + node);
  }

  public static String findNode() throws IOException, InterruptedException {
    String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);

    if (os.contains("win")) {
      // 1. Windows: system wide Node
      String nodePath = findNodeWindows();
      if (nodePath != null)
        return nodePath;

      // 2. Windows: nvm-windows
      nodePath = findNodeNvmWindows();
      if (nodePath != null)
        return nodePath;

    }
    else {
      // 1. Linux/macOS: system wide Node
      String nodePath = findNodeUnix();
      if (nodePath != null)
        return nodePath;

      // 2. Linux/macOS: nvm
      nodePath = findNodeNvmUnix();
      if (nodePath != null)
        return nodePath;
    }

    throw new RuntimeException("Node executable could not be found on this system");
  }

  // ---------------- Windows Helpers ----------------
  private static String findNodeWindows() throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder("where", "node");
    Process p = pb.start();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String line = reader.readLine();
      p.waitFor();
      if (line != null && !line.isEmpty())
        return line.trim();
    }
    return null;
  }

  private static String findNodeNvmWindows() throws IOException, InterruptedException {
    // nvm-windows typically stores path in NVM_HOME or NVM_SYMLINK
    String nvmSymlink = System.getenv("NVM_SYMLINK");
    if (nvmSymlink != null) {
      File node = new File(nvmSymlink, "node.exe");
      if (node.exists())
        return node.getAbsolutePath();
    }
    return null;
  }

  // ---------------- Unix/Linux/macOS Helpers ----------------
  private static String findNodeUnix() throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder("which", "node");
    Process p = pb.start();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String line = reader.readLine();
      p.waitFor();
      if (line != null && !line.isEmpty())
        return line.trim();
    }
    return null;
  }

  private static String findNodeNvmUnix() throws IOException, InterruptedException {
    // Try loading nvm explicitly
    String command = "export NVM_DIR=\"$HOME/.nvm\" && " + "[ -s \"$NVM_DIR/nvm.sh\" ] && . \"$NVM_DIR/nvm.sh\" && " + "nvm which current";

    ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
    Process p = pb.start();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String line = reader.readLine();
      p.waitFor();
      if (line != null && !line.isEmpty())
        return line.trim();
    }
    return null;
  }
}
