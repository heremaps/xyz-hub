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

package com.here.xyz.util.db.pg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.pg.InstalledScripts.Row;
import java.util.List;
import org.junit.jupiter.api.Test;

public class InstalledScriptsTest {
  private static final String SCRIPT_ID = "hub.functions";
  private static final Script SCRIPT = new Script("/sqlSamples/functions.sql", null, "3.24.15", "hub");

  private static InstalledScripts snapshot(Row... rows) {
    return new InstalledScripts(List.of(SCRIPT_ID), List.of(rows));
  }

  private static Row versioned(String version, String hash) {
    return new Row(SCRIPT_ID + ":" + version, true, hash, true, version);
  }

  private static Row unversioned(String hash, String version) {
    return new Row(SCRIPT_ID, true, hash, true, version);
  }

  private static Row[] versions(String unversionedVersion, String... versions) {
    Row[] rows = new Row[versions.length + 1];
    rows[0] = unversioned("someHash", unversionedVersion);
    for (int i = 0; i < versions.length; i++)
      rows[i + 1] = versioned(versions[i], "someHash");
    return rows;
  }

  @Test
  public void parseSchemasWithAndWithoutVersion() {
    InstalledScripts snapshot = snapshot(
        unversioned("hash2", "1.0.1"),
        versioned("1.0.0", "hash1"),
        versioned("1.0.1", "hash2"),
        new Row("hub.other:1.0.2", true, "otherHash", true, "1.0.2"),
        new Row("hub.functions_old:1.0.3", true, "oldHash", true, "1.0.3"));

    assertEquals(List.of("1.0.0", "1.0.1"), snapshot.getVersions(SCRIPT_ID));
    assertTrue(snapshot.exists(SCRIPT_ID, null));
    assertEquals("hash2", snapshot.getHash(SCRIPT_ID, null));
    assertEquals("1.0.1", snapshot.getUnversionedVersion(SCRIPT_ID));
    assertTrue(snapshot.exists(SCRIPT_ID, "1.0.0"));
    assertEquals("hash1", snapshot.getHash(SCRIPT_ID, "1.0.0"));
    assertFalse("Schemas of other scripts must not be taken into account", snapshot.exists(SCRIPT_ID, "1.0.2"));
    assertFalse(snapshot.exists(SCRIPT_ID, "1.0.3"));
    assertEquals("Scripts that were not requested have no schemas", List.of(), snapshot.getVersions("hub.other"));
  }

  @Test
  public void parseVersionedSchemasOnly() {
    InstalledScripts snapshot = snapshot(versioned("1.0.0", "hash1"));

    assertFalse(snapshot.exists(SCRIPT_ID, null));
    assertNull(snapshot.getHash(SCRIPT_ID, null));
    assertNull(snapshot.getUnversionedVersion(SCRIPT_ID));
    assertEquals(List.of("1.0.0"), snapshot.getVersions(SCRIPT_ID));
  }

  @Test
  public void sortVersionsNumerically() {
    InstalledScripts snapshot = snapshot(versions("3.24.15", "3.24.15", "0.0.642", "3.24.4"));

    assertEquals(List.of("0.0.642", "3.24.4", "3.24.15"), snapshot.getVersions(SCRIPT_ID));
  }

  @Test
  public void resolveNewestVersionUpToSoftwareVersion() {
    InstalledScripts snapshot = snapshot(versions("3.24.15", "3.24.4", "3.24.14", "3.24.15"));

    assertEquals("hub.functions:3.24.15", SCRIPT.getCompatibleSchema("3.24.15", snapshot));
    assertEquals("hub.functions:3.24.14", SCRIPT.getCompatibleSchema("3.24.14", snapshot));
    assertEquals("hub.functions:3.24.4", SCRIPT.getCompatibleSchema("3.24.10", snapshot));
    assertEquals("hub.functions:3.24.15", SCRIPT.getCompatibleSchema("4.0.0", snapshot));
    assertEquals("Without an installed version up to the software version, the unversioned schema is used", "hub.functions",
        SCRIPT.getCompatibleSchema("3.24.3", snapshot));
    assertEquals("Without a software version, the unversioned schema is used", "hub.functions",
        SCRIPT.getCompatibleSchema(null, snapshot));
    assertEquals("Without a snapshot, the unversioned schema is used", "hub.functions", SCRIPT.getCompatibleSchema("3.24.15", null));
  }

  @Test
  public void keepTheVersionInUseBeyondTheNewestFive() {
    //The oldest version is the one the unversioned schema reports, so it is still in use and must not be uninstalled
    InstalledScripts snapshot = snapshot(versions("0.0.642", "0.0.642", "3.24.4", "3.24.6", "3.24.7", "3.24.14", "3.24.15"));

    assertEquals(List.of(), snapshot.versionsToUninstall(SCRIPT_ID, 5));
    assertEquals(List.of(), SCRIPT.versionsToUninstall(snapshot, 5));
  }

  @Test
  public void uninstallTheVersionsBeyondTheNewestFive() {
    InstalledScripts snapshot = snapshot(versions("1.0.7", "1.0.1", "1.0.2", "1.0.3", "1.0.4", "1.0.5", "1.0.6", "1.0.7"));
    assertEquals(List.of("1.0.1", "1.0.2"), snapshot.versionsToUninstall(SCRIPT_ID, 5));

    InstalledScripts snapshotWithInUseVersion = snapshot(versions("1.0.1", "1.0.1", "1.0.2", "1.0.3", "1.0.4", "1.0.5", "1.0.6",
        "1.0.7"));
    assertEquals(List.of("1.0.2"), snapshotWithInUseVersion.versionsToUninstall(SCRIPT_ID, 5));

    InstalledScripts snapshotWithFiveVersions = snapshot(versions("1.0.5", "1.0.1", "1.0.2", "1.0.3", "1.0.4", "1.0.5"));
    assertEquals(List.of(), snapshotWithFiveVersions.versionsToUninstall(SCRIPT_ID, 5));

    assertEquals(List.of("1.0.3", "1.0.4", "1.0.5", "1.0.6", "1.0.7"),
        snapshot.withoutVersion(SCRIPT_ID, "1.0.1").withoutVersion(SCRIPT_ID, "1.0.2").getVersions(SCRIPT_ID));
  }

  @Test
  public void handleValuesThatDoNotParse() throws Exception {
    InstalledScripts snapshot = snapshot(
        new Row(SCRIPT_ID, true, null, true, null),
        versioned("1.0.0", "hash1"),
        new Row(SCRIPT_ID + ":latest", true, "hash2", true, "latest"),
        new Row(SCRIPT_ID + ":1.0", true, "hash3", true, "1.0"));

    assertEquals("Schemas with a version that does not parse are skipped", List.of("1.0.0"), snapshot.getVersions(SCRIPT_ID));
    assertTrue(snapshot.exists(SCRIPT_ID, null));
    assertNull(snapshot.getHash(SCRIPT_ID, null));
    assertTrue("A missing hash of the unversioned schema counts as a mismatch", SCRIPT.needsInstallation(snapshot));
  }

  @Test
  public void needsInstallationOnlyIfContentDiffersOrVersionIsMissing() throws Exception {
    String hash = SCRIPT.getContentHash();

    assertFalse(SCRIPT.needsInstallation(snapshot(unversioned(hash, "3.24.15"), versioned("3.24.15", hash))));
    assertTrue("The versioned schema is missing", SCRIPT.needsInstallation(snapshot(unversioned(hash, "3.24.14"),
        versioned("3.24.14", hash))));
    assertTrue("The unversioned schema has other content", SCRIPT.needsInstallation(snapshot(unversioned("otherHash", "3.24.16"),
        versioned("3.24.15", hash))));
    assertTrue(SCRIPT.needsInstallation(snapshot()));
  }
}
