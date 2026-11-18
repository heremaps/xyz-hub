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

package com.here.xyz.jobs.steps.impl.export;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.models.hub.Ref.HEAD;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exports of 1-level and 2-level composite spaces with ExportSpaceToFiles.
 *
 * <pre>
 * SPACE_ID (root, A)      50 features, i = 1..50
 *   └─ SPACE_ID_L1 (B)     2 features, i = 1..2   (extends A)
 *        └─ SPACE_ID_L2 (C) 1 feature, i = 1      (extends B)
 * </pre>
 *
 * All feature IDs are distinct, so the composite content is the union of all three spaces. The root space has more
 * rows (higher i values) than the other two, so an i-range that only considers B and C cannot cover it.
 */
public class TwoLevelCompositeExportStepTest extends ExportTestBase {
  private static final Logger logger = LogManager.getLogger();
  private static final int ROOT_COUNT = 50;
  private static final int L1_COUNT = 2;
  private static final int L2_COUNT = 1;

  private final String SPACE_ID_L1 = SPACE_ID + "_l1";
  private final String SPACE_ID_L2 = SPACE_ID + "_l2";

  @BeforeEach
  public void setUpTwoLevelComposite() {
    putFeatureCollectionToSpace(SPACE_ID, createKnownFeatures("root", ROOT_COUNT));

    createSpace(new Space().withId(SPACE_ID_L1).withExtension(new Space.Extension().withSpaceId(SPACE_ID)), false);
    putFeatureCollectionToSpace(SPACE_ID_L1, createKnownFeatures("l1", L1_COUNT));

    createSpace(new Space().withId(SPACE_ID_L2).withExtension(new Space.Extension().withSpaceId(SPACE_ID_L1)), false);
    putFeatureCollectionToSpace(SPACE_ID_L2, createKnownFeatures("l2", L2_COUNT));
  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(SPACE_ID_L2);
    deleteSpace(SPACE_ID_L1);
    super.cleanup();
  }

  @Test
  public void oneLevelCompositeContextDefault() throws Exception {
    exportAndCompare(SPACE_ID_L1, DEFAULT, ROOT_COUNT + L1_COUNT, List.of(SPACE_ID_L1, SPACE_ID));
  }

  @Test
  public void twoLevelCompositeContextDefault() throws Exception {
    exportAndCompare(SPACE_ID_L2, DEFAULT, ROOT_COUNT + L1_COUNT + L2_COUNT, List.of(SPACE_ID_L2, SPACE_ID_L1));
  }

  @Test
  public void twoLevelCompositeContextExtension() throws Exception {
    exportAndCompare(SPACE_ID_L2, EXTENSION, L2_COUNT, List.of(SPACE_ID_L2, SPACE_ID_L1));
  }

  /**
   * @param iRangeSpaces The spaces ExportSpaceToFiles#resolveIRange() reads min / max i from: the space itself and its
   *                     direct base. Used only to report the resulting range.
   */
  private void exportAndCompare(String spaceId, SpaceContext context, int expectedCount, List<String> iRangeSpaces)
      throws Exception {
    Set<String> expectedIds = customReadFeaturesQuery(spaceId, "/search?limit=1000&context=" + context).getFeatures().stream()
        .map(Feature::getId).collect(Collectors.toCollection(TreeSet::new));

    ExportSpaceToFiles step = new ExportSpaceToFiles()
        .withThreadCount(10)
        .withSpaceId(spaceId)
        .withJobId(JOB_ID)
        .withContext(context)
        .withVersionRef(new Ref(HEAD));

    sendLambdaStepRequestBlock(step, true);
    Set<String> exportedIds = exportedIds(step.loadOutputs());

    Set<String> missing = new TreeSet<>(expectedIds);
    missing.removeAll(exportedIds);
    Set<String> unexpected = new TreeSet<>(exportedIds);
    unexpected.removeAll(expectedIds);

    long[] iRange = iRange(iRangeSpaces);
    Map<String, Long> missingRootIs = rootIs(missing);

    String report = String.format("""
        export of %s, context %s:
          hub returned %d features (test data expects %d), export contains %d
          i-range from %s: [%d, %d], root %s: %s
          missing %d: %s
          i of missing ids in root: %s
          unexpected %d: %s""",
        spaceId, context, expectedIds.size(), expectedCount, exportedIds.size(),
        iRangeSpaces, iRange[0], iRange[1], SPACE_ID, Map.of("min_i", iRange(List.of(SPACE_ID))[0], "max_i", iRange(List.of(SPACE_ID))[1]),
        missing.size(), missing, missingRootIs, unexpected.size(), unexpected);
    logger.info(report);
    System.out.println("TWO_LEVEL_EXPORT_REPORT " + report);

    if (expectedIds.size() != expectedCount || !missing.isEmpty() || !unexpected.isEmpty())
      throw new AssertionError(report);
  }

  private Set<String> exportedIds(List<Output> outputs) throws IOException {
    Set<String> ids = new HashSet<>();
    for (Output output : outputs)
      if (output instanceof DownloadUrl downloadUrl)
        downloadFileAndDeserializeFeatures(downloadUrl).forEach(f -> ids.add(f.getId()));
    return ids;
  }

  private long[] iRange(List<String> spaceIds) throws SQLException {
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    try (Connection connection = getTestDatabaseConnection()) {
      for (String spaceId : spaceIds)
        try (PreparedStatement ps = connection.prepareStatement(
            "SELECT min(i), max(i) FROM public.\"" + getRootTableName(spaceId) + "\"");
            ResultSet rs = ps.executeQuery()) {
          rs.next();
          min = Math.min(min, rs.getLong(1));
          max = Math.max(max, rs.getLong(2));
        }
    }
    return new long[]{min, max};
  }

  private Map<String, Long> rootIs(Set<String> ids) throws SQLException {
    Map<String, Long> result = new LinkedHashMap<>();
    if (ids.isEmpty())
      return result;
    try (Connection connection = getTestDatabaseConnection();
        PreparedStatement ps = connection.prepareStatement(
            "SELECT id, i FROM public.\"" + getRootTableName(SPACE_ID) + "\" WHERE id = ANY(?) ORDER BY i")) {
      ps.setArray(1, connection.createArrayOf("text", ids.toArray()));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next())
          result.put(rs.getString(1), rs.getLong(2));
      }
    }
    return result;
  }
}
