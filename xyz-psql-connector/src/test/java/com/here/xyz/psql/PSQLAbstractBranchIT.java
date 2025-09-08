/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.psql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ModifyBranchEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;

public abstract class PSQLAbstractBranchIT extends PSQLAbstractIT {

  @BeforeAll
  public static void init() throws Exception {
    initEnv(null);
  }

  protected String writeFeature(String featureId) throws Exception {
    return writeFeature(featureId, 0, null);
  }

  protected String writeFeature(String featureId, int nodeId, List<Ref> branchPath) throws Exception {
    return invokeLambda(new WriteFeaturesEvent()
            .withSpace(TEST_SPACE_ID)
            .withNodeId(nodeId)
            .withBranchPath(branchPath)
            .withResponseDataExpected(true)
            .withModifications(Set.of(new WriteFeaturesEvent.Modification()
                    .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
                    .withFeatureData(new FeatureCollection().withFeatures(List.of(newTestFeature(featureId))))))
    );
  }

  protected ModifyBranchEvent eventForCreate(ModifyBranchEvent.Operation operation, Ref ref) {
    return new ModifyBranchEvent()
            .withOperation(operation)
            .withSpace(TEST_SPACE_ID)
            .withBaseRef(ref);
  }

  protected Long extractVersion(FeatureCollection featureCollection) throws JsonProcessingException {
    if (featureCollection == null || featureCollection.getFeatures() == null || featureCollection.getFeatures().isEmpty())
      return null;

    return featureCollection.getFeatures().get(0).getProperties().getXyzNamespace().getVersion();
  }

  protected boolean checkIfBranchTableExists(int branchNodeId, int baseNodeId, long baseVersion) throws Exception {
    return checkIfBranchTableExists(TEST_SPACE_ID, branchNodeId, baseNodeId, baseVersion);
  }

  private boolean checkIfBranchTableExists(String spaceId, int branchNodeId, int baseNodeId, long baseVersion) throws Exception {
    try (Connection connection = getDataSourceProvider().getReader().getConnection()) {
      String tableName = spaceId + "_" + baseNodeId + "_" + baseVersion + "_" + branchNodeId;
      return connection
              .getMetaData()
              .getTables(null, PG_SCHEMA, tableName, null)
              .next();
    }
  }

  protected Ref getBaseRef(int nodeId) {
    return getBaseRef(nodeId, -1);
  }

  protected Ref getBaseRef(int nodeId, long version) {
    return new Ref("~" + nodeId + ":" + (version == -1 ? "HEAD" : version));
  }
}
