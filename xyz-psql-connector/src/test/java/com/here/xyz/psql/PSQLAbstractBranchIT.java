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

import static com.here.xyz.events.ModifyBranchEvent.Operation.CREATE;
import static com.here.xyz.events.ModifyBranchEvent.Operation.DELETE;
import static com.here.xyz.events.ModifyBranchEvent.Operation.MERGE;
import static com.here.xyz.events.ModifyBranchEvent.Operation.REBASE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ModifyBranchEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.db.SQLQuery;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public abstract class PSQLAbstractBranchIT extends PSQLAbstractIT {

  @BeforeAll
  public static void init() throws Exception {
    initEnv(null);
  }

  @BeforeEach
  public void setup() throws Exception {
    invokeCreateTestSpace(null, TEST_SPACE_ID());
  }

  @AfterEach
  public void shutdown() throws Exception {
    invokeDeleteTestSpaces(null, List.of(TEST_SPACE_ID()));
    dropSpaceTables(PG_SCHEMA, TEST_SPACE_ID());
  }

  protected final String TEST_SPACE_ID() {
    return spaceId();
  }

  protected String writeFeature(String featureId) throws Exception {
    return writeFeature(featureId, 0, null);
  }

  protected String writeFeature(String featureId, int nodeId, List<Ref> branchPath) throws Exception {
    return writeFeature(featureId, nodeId, branchPath, false);
  }

  protected String writeFeature(String featureId, int nodeId, List<Ref> branchPath, boolean addRandomProperties) throws Exception {
    return writeFeature(featureId, nodeId, branchPath, addRandomProperties, UpdateStrategy.DEFAULT_UPDATE_STRATEGY);
  }

  protected String writeFeature(String featureId, int nodeId, List<Ref> branchPath, boolean addRandomProperties,
                                UpdateStrategy updateStrategy) throws Exception {
    return invokeLambda(new WriteFeaturesEvent()
            .withSpace(TEST_SPACE_ID())
            .withNodeId(nodeId)
            .withBranchPath(branchPath)
            .withResponseDataExpected(true)
            .withVersionsToKeep(1000)
            .withModifications(Set.of(new WriteFeaturesEvent.Modification()
                    .withUpdateStrategy(updateStrategy)
                    .withFeatureData(new FeatureCollection().withFeatures(List.of(newTestFeature(featureId, addRandomProperties))))))
    );
  }

  protected ModifyBranchEvent eventForCreate(Ref baseRef) {
    return new ModifyBranchEvent()
            .withOperation(CREATE)
            .withSpace(TEST_SPACE_ID())
            .withBaseRef(baseRef);
  }

  protected ModifyBranchEvent eventForDelete(int nodeId, Ref baseRef) {
    return new ModifyBranchEvent()
            .withOperation(DELETE)
            .withSpace(TEST_SPACE_ID())
            .withNodeId(nodeId)
            .withBaseRef(baseRef);
  }

  protected ModifyBranchEvent eventForMerge(int nodeId, Ref baseRef, int targetNodeId) {
    return new ModifyBranchEvent()
            .withOperation(MERGE)
            .withSpace(TEST_SPACE_ID())
            .withNodeId(nodeId)
            .withMergeTargetNodeId(targetNodeId)
            .withBaseRef(baseRef);
  }

  protected ModifyBranchEvent eventForRebase(int nodeId, Ref baseRef, Ref newBaseRef) {
    return new ModifyBranchEvent()
            .withOperation(REBASE)
            .withSpace(TEST_SPACE_ID())
            .withNodeId(nodeId)
            .withBaseRef(baseRef)
            .withNewBaseRef(newBaseRef);
  }


  protected Long extractVersion(FeatureCollection featureCollection) throws JsonProcessingException {
    if (featureCollection == null || featureCollection.getFeatures() == null || featureCollection.getFeatures().isEmpty())
      return null;

    return featureCollection.getFeatures().get(0).getProperties().getXyzNamespace().getVersion();
  }

  protected boolean checkIfBranchTableExists(int branchNodeId, int baseNodeId, long baseVersion) throws Exception {
    return checkIfTableExists(getBranchTableName(TEST_SPACE_ID(), branchNodeId, baseNodeId, baseVersion));
  }

  protected boolean checkIfTableExists(String tableName) throws Exception {
    try (Connection connection = getDataSourceProvider().getReader().getConnection()) {
      return connection
              .getMetaData()
              .getTables(null, PG_SCHEMA, tableName, null)
              .next();
    }
  }

  protected List<FeatureRow> getAllRowFromTable(String tableName) throws SQLException {
    return new SQLQuery("SELECT id, version FROM ${schema}.${table} ")
            .withVariable("schema", PG_SCHEMA)
            .withVariable("table", tableName)
            .run(getDataSourceProvider(), rs -> {
              List<FeatureRow> allFeatureIdAndVersion = new ArrayList<>();
              while(rs.next()) {
                allFeatureIdAndVersion.add(new FeatureRow(rs.getString("id"), rs.getLong("version")));
              }
              return allFeatureIdAndVersion;
            });
  }

  public static class FeatureRow {
    private final String id;
    private final long version;

    public FeatureRow(String id, long version) {
      this.id = id;
      this.version = version;
    }

    public String id() {
      return id;
    }

    public long version() {
      return version;
    }
  }

  protected String getBranchTableName(String spaceId, int branchNodeId, Ref ref) {
    return getBranchTableName(spaceId, branchNodeId, Integer.parseInt(ref.getBranch().replace("~", "")), ref.getVersion());
  }

  protected String getBranchTableName(String spaceId, int branchNodeId, int baseNodeId, long baseVersion) {
    return spaceId + "_" + baseNodeId + "_" + baseVersion + "_" + branchNodeId;
  }

  protected Ref getBaseRef(int nodeId) {
    return getBaseRef(nodeId, -1);
  }

  protected Ref getBaseRef(int nodeId, long version) {
    return new Ref("~" + nodeId + ":" + (version == -1 ? "HEAD" : version));
  }
}
