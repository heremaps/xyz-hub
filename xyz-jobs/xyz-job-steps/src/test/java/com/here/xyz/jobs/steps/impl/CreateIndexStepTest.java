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

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.util.Random.randomAlpha;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import com.here.xyz.XyzSerializable;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.util.db.pg.IndexHelper.Index;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * One test instance for the class, so that the connector of the test can be deleted after all cases.
 */
@TestInstance(PER_CLASS)
public class CreateIndexStepTest extends StepTest {
    /** The Hub creates the client of a new connector in its config update cycle, which runs every 2 minutes. */
    private static final long CONNECTOR_READY_TIMEOUT_MILLIS = 3 * 60 * 1_000;
    private final List<String> createdConnectors = new ArrayList<>();

    @AfterAll
    public void deleteConnectors() {
        createdConnectors.forEach(this::deleteConnector);
    }

    @Test
    public void testCreateSystemIndex() throws Exception {
        executeCreateIndexStep(SPACE_ID, SystemIndex.GEO);
    }

    @Test
    public void testCreateOnDemandIndex() throws Exception {
        executeCreateIndexStep(SPACE_ID, new OnDemandIndex().withPropertyPath("foo"));

    }

    @Test
    public void neverWrittenLazySpaceIsSkipped() throws Exception {
        Space lazy = createLazyTestSpace(SPACE_ID + "-lazy", LAZY_CONNECTOR);
        CreateIndex step = new CreateIndex().withSpaceId(lazy.getId()).withIndex(new OnDemandIndex().withPropertyPath("foo"))
            .withJobId(JOB_ID);

        sendLambdaStepRequestBlock(step, true);
        Assertions.assertEquals(0, relationCount(DB_URL, tableName(lazy)), "No table is created for an index");
        Assertions.assertTrue(isFlagged(lazy.getId()));

        //The completion path of production: execute() starts no query, the next state check loads the stored step
        CreateIndex executed = new CreateIndex().withSpaceId(lazy.getId()).withIndex(new OnDemandIndex().withPropertyPath("foo"))
            .withJobId(JOB_ID);
        executed.execute(false);
        Assertions.assertEquals("SUCCEEDED", String.valueOf(executed.getExecutionState()));
        CreateIndex stored = XyzSerializable.deserialize(XyzSerializable.serialize(executed, XyzSerializable.Internal.class),
            CreateIndex.class);
        Assertions.assertEquals("SUCCEEDED", String.valueOf(stored.getExecutionState()));
        Assertions.assertEquals(0, relationCount(DB_URL, tableName(lazy)));
        Assertions.assertTrue(isFlagged(lazy.getId()));
    }

    @Test
    public void indexOnMaterializedExtensionOfNeverWrittenLazyBase() throws Exception {
        Space base = createLazyTestSpace(SPACE_ID + "-base", LAZY_CONNECTOR);
        Space extension = createTestSpace(new Space().withId(SPACE_ID + "-ext").withExtension(new Extension().withSpaceId(base.getId())));
        Assertions.assertTrue(isFlagged(extension.getId()));
        //Materializes only the extension
        putFeatureToSpace(extension.getId(), simpleFeature("f1"));
        Assertions.assertTrue(tableExists(DB_URL, tableName(extension)));

        assertIndexOnExtension(base, extension);
    }

    @Test
    public void indexOnEagerExtensionOfNeverWrittenLazyBase() throws Exception {
        String connectorId = getClass().getSimpleName() + "_" + randomAlpha(5);
        createdConnectors.add(connectorId);
        createConnectorCopy(LAZY_CONNECTOR, connectorId, Map.of());
        Space base = createTestSpaceWhenConnectorIsReady(new Space().withId(SPACE_ID + "-base")
            .withStorage(new ConnectorRef().withId(connectorId)), CONNECTOR_READY_TIMEOUT_MILLIS);
        Assertions.assertTrue(isFlagged(base.getId()));

        //A switch-off takes effect at once for the next create
        patchConnectorParams(connectorId, Map.of("lazyTableCreation", false));
        Space extension = createTestSpace(new Space().withId(SPACE_ID + "-ext").withExtension(new Extension().withSpaceId(base.getId())));
        Assertions.assertEquals(connectorId, extension.getStorage().getId());
        Assertions.assertFalse(isFlagged(extension.getId()));
        Assertions.assertTrue(tableExists(DB_URL, tableName(extension)));

        assertIndexOnExtension(base, extension);
    }

    private void assertIndexOnExtension(Space base, Space extension) throws Exception {
        String extensionTable = tableName(extension);
        OnDemandIndex index = new OnDemandIndex().withPropertyPath("foo");
        Assertions.assertTrue(getOnDemandIndices(extensionTable).isEmpty());

        sendLambdaStepRequestBlock(new CreateIndex().withSpaceId(extension.getId()).withIndex(index).withJobId(JOB_ID), true);

        List<Index> indexes = getOnDemandIndices(extensionTable);
        Assertions.assertEquals(1, indexes.size());
        Assertions.assertEquals(index.getIndexName(extensionTable), indexes.get(0).getIndexName(extensionTable));
        Assertions.assertEquals(0, relationCount(DB_URL, tableName(base)), "The base still has no table");
        Assertions.assertTrue(isFlagged(base.getId()));
    }

    private void executeCreateIndexStep(String spaceId, Index index) throws Exception {
        String tableName = getRootTableName(spaceId);
        CreateIndex step = new CreateIndex().withSpaceId(spaceId).withIndex(index);

        deleteAllExistingIndices(tableName);
        Assertions.assertTrue(getAllExistingIndices(tableName).isEmpty());

        sendLambdaStepRequestBlock(step, true);
        List<Index> indexes = getIndices(tableName, step.getIndex() instanceof SystemIndex, step.getIndex() instanceof OnDemandIndex);
        Assertions.assertEquals(1, indexes.size());
        Assertions.assertEquals(step.getIndex().getIndexName(tableName), indexes.get(0).getIndexName(tableName));
    }
}
