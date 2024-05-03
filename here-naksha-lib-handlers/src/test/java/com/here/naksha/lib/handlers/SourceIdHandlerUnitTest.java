package com.here.naksha.lib.handlers;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.lambdas.F0;
import com.here.naksha.lib.core.lambdas.Lambda;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.handlers.util.PropertyOperationUtil;
import com.here.naksha.test.common.FileUtil;
import org.json.JSONException;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.Optional;
import java.util.stream.Stream;

import static com.here.naksha.test.common.FileUtil.loadFileOrFail;
import static com.here.naksha.test.common.FileUtil.parseJsonFileOrFail;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SourceIdHandlerUnitTest {

    @Test
    void tc2002_testMapEqToContainsTag() {
        //given
        NonIndexedPRef pRef = new NonIndexedPRef(XyzFeature.PROPERTIES, XyzProperties.HERE_META_NS, "sourceId");
        POp given = POp.eq(pRef, "task_1");
        //when

        Optional<POp> result = SourceIdHandler.mapIntoTagOperation(given);
        //then

        assertTrue(result.isPresent());
        assertEquals(result.get().getPropertyRef().getTagName(), "xyz_source_id_task_1");
        assertEquals(result.get().op(), POpType.EXISTS);
    }

    @Test
    void tc2003_testMapNotEqToNotContainsTag() {
        //given
        NonIndexedPRef pRef = new NonIndexedPRef(XyzFeature.PROPERTIES, XyzProperties.HERE_META_NS, "sourceId");
        POp given = POp.not(POp.eq(pRef, "task_1"));
        //when

        PropertyOperationUtil.transformPropertyInPropertyOperationTree(given, SourceIdHandler::mapIntoTagOperation);
        //then

        assertFalse(given.children().isEmpty());

        POp nestedPop = given.children().get(0);
        assertEquals(nestedPop.getPropertyRef().getTagName(), "xyz_source_id_task_1");
        assertEquals(nestedPop.op(), POpType.EXISTS);
    }

    @Test
    void tc2004_testMapContainsToContainsTag() {
        //given
        NonIndexedPRef pRef = new NonIndexedPRef(XyzFeature.PROPERTIES, XyzProperties.HERE_META_NS, "sourceId");
        POp given = POp.contains(pRef, "task_1");
        //when

        Optional<POp> result = SourceIdHandler.mapIntoTagOperation(given);
        //then

        assertTrue(result.isPresent());
        assertEquals(result.get().getPropertyRef().getTagName(), "xyz_source_id_task_1");
        assertEquals(result.get().op(), POpType.EXISTS);
    }

    @Test
    void tc2005_testMapOnlyCorrectPref() {
        //given
        NonIndexedPRef pRef = new NonIndexedPRef(XyzFeature.PROPERTIES, XyzProperties.HERE_META_NS, "WrongPRef");
        POp given = POp.eq(pRef, "task_1");
        //when

        Optional<POp> result = SourceIdHandler.mapIntoTagOperation(given);
        //then

        assertTrue(result.isEmpty());
    }

    @Test
    void tc2006_testMapsCorrectlyCombinedOperation () {
        //given
        NonIndexedPRef pRef = new NonIndexedPRef(XyzFeature.PROPERTIES, XyzProperties.HERE_META_NS, "sourceId");
        POp given = POp.and(POp.not(POp.eq(pRef, "task_1")), POp.contains(PRef.tag("funnyTag"), "4"));
        //when

        PropertyOperationUtil.transformPropertyInPropertyOperationTree(given, SourceIdHandler::mapIntoTagOperation);
        //then

        assertEquals(given.op(), OpType.AND);
        assertFalse(given.children().isEmpty());
        assertEquals(given.children().size(), 2);
        assertEquals(given.children().get(0).op(), OpType.NOT);

        POp nestedPop = given.children().get(0).children().get(0);
        assertEquals(nestedPop.getPropertyRef().getTagName(), "xyz_source_id_task_1");
        assertEquals(nestedPop.op(), POpType.EXISTS);

        assertEquals(given.children().get(1).op(), POpType.CONTAINS);
    }
    @Test
    void tc2007_testMapEqToContainsTagWithoutNormalization() {
        //given
        NonIndexedPRef pRef = new NonIndexedPRef(XyzFeature.PROPERTIES, XyzProperties.HERE_META_NS, "sourceId");
        POp given = POp.eq(pRef, "tAskK_1");

        //when
        Optional<POp> result = SourceIdHandler.mapIntoTagOperation(given);

        //then
        assertTrue(result.isPresent());
        assertEquals(result.get().getPropertyRef().getTagName(), "xyz_source_id_tAskK_1");
        assertEquals(result.get().op(), POpType.EXISTS);
    }

    @ParameterizedTest
    @MethodSource("writeRequestTestParams")
    void testWriteRequestTagPopulation(final WriteFeatures<XyzFeature,?,?> wf, final String expectedFeatureJson) throws JSONException {
        // Given: Mocking in place
        final INaksha naksha = mock(INaksha.class);
        final IEvent event = mock(IEvent.class);
        when(event.getRequest()).thenReturn((Request)wf);
        when(event.sendUpstream(any())).thenReturn(new SuccessResult());

        // Given: Handler initialization
        final EventHandler e = new EventHandler(SourceIdHandler.class, "some_id");
        final SourceIdHandler sourceIdHandler = new SourceIdHandler(e, naksha, null);

        // When: handler processing logic is invoked
        try (final Result result = sourceIdHandler.process(event)) {
            assertTrue(result instanceof SuccessResult, "SuccessResult was expected");
        }
        // Then: validate that the feature in the original request is modified as per expectation
        assertNotNull(wf.features.get(0));
        assertNotNull(wf.features.get(0).getFeature());
        JSONAssert.assertEquals("Output Feature not as expected", expectedFeatureJson, wf.features.get(0).getFeature().serialize(), JSONCompareMode.STRICT);
    }

    private static Stream<Arguments> writeRequestTestParams() {
        // Common parameters across tests
        final String commonFilePath = "SourceIdFilter/testWriteFeatureTagPopulation/input_feature.json";
        final String expectedFeatureJson = loadFileOrFail("SourceIdFilter/testWriteFeatureTagPopulation/output_feature.json");

        return Stream.of(
                Arguments.arguments(
                        Named.named(
                                "WriteXyzFeatures tag population",
                                createWriteXyzFeaturesFromFile(commonFilePath)
                        ),
                        expectedFeatureJson
                ),
                Arguments.arguments(
                        Named.named(
                                "ContextWriteXyzFeatures tag population",
                                createContextWriteXyzFeaturesFromFile(commonFilePath)
                        ),
                        expectedFeatureJson
                )
        );
    }

    private static WriteFeatures<?,?,?> createWriteXyzFeaturesFromFile(final String filePath) {
        final XyzFeature feature = parseJsonFileOrFail(filePath, XyzFeature.class);
        return new WriteXyzFeatures("some_collection").add(EWriteOp.CREATE, feature);
    }

    private static WriteFeatures<?,?,?> createContextWriteXyzFeaturesFromFile(final String filePath) {
        final XyzFeature feature = parseJsonFileOrFail(filePath, XyzFeature.class);
        return new ContextWriteXyzFeatures("some_collection").add(EWriteOp.CREATE, feature);
    }

}