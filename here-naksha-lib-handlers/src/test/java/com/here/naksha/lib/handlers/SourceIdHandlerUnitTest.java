package com.here.naksha.lib.handlers;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.handlers.util.PropertyOperationUtil;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

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
}