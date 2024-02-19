package com.here.naksha.lib.handlers;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.test.common.FileUtil;
import com.here.naksha.test.common.JsonUtil;
import com.here.naksha.test.common.assertions.POpAssertion;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.here.naksha.lib.core.models.storage.PRef.TAGS_PROP_PATH;
import static org.junit.jupiter.api.Assertions.*;

class TagFilterHandlerTest {

    @Test
    void testReadWithoutPOpAndSingleTagFilter() {
        // Given: ReadFeatures request without any property operation
        final ReadFeatures request = new ReadFeatures();
        // Given: single tag filter
        final List<String> tagFilter = List.of("violated_ftype_topology");
        // When: a function is called with request and tag filter configuration
        TagFilterHandler.applyFilterConditionOnRequest(request, tagFilter);
        // Then: Validate that Tags "exists" condition is added to request
        POpAssertion.assertThatOperation(request.getPropertyOp())
                .hasType(POpType.EXISTS)
                .hasPRefWithPath(TAGS_PROP_PATH)
                .existsWithTagName("violated_ftype_topology")
        ;
    }

    @Test
    void testReadWithoutPOpAndMultipleTagFilters() {
        // Given: ReadFeatures request without any property operation
        final ReadFeatures request = new ReadFeatures();
        // Given: multiple tag filters
        final List<String> tagFilter = List.of("violated_ftype_topology","some_other_tag");
        // When: a function is called with request and tag filter configuration
        TagFilterHandler.applyFilterConditionOnRequest(request, tagFilter);
        // Then: Validate that Tags "and" condition is added to request with multiple tag filters
        POpAssertion.assertThatOperation(request.getPropertyOp())
                .hasType(POpType.AND)
                .hasChildrenThat(
                        op1 -> op1.hasType(POpType.EXISTS).hasPRefWithPath(TAGS_PROP_PATH).existsWithTagName("violated_ftype_topology"),
                        op2 -> op2.hasType(POpType.EXISTS).hasPRefWithPath(TAGS_PROP_PATH).existsWithTagName("some_other_tag")
                )
        ;
    }

    @Test
    void testReadWithPOpAndSingleTagFilter() {
        // Given: ReadFeatures request with atleast one property operation
        final POp inputPOp = POp.eq(PRef.uuid(),"some_uuid_value");
        final ReadFeatures request = new ReadFeatures().withPropertyOp(inputPOp);
        // Given: single tag filter
        final List<String> tagFilter = List.of("violated_ftype_topology");
        // When: a function is called with request and tag filter configuration
        TagFilterHandler.applyFilterConditionOnRequest(request, tagFilter);
        // Then: Validate that Tags "and" condition is added to request including original inputPOp and Tag condition
        POpAssertion.assertThatOperation(request.getPropertyOp())
                .hasType(POpType.AND)
                .hasChildrenThat(
                        op1 -> assertEquals(inputPOp, op1.getPOp()),
                        op2 -> op2.hasType(POpType.EXISTS).hasPRefWithPath(TAGS_PROP_PATH).existsWithTagName("violated_ftype_topology")
                )
        ;
    }

    @Test
    void testReadWithPOpAndMultipleTagFilters() {
        // Given: ReadFeatures request with atleast one property operation
        final POp inputPOp = POp.eq(PRef.uuid(),"some_uuid_value");
        final ReadFeatures request = new ReadFeatures().withPropertyOp(inputPOp);
        // Given: multiple tag filters
        final List<String> tagFilter = List.of("violated_ftype_topology","some_other_tag");
        // When: a function is called with request and tag filter configuration
        TagFilterHandler.applyFilterConditionOnRequest(request, tagFilter);
        // Then: Validate that Tags "and" condition is added to request including original inputPOp
        // and nested "and" condition between multiple Tags
        POpAssertion.assertThatOperation(request.getPropertyOp())
                .hasType(POpType.AND)
                .hasChildrenThat(
                        op1 -> assertEquals(inputPOp, op1.getPOp()),
                        op2 -> op2.hasType(POpType.AND)
                                .hasChildrenThat(
                                        cop1 -> cop1.hasType(POpType.EXISTS).hasPRefWithPath(TAGS_PROP_PATH).existsWithTagName("violated_ftype_topology"),
                                        cop2 -> cop2.hasType(POpType.EXISTS).hasPRefWithPath(TAGS_PROP_PATH).existsWithTagName("some_other_tag")
                                )
                )
        ;
    }

    @Test
    void testReadWithPOpButWithoutTagFilter() {
        // Given: ReadFeatures request with atleast one property operation
        final POp inputPOp = POp.eq(PRef.uuid(),"some_uuid_value");
        final ReadFeatures request = new ReadFeatures().withPropertyOp(inputPOp);
        // When: a function is called with request and no tag filter configuration (i.e. null)
        TagFilterHandler.applyFilterConditionOnRequest(request, null);
        // Then: Validate that existing request operation remains unchanged
        assertEquals(inputPOp, request.getPropertyOp(), "Expected request operation same as input operation");
        // When: a function is called with request and empty tag filter configuration
        TagFilterHandler.applyFilterConditionOnRequest(request, List.of());
        // Then: Validate that existing request operation remains unchanged
        assertEquals(inputPOp, request.getPropertyOp(), "Expected request operation same as input operation");
    }

    private static Stream<Arguments> writeTestData() {
        return Stream.of(
                // without any tag filter
                writeTestSpec(
                        "testWriteWithoutTagFilter",
                        "TagFilter/input_features.json",
                        null,
                        null,
                        "TagFilter/testWriteWithoutTagFilter/features.json"
                ),
                // with addTags having one tag which is already present
                writeTestSpec(
                        "testWriteWithAddTags",
                        "TagFilter/input_features.json",
                        List.of("nine", "one"),
                        null,
                        "TagFilter/testWriteWithAddTags/features.json"
                ),
                // with removeTags having one tag which is not present
                writeTestSpec(
                        "testWriteWithRemoveTags",
                        "TagFilter/input_features.json",
                        null,
                        List.of("nine", "on"),
                        "TagFilter/testWriteWithRemoveTags/features.json"
                ),
                // with both addTags and removeTags, by replacing existing tag "one" with "once"
                writeTestSpec(
                        "testWriteWithAddAndRemoveTags",
                        "TagFilter/input_features.json",
                        List.of("two", "once"),
                        List.of("two", "nine", "on"),
                        "TagFilter/testWriteWithAddAndRemoveTags/features.json"
                )
        );
    }

    private static Arguments writeTestSpec(final String testDesc,
                                           final @NotNull String inputFilePath,
                                           final @Nullable List<String> addTags,
                                           final @Nullable List<String> removeTags,
                                           final @NotNull String outputFilePath) {
        return Arguments.arguments(inputFilePath, addTags, removeTags, Named.named(testDesc, outputFilePath));
    }

    @ParameterizedTest
    @MethodSource("writeTestData")
    void commonWriteTest(final @NotNull String inputFilePath,
                               final @Nullable List<String> addTags,
                               final @Nullable List<String> removeTags,
                               final @NotNull String outputFilePath) throws JSONException {
        // Given: WriteXyzFeatures request with some tags already part of features
        final String featuresJson = FileUtil.loadFileOrFail(inputFilePath);
        final XyzFeatureCollection inputCollection = JsonUtil.parseJson(featuresJson, XyzFeatureCollection.class);
        final WriteXyzFeatures wf = RequestHelper.upsertFeaturesRequest("some_space", inputCollection.getFeatures());
        // Given: Expected feature collection JSON
        final String expectedJson = FileUtil.loadFileOrFail(outputFilePath);

        // When: a function is called with request and given tag filter configuration
        TagFilterHandler.applyTagChangesOnRequest(wf, addTags, removeTags);
        // Then: Validate that the output features in the request is as expected
        final String actualJson = covertWriteFeaturesToCollectionJson(wf.features);
        JSONAssert.assertEquals("List of output features don't match", expectedJson, actualJson, JSONCompareMode.STRICT_ORDER);
    }

    private String covertWriteFeaturesToCollectionJson(final @NotNull List<XyzFeatureCodec> codecList) {
        final List<XyzFeature> features = new ArrayList<>();
        for (final @NotNull XyzFeatureCodec codec : codecList) {
            features.add(codec.getFeature());
        }
        final XyzFeatureCollection outputCollection = new XyzFeatureCollection().withFeatures(features);
        return JsonUtil.toJson(outputCollection);
    }

}