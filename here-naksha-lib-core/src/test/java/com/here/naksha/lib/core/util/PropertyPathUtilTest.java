package com.here.naksha.lib.core.util;

import com.here.naksha.lib.core.common.TestUtil;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PropertyPathUtilTest {

    private static final String TEST_DATA_FOLDER = "src/test/resources/prop_path_data/";
    private static final String COMMON_INPUT_FILE = "CommonInput.json";



    private static Stream<Arguments> propPathTestData() {
        return Stream.of(
                propPathTestSpec(
                        "Merge of multiple property paths",
                        COMMON_INPUT_FILE,
                        "MergeMultiplePaths/Expected.json",
                        Set.of(
                                "id",
                                "type",
                                "geometry",
                                "addProp", // additional property
                                "properties.status", // just a normal property
                                "properties.references.0", // entire array element
                                "properties.isoCountryCode",
                                "properties.@ns:com:here:utm.priorityParams.backup.dueBy",
                                "properties.@ns:com:here:utm.priorityParams.backup.priority", // integer
                                "properties.@ns:com:here:utm.priorityParams.derived.boost", // float
                                "properties.options.0.key", // nested field inside array
                                "properties.options.1.key", // another element from same "options" array
                                "properties.options.1.value",
                                "properties.options.1.value.lang", // repeating nested element
                                "properties.options.0.value", // going back to previous array element
                                "properties.@ns:com:here:xyz.uuid",
                                "properties.@ns:com:here:xyz.deleted", // boolean
                                "properties.@ns:com:here:xyz.tags.0", // string inside array
                                "properties.@ns:com:here:xyz.tags.2"
                        )
                ),
                propPathTestSpec(
                        "Unknown properties at various levels",
                        COMMON_INPUT_FILE,
                        "UnknownPaths/Expected.json",
                        Set.of(
                                "id",
                                "type",
                                "geometry",
                                "", // empty path
                                "unknown",
                                "properties.unknown",
                                "properties.priority.unknown",
                                "properties.references.0.unknown",
                                "properties.@ns:com:here:utm.tags.0.unknown",
                                "properties.options.1.value.unknown",
                                "properties.@ns:com:here:xyz.tags.unknown", // invalid array index
                                "properties.@ns:com:here:xyz.tags.50", // array index out-of-bounds
                                "properties.@ns:com:here:xyz.tags.-1"
                        )
                ),
                propPathTestSpec(
                        "Select multiple parts of Geometry",
                        COMMON_INPUT_FILE,
                        "PartOfGeometry/WithTypeAndCoordinates.json",
                        Set.of(
                                "id",
                                "type",
                                "geometry.type",
                                "geometry.coordinates.0",
                                "geometry.coordinates.2"
                        )
                ),
                propPathTestSpec(
                        "Select only Geometry type",
                        COMMON_INPUT_FILE,
                        "PartOfGeometry/WithOnlyType.json",
                        Set.of(
                                "id",
                                "type",
                                "geometry.type"
                        )
                ),
                propPathTestSpec(
                        "Skip Geometry completely",
                        COMMON_INPUT_FILE,
                        "PartOfGeometry/EmptyGeometry.json",
                        Set.of(
                                "id",
                                "type",
                                "geometry.none"
                        )
                ),
                propPathTestSpec(
                        "Empty map",
                        COMMON_INPUT_FILE,
                        "EmptyMap/Expected.json",
                        Set.of()
                ),
                propPathTestSpec(
                        "Empty map",
                        COMMON_INPUT_FILE,
                        "EmptyMap/Expected.json",
                        null
                )
        );
    }


    @ParameterizedTest
    @MethodSource("propPathTestData")
    void parameterizedPropPathTest(
            final @NotNull String inputFilePath,
            final @NotNull String expectedFilePath,
            final @Nullable Set<String> propPaths
    ) throws JSONException {
        // Given: Input Feature content
        final String featureJson = TestUtil.loadFileOrFail(TEST_DATA_FOLDER, inputFilePath); // "JsonTest/Input.json");
        final XyzFeature feature = TestUtil.parseJson(featureJson, XyzFeature.class);

        // Given: Expected Feature content
        final String expectedJsonData = TestUtil.loadFileOrFail(TEST_DATA_FOLDER, expectedFilePath); // "JsonTest/Expected.json");

        // When: target function is invoked to extract property paths
        final Map<String, Object> newF = PropertyPathUtil.extractPropertyMapFromFeature(feature, propPaths);

        // Then: validate output Json content matches the expectations
        assertNotNull(newF);
        final String actualJsonData = TestUtil.toJson(newF);
        JSONAssert.assertEquals("Extracted property map doesn't match", expectedJsonData, actualJsonData, JSONCompareMode.STRICT);
    }

    private static Arguments propPathTestSpec(
            final @NotNull String testDesc,
            final @NotNull String inputFilePath,
            final @NotNull String expectedFilePath,
            final @Nullable Set<String> propPaths
    ) {
        return Arguments.arguments(inputFilePath, expectedFilePath, Named.named(testDesc, propPaths));
    }

}
