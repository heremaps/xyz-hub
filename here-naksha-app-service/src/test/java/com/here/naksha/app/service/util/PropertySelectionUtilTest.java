package com.here.naksha.app.service.util;

import com.here.naksha.app.service.http.ops.PropertySelectionUtil;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.Set;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class PropertySelectionUtilTest {

    private static Stream<Arguments> propSelectionPathTestData() {
        return Stream.of(
                selectionTestSpec(
                        "Single selection param",
                        "selection=p.status"
                        ,
                        Set.of(
                                "id",
                                "type",
                                "geometry",
                                "properties.status"
                        )
                ),
                selectionTestSpec(
                        "Positive test with multiple params",
                        "selection=p.name,p.capacity" // comma separated properties
                                + "&selection=p.references.0,p.references.0" // repeating entire array element
                                + "&selection=p.isoCountryCode"
                                + "&selection=properties.isoCountryCode" // repeating element in expanded form
                                + "&selection=p.color,rootPropertyName" // root level property
                                + "&selection=,f.tags" // empty prop and special prop
                                + "&selection=f.id," // standard prop and empty prop
                                + "&selection=123" // numeric property
                        ,
                        Set.of(
                                "id",
                                "type",
                                "geometry",
                                "properties.name",
                                "properties.capacity",
                                "properties.references.0",
                                "properties.isoCountryCode",
                                "properties.color",
                                "rootPropertyName",
                                "properties.@ns:com:here:xyz.tags",
                                "123"
                        )
                ),
                selectionTestSpec(
                        "Mixed query params",
                        "selection=p.status"
                                + "&tags=tag_1,tag_2" // some other query param
                                + "&selection=p.isoCountryCode"
                        ,
                        Set.of(
                                "id",
                                "type",
                                "geometry",
                                "properties.status",
                                "properties.isoCountryCode"
                        )
                ),
                selectionTestSpec(
                        "No selection param",
                        "tags=tag_1,tag_2"
                        ,
                        null
                ),
                selectionTestSpec(
                        "Empty selection param",
                        "selection="
                        ,
                        null
                ),
                selectionTestSpec(
                        "Multiple empty selection params",
                        "selection="
                                +"&selection=," // comma separated empty params
                        ,
                        null
                )
        );
    }

    @ParameterizedTest
    @MethodSource("propSelectionPathTestData")
    void testPropSelectionPathSetFromQueryParam(
            final @NotNull String queryString,
            final @Nullable Set<String> expectedPropPaths) {
        // Given: input query param string
        final QueryParameterList params = new QueryParameterList(queryString);

        // When: test function is invoked with query params
        final Set<String> actualPropPaths = PropertySelectionUtil.buildPropPathSetFromQueryParams(params);

        // Then: validate list of property path strings are as expected
        if (expectedPropPaths == null) {
            assertNull(actualPropPaths, "Expected null list");
        } else {
            // compare two sets
            assertEquals(expectedPropPaths.size(), actualPropPaths.size(),
                    "Number of paths in a set doesn't match.\nExpected %s.\nActual %s".formatted(
                            expectedPropPaths.stream().sorted().toList(),
                            actualPropPaths.stream().sorted().toList()
                    ));
            for (final String path : expectedPropPaths) {
                assertTrue(actualPropPaths.contains(path), "Property path %s not found.\nExpected %s.\nActual %s.".formatted(
                        path,
                        expectedPropPaths.stream().sorted().toList(),
                        actualPropPaths.stream().sorted().toList()
                ));
            }

        }
    }

    private static Arguments selectionTestSpec(String testDesc, String query, Set<String> expectedPropPaths) {
        return arguments(query, named(testDesc, expectedPropPaths));
    }

}
