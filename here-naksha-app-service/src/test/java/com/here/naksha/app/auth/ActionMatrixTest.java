package com.here.naksha.app.auth;

import com.here.naksha.app.service.http.auth.ActionMatrix;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ActionMatrixTest {

    private static Stream<Arguments> testParams() {
        return Stream.of(
                testSpec(
                        "URM OR with Multiple AttributeMaps - Positive",
                        // URM ActionMatrix
                        """
                            {
                              "readUser": [
                                {"communityIds": "here"},
                                {"communityIds": "foo"},
                                {"communityIds": "bar"}
                              ]
                            }
                        """,
                        // ARM ActionMatrix
                        """
                            {
                              "readUser": [
                                {
                                  "id": "user_foo",
                                  "communityIds": [ "here", "bmw", "audi" ]
                                }
                              ]
                            }
                        """,
                        // Expected Auth result
                        true
                ),
                testSpec(
                        "URM OR with Multiple AttributeMaps - Negative",
                        """
                            {
                              "readUser": [
                                {"communityIds": "foo"},
                                {"communityIds": "bar"}
                              ]
                            }
                        """,
                        """
                            {
                              "readUser": [
                                {
                                  "id": "user_foo",
                                  "communityIds": [ "here", "bmw", "audi" ]
                                }
                              ]
                            }
                        """,
                        false
                ),
                testSpec(
                        "URM AND with Multiple Attribute Values - Positive",
                        """
                            {
                              "readUser": [
                                {"communityIds": ["here", "bmw"]}
                              ]
                            }
                        """,
                        """
                            {
                              "readUser": [
                                {
                                  "id": "user_foo",
                                  "communityIds": [ "here", "bmw", "audi" ]
                                }
                              ]
                            }
                        """,
                        true
                ),
                testSpec(
                        "URM AND with Multiple Attribute Values - Negative",
                        """
                            {
                              "readUser": [
                                {"communityIds": ["here", "foo"]}
                              ]
                            }
                        """,
                        """
                            {
                              "readUser": [
                                {
                                  "id": "user_foo",
                                  "communityIds": [ "here", "bmw", "audi" ]
                                }
                              ]
                            }
                        """,
                        false
                )

        );
    }

    private static Arguments testSpec(final @NotNull String testDesc,
                                      final @NotNull String URM,
                                      final @NotNull String ARM,
                                      final boolean expectedAuthResult) {
        return Arguments.arguments(URM, ARM, Named.named(testDesc, expectedAuthResult));
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void parameterizedTestExecution (
            final @NotNull String URM,
            final @NotNull String ARM,
            final boolean expectedAuthResult) {
        final ActionMatrix urmMatrix = JsonSerializable.deserialize(URM, ActionMatrix.class);
        final ActionMatrix armMatrix = JsonSerializable.deserialize(ARM, ActionMatrix.class);
        assertNotNull(urmMatrix);
        assertNotNull(armMatrix);
        boolean actualAuthResult = urmMatrix.matches(armMatrix);
        assertEquals(expectedAuthResult, actualAuthResult, "Auth matrix comparison result mismatch");
    }

}
