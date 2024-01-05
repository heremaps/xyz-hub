package com.here.naksha.app.service.util;

import com.here.naksha.app.service.util.logging.AccessLogUtil;
import com.here.naksha.lib.core.models.XyzError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AccessLogUtilTest {

    private static Stream<Arguments> uriValues() {
        return Stream.of(
                Arguments.of( "TOKEN_AT_END",
                        "/hub/spaces/local-space/features?id=some-id-1&access_token=123456789",
                        "/hub/spaces/local-space/features?id=some-id-1&access_token=xxxx"),
                Arguments.of("TOKEN_AT_START",
                        "/hub/spaces/local-space/features?access_token=123456789&id=some-id-1",
                        "/hub/spaces/local-space/features?access_token=xxxx&id=some-id-1"),
                Arguments.of("TOKEN_IN_BETWEEN",
                        "/hub/spaces/local-space/features?id=some-id-1&access_token=123456789&id=some-id-2",
                        "/hub/spaces/local-space/features?id=some-id-1&access_token=xxxx&id=some-id-2"),
                Arguments.of("ONLY_TOKEN_PARAM",
                        "/hub/spaces/local-space/features?access_token=123456789",
                        "/hub/spaces/local-space/features?access_token=xxxx"),
                Arguments.of("NO_TOKEN",
                        "/hub/spaces/local-space/features?id=some-id-1&id=some-id-2",
                        "/hub/spaces/local-space/features?id=some-id-1&id=some-id-2"),
                Arguments.of("NO_PARAMS",
                        "/hub/spaces/local-space/features",
                        "/hub/spaces/local-space/features")
        );
    }

    @ParameterizedTest
    @MethodSource("uriValues")
    void testUriObscuring(String testName, String inputUri, String expectedObscuredUri) {
        assertEquals(expectedObscuredUri,
                AccessLogUtil.getObscuredURI(inputUri), "URI Obscuring failed for Test "+ testName);
    }

}
