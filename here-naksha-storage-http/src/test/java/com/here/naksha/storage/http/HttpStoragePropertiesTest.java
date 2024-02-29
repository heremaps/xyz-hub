package com.here.naksha.storage.http;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HttpStoragePropertiesTest {

    final static String TEST_RESOURCE_DIR = "/unit_test_data/HttpStorageProperties/";

    @Test
    void t01_testConvertAllFields() {
        var properties = jsonResourceToPropertiesOrFail("t01_testConvertAllFields");

        assertEquals("https://example.org", properties.getUrl());
        assertEquals(60, properties.getConnectTimeout());
        assertEquals(3600, properties.getSocketTimeout());

        Map<String, String> headers = properties.getHeaders();
        assertEquals("Bearer <token>", headers.get("Authorization"));
        assertEquals("application/json", headers.get("Content-Type"));
        assertEquals(2, properties.getHeaders().size());
    }

    @Test
    void t02_testConvertMissingToDefault() {
        var properties = jsonResourceToPropertiesOrFail("t02_testConvertMissingToNull");

        assertEquals("https://example.org", properties.getUrl());
        assertEquals(HttpStorageProperties.DEF_CONNECTION_TIMEOUT_SEC, properties.getConnectTimeout());
        assertEquals(HttpStorageProperties.DEF_SOCKET_TIMEOUT_SEC, properties.getSocketTimeout());

        assertEquals(HttpStorageProperties.DEFAULT_HEADERS, properties.getHeaders());
    }

    @Test
    void t03_testDontThrowOnExcessFields() {
        assertDoesNotThrow(
                () -> jsonResourceToPropertiesOrFail("t03_testDontThrowOnExcessFields")
        );
    }

    @Test
    void t04_testThrowOnMissingMandatory() {
        UncheckedIOException wrappingException = assertThrows(
                UncheckedIOException.class,
                () -> jsonResourceToPropertiesOrFail("t04_testThrowOnMissingMandatory")
        );

        IOException causeException = wrappingException.getCause();
        assertInstanceOf(
                MismatchedInputException.class,
                causeException
        );
    }

    private HttpStorageProperties jsonResourceToPropertiesOrFail(String fileName) {
        String resource = TEST_RESOURCE_DIR + fileName + ".json";

        try (InputStream testResourceStream = this.getClass().getResourceAsStream(resource)) {
            if (testResourceStream == null) throw new IOException("Could not access " + resource + " resource");
            return JsonSerializable.deserialize(testResourceStream, HttpStorageProperties.class);
        } catch (IOException e) {
            fail("Unable to convert json resource", e);
            return null;
        }
    }
}