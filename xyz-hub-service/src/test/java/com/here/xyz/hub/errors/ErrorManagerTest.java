package com.here.xyz.hub.errors;

import com.here.xyz.util.service.DetailedHttpException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ErrorManagerTest {
    @BeforeAll
    public static void setUp() {
        ErrorManager.loadErrors("errors.json");
        Map<String, String> defaults = new HashMap<>();
        defaults.put("resource", "TestResource");
        defaults.put("versionRef", "v1.0");
        defaults.put("resourceId", "12345");
        ErrorManager.init(defaults);
    }

    @Test
    public void testComposeWithoutPlaceholders_validErrorCode() {
        String errorCode = "E318441"; // "$resource not found"
        DetailedHttpException exception = ErrorManager.getHttpException(errorCode);
        assertNotNull(exception);
        assertEquals(404, exception.status.code());
        String message = exception.getMessage();
        assertTrue(message.contains("TestResource not found"));
    }

    @Test
    public void testComposeWithPlaceholders_validErrorCode() {
        String errorCode = "E318404"; // "Invalid value for versionRef"
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("versionRef", "v2.0");
        placeholders.put("cause", "the version does not exist");

        DetailedHttpException exception = ErrorManager.getHttpException(errorCode, placeholders);
        assertNotNull(exception);
        assertEquals(400, exception.status.code());

        String message = exception.getMessage();
        assertTrue(message.contains("v2.0"));
        assertTrue(message.contains("the version does not exist"));
    }

    @Test
    public void testComposeWithDefaultPlaceholders() {
        String errorCode = "E318441"; // "$resource not found"

        DetailedHttpException exception = ErrorManager.getHttpException(errorCode);
        assertNotNull(exception);
        assertEquals(404, exception.status.code());

        String message = exception.getMessage();
        assertTrue(message.contains("12345"));
    }

    @Test
    public void testCompose_invalidErrorCode() {
        String unknownErrorCode = "UNKNOWN_CODE";
        assertThrows(IllegalArgumentException.class, () -> ErrorManager.getHttpException(unknownErrorCode));
    }
}