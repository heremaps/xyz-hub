package com.here.xyz.models.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SpaceModelTest {
    private static ObjectMapper MAPPER;

    @BeforeAll
    public static void setup() {
        MAPPER = XyzSerializable.Mappers.DEFAULT_MAPPER.get();
    }

    @Test
    public void testJsonSerializeDeserializeWithExtensionVersion() throws Exception {
        Space in = new Space()
                .withId("space-123")
                .withExtension(new Space.Extension().withSpaceId("parent-space").withVersion(5));

        String json = MAPPER.writeValueAsString(in);
        assertTrue(json.contains("\"extends\""), "JSON should contain 'extends' object");
        assertTrue(json.contains("\"spaceId\":\"parent-space\""), "JSON missing extends.spaceId");
        assertTrue(json.contains("\"version\":5"), "JSON missing extends.version");

        Space out = MAPPER.readValue(json, Space.class);
        assertNotNull(out.getExtension(), "Extension should not be null after deserialize");
        assertEquals("parent-space", out.getExtension().getSpaceId());
        assertEquals(5, out.getExtension().getVersion());
    }

    @Test
    public void testToMapFromMapWithExtensionVersion() {
        Space in = new Space()
                .withId("space-456")
                .withExtension(new Space.Extension().withSpaceId("ext-space").withVersion(42));

        Map<String,Object> map = XyzSerializable.toMap(in, XyzSerializable.Static.class);
        assertTrue(map.containsKey("extends"), "Map should contain 'extends' key");

        Map<String,Object> extMap = (Map<String,Object>)map.get("extends");
        assertEquals("ext-space", extMap.get("spaceId"));
        assertEquals(42, ((Number)extMap.get("version")).intValue());

        Space out = XyzSerializable.fromMap(map, Space.class);
        assertNotNull(out.getExtension(), "Extension should not be null after fromMap");
        assertEquals("ext-space", out.getExtension().getSpaceId());
        assertEquals(42, out.getExtension().getVersion());
    }
}
