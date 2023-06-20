package com.here.naksha.psql;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.mapcreator.ext.naksha.PsqlConfigBuilder;
import com.here.naksha.lib.core.exceptions.ParameterError;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PsqlConfigBuilderTest {

    @Test
    void withUrl() throws URISyntaxException, ParameterError, UnsupportedEncodingException {
        PsqlConfigBuilder builder = new PsqlConfigBuilder();
        builder.parseUrl("jdbc:postgresql://localhost/test?user=foo&password=foobar&schema=bar");
        assertEquals("localhost", builder.getHost());
        assertEquals(5432, builder.getPort());
        assertEquals("test", builder.getDb());
        assertEquals("foo", builder.getUser());
        assertEquals("foobar", builder.getPassword());
        assertEquals("bar", builder.getSchema());

        builder = new PsqlConfigBuilder();
        builder.parseUrl("jdbc:postgresql://foobar:1234/" + URLEncoder.encode("test:colon", StandardCharsets.UTF_8));
        assertEquals("foobar", builder.getHost());
        assertEquals(1234, builder.getPort());
        assertEquals("test:colon", builder.getDb());
        assertNull(builder.getUser());
        assertNull(builder.getPassword());
    }
}
