package com.here.xyz.util.json;

import static com.here.xyz.util.StringCache.intern;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.here.xyz.util.StringCache;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

/**
 * Jackson JSON parser module that internalizes (de-duplicates) strings using the {@link
 * StringCache}. This reduces the memory usage and improves the performance when comparing strings,
 * especially when strings as used in hash-maps as keys.
 */
public class JsonModule extends SimpleModule {

    public JsonModule() {
        addKeySerializer(String.class, new StringKeySerializer());
        addKeyDeserializer(String.class, new StringKeyDeserializer());
        addSerializer(String.class, new StringSerializer());
        addDeserializer(String.class, new StringDeserializer());
    }

    private static final class StringKeyDeserializer extends KeyDeserializer {

        @Override
        public Object deserializeKey(String key, @NotNull DeserializationContext ctxt) throws IOException {
            return key != null ? intern(key) : null;
        }
    }

    private static final class StringKeySerializer extends StdSerializer<String> {

        private StringKeySerializer() {
            super(String.class);
        }

        @Override
        public void serialize(@NotNull String value, @NotNull JsonGenerator g, @NotNull SerializerProvider provider)
                throws IOException {
            g.writeFieldName(value);
        }
    }

    private static final class StringSerializer extends StdSerializer<String> {

        private StringSerializer() {
            super(String.class);
        }

        @Override
        public void serialize(String value, JsonGenerator g, SerializerProvider serializers) throws IOException {
            if (value == null) {
                g.writeNull();
            } else {
                g.writeString(value);
            }
        }
    }

    private static final class StringDeserializer extends StdDeserializer<String> {

        private StringDeserializer() {
            super(String.class);
        }

        @Override
        public String deserialize(@NotNull JsonParser p, @NotNull DeserializationContext ctx)
                throws IOException, JacksonException {
            final TreeNode node = p.getCodec().readTree(p);
            final String s;
            if (node instanceof NumericNode numericNode) {
                s = numericNode.asText();
            } else if (node instanceof BooleanNode booleanNode) {
                s = booleanNode.toString();
            } else if (node instanceof TextNode textNode) {
                s = textNode.asText();
            } else {
                s = null;
            }
            return s != null ? intern(s) : null;
        }
    }
}
