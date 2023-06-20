/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.util.json.JsonSerializable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LazyParsableFeatureList {

    /** Type for a feature list. */
    public static final TypeReference<List<@NotNull Feature>> FEATURE_LIST = new TypeReference<>() {};

    private static final String FEATURE_TYPE = "Feature";
    private String valueString;
    private List<@NotNull Feature> value;

    public LazyParsableFeatureList() {}

    @JsonCreator(mode = Mode.DELEGATING)
    public LazyParsableFeatureList(@Nullable String valueString) {
        this.valueString = valueString;
    }

    @JsonCreator(mode = Mode.DELEGATING)
    public LazyParsableFeatureList(@Nullable List<@NotNull Feature> value) {
        this.value = value;
    }

    @JsonValue
    public @NotNull List<@NotNull Feature> get() {
        if (valueString != null) {
            // TODO: Make generic
            try {
                value = JsonSerializable.deserialize(valueString, FEATURE_LIST);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            valueString = null;
        }
        if (value == null) {
            value = new ArrayList<>();
        }
        return value;
    }

    public void set(@NotNull List<@NotNull Feature> value) {
        this.value = value;
        this.valueString = null;
    }

    public void set(@NotNull String valueString) {
        this.value = null;
        this.valueString = valueString;
    }

    private @Nullable String getValueString() {
        return valueString;
    }

    private void setValueString(String valueString) {
        this.valueString = valueString;
    }

    public static class RawDeserializer extends JsonDeserializer<Object> {

        @Override
        public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            int start = (int) jp.getCurrentLocation().getCharOffset();

            // TODO: Currently the object is parsed, in few cases when this could be avoided.
            // 1. If getTokenLocation()/getCurrentLocation().getCharOffset() returns -1, the source
            // reference is not a string, but an input
            //  stream, byte array, etc. and the value could be still extracted as a string efficiently.
            // 2.  If getTokenLocation()/getCurrentLocation().getCharOffset() larger than 0, but the
            // location doesn't point to the position of
            // the token, then it is possible to extract the value in some cases. For the rest (e.g. input
            // stream without mark support, etc.)
            // the value must be parsed.
            final Object sourceRef = jp.getCurrentLocation().getSourceRef();
            String source = (sourceRef instanceof String)
                    ? (String) sourceRef
                    : (sourceRef instanceof ProxyStringReader) ? ((ProxyStringReader) sourceRef).source : null;

            if (source != null && start > 1 && source.charAt(start - 1) == '[') {
                jp.skipChildren();
                int end = (int) jp.getCurrentLocation().getCharOffset();
                return source.substring(start - 1, end);
            }

            // necessary to allow Feature objects which has no type attribute (for backward compatibility)
            final JsonNode node = jp.readValueAsTree();
            for (JsonNode currNode : node) {
                // check the type and set in case of null
                if (currNode instanceof ObjectNode && currNode.get("type") == null) {
                    ((ObjectNode) currNode).put("type", FEATURE_TYPE);
                }
            }

            final ObjectMapper mapper = (ObjectMapper) jp.getCodec();
            return mapper.treeAsTokens(node).readValueAs(FEATURE_LIST);
        }
    }

    public static class RawSerializer extends JsonSerializer {

        @Override
        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            if (value instanceof LazyParsableFeatureList) {
                final String valueString = ((LazyParsableFeatureList) value).valueString;
                if (valueString != null) {
                    gen.writeRawValue(valueString);
                } else {
                    // TODO: Make generic
                    serializers
                            .findTypedValueSerializer(
                                    serializers.getTypeFactory().constructType(FEATURE_LIST), true, null)
                            .serialize(((LazyParsableFeatureList) value).value, gen, serializers);
                }
            } else {
                // TODO: Make generic
                serializers
                        .findTypedValueSerializer(serializers.getTypeFactory().constructType(FEATURE_LIST), true, null)
                        .serialize(value, gen, serializers);
            }
        }
    }

    /** A string reader, which provides access to the underlying string. */
    public static class ProxyStringReader extends StringReader {

        public ProxyStringReader(String s) {
            super(s);
            source = s;
        }

        public final String source;
    }
}
