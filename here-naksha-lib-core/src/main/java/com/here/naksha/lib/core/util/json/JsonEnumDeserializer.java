/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.lib.core.util.json;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class JsonEnumDeserializer extends StdDeserializer<JsonEnum> implements ContextualDeserializer {

  @Override
  public JsonDeserializer<?> createContextual(DeserializationContext ctx, BeanProperty property)
      throws JsonMappingException {
    JavaType javaType = ctx.getContextualType();
    if (javaType == null && property != null) {
      javaType = property.getType();
    }
    if (javaType == null) {
      throw JsonMappingException.from(ctx, "No context type");
    }
    final Class<?> rawClass = javaType.getRawClass();
    if (rawClass != null && !JsonEnum.class.isAssignableFrom(rawClass)) {
      throw JsonMappingException.from(ctx, "The raw-class of the java-type is no valid JsonEnum");
    }
    //noinspection unchecked
    this.targetClass = (Class<? extends JsonEnum>) rawClass;
    return this;
  }

  private Class<? extends JsonEnum> targetClass;

  public JsonEnumDeserializer() {
    // Note: We need this only so that Jackson can invoke createContextual().
    this(null);
  }

  public JsonEnumDeserializer(Class<?> valueClass) {
    super(valueClass);
    if (valueClass != null && JsonEnum.class.isAssignableFrom(valueClass)) {
      //noinspection unchecked
      this.targetClass = (Class<? extends JsonEnum>) valueClass;
    }
  }

  @Override
  public JsonEnum deserialize(@NotNull JsonParser p, @NotNull DeserializationContext ctx)
      throws IOException, JacksonException {
    final ObjectMapper mapper = (ObjectMapper) p.getCodec();
    assert mapper != null;
    final TreeNode node = mapper.readTree(p);
    assert node != null;
    if (node.isValueNode() && node instanceof JsonNode) {
      JsonNode jsonNode = (JsonNode) node;
      if (jsonNode.isTextual()) {
        return JsonEnum.get(targetClass, jsonNode.asText());
      } else if (jsonNode.isNumber()) {
        if (jsonNode.isIntegralNumber()) {
          return JsonEnum.get(targetClass, jsonNode.asLong());
        }
        if (jsonNode.isFloatingPointNumber()) {
          return JsonEnum.get(targetClass, jsonNode.asDouble());
        }
      } else if (jsonNode.isBoolean()) {
        return JsonEnum.get(targetClass, jsonNode.asBoolean());
      } else if (jsonNode.isNull()) {
        return JsonEnum.get(targetClass, null);
      }
    }
    throw JsonMappingException.from(ctx, "Invalid node, expected JsonNode that is a value, but found: " + node);
  }
}
