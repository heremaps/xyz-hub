/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.psql;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;

public class PsqlInstanceConfigDeserializer extends StdDeserializer<PsqlInstanceConfig> {

  public PsqlInstanceConfigDeserializer() {
    this(null);
  }

  public PsqlInstanceConfigDeserializer(Class<?> valueClass) {
    super(valueClass);
  }

  @Override
  public PsqlInstanceConfig deserialize(JsonParser p, DeserializationContext ctx)
      throws IOException, JacksonException {
    final ObjectMapper mapper = (ObjectMapper) p.getCodec();
    assert mapper != null;
    final TreeNode node = mapper.readTree(p);
    assert node != null;
    if (node.isValueNode() && node instanceof JsonNode jsonNode) {
      if (jsonNode.isTextual()) {
        final String url = jsonNode.asText();
        final PsqlInstanceConfigBuilder builder = new PsqlInstanceConfigBuilder();
        builder.parseUrl(url);
        return builder.build();
      } else if (jsonNode.isNull()) {
        return null;
      }
    }
    // TODO: Fix that we're able to parse object itself again, we should somehow redirect to Jackson default, but
    // how?
    throw JsonMappingException.from(ctx, "Invalid node, expected JsonNode that is a value, but found: " + node);
  }
}
