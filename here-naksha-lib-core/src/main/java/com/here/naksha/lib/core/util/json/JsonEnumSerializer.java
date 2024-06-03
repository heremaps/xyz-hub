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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

@SuppressWarnings("unused")
public class JsonEnumSerializer extends StdSerializer<JsonEnum> {

  public JsonEnumSerializer() {
    this(null);
  }

  public JsonEnumSerializer(Class<JsonEnum> t) {
    super(t);
  }

  @Override
  public void serialize(JsonEnum enumValue, JsonGenerator g, SerializerProvider provider) throws IOException {
    if (enumValue == null || enumValue.isNull()) {
      g.writeNull();
      return;
    }
    final Object value = enumValue.value;
    if (value instanceof Long) {
      Long l = (Long) value;
      g.writeNumber(l);
    } else if (value instanceof Double) {
      Double d = (Double) value;
      g.writeNumber(d);
    } else if (value instanceof String) {
      String s = (String) value;
      g.writeString(s);
    } else {
      g.writeObject(value);
    }
  }
}
