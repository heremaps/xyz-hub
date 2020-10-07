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

package com.here.xyz.connectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.decryptors.EventDecryptor;
import com.here.xyz.connectors.decryptors.EventDecryptor.Decryptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class EventDecryptorTest {
  @SuppressWarnings("unchecked")
  @Test
  public void testEnDecryption() throws JsonProcessingException {
    EventDecryptor decryptor = EventDecryptor.getInstance(Decryptors.TEST);
    ObjectMapper mapper = new ObjectMapper();

    Set<String> fieldsToEncrypt = new HashSet<>();
    fieldsToEncrypt.add("secret");
    fieldsToEncrypt.add("object.secret");
    fieldsToEncrypt.add("list[].secret");
    fieldsToEncrypt.add("anotherList[]");

    Map<String, Object> params = new HashMap<>();
    params.put("secret", "test");
    params.put("object", new HashMap<String, Object>() {{ this.put("secret", "test"); }});
    params.put("list", new ArrayList<Map<String, Object>>(1){{ this.add(new HashMap<String, Object>() {{ this.put("secret", "test"); }});}});
    params.put("anotherList", new ArrayList<String>(3) {{ this.add("test1"); this.add("test2"); this.add("test3"); }});

    // make copy of params for later comparison
    String original = mapper.writeValueAsString(params);

    System.out.println("Original params:");
    System.out.println(original);

    Map<String, Object> result = decryptor.encryptParams(params, fieldsToEncrypt, "mySpaceId");

    String encrypted = mapper.writeValueAsString(result);
    System.out.println("Encrypted params:");
    System.out.println(encrypted);

    assertTrue(decryptor.isEncrypted((String) result.get("secret")));
    assertTrue(decryptor.isEncrypted((String) ((Map<String, Object>) result.get("object")).get("secret")));
    assertTrue(decryptor.isEncrypted((String) ((List<Map<String, Object>>) result.get("list")).get(0).get("secret")));
    assertTrue(decryptor.isEncrypted((String) ((List<Object>) result.get("anotherList")).get(0)));
    assertTrue(decryptor.isEncrypted((String) ((List<Object>) result.get("anotherList")).get(1)));
    assertTrue(decryptor.isEncrypted((String) ((List<Object>) result.get("anotherList")).get(2)));

    // de-crypt again
    result = decryptor.decryptParams(result, "mySpaceId");

    String tmp = mapper.writeValueAsString(result);

    System.out.println("Decrypted params:");
    System.out.println(tmp);

    assertEquals(original, tmp);

    // check if decryption fails if spaceId does not match
    result = decryptor.decryptParams(mapper.readValue(encrypted, new TypeReference<Map<String, Object>>() {}), "invalidSpaceId");

    System.out.println("Decrypted params with invalid space ID:");
    System.out.println(mapper.writeValueAsString(result));

    assertEquals("invalid", result.get("secret"));
  }
}
