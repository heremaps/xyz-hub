/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import static org.junit.Assert.assertEquals;

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQueryOp;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.hub.rest.ApiParam.Query;
import org.junit.Test;

public class ApiParamTest {

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void parsePropertiesQuery() {
    String URIquery = "a=1&b=2&p.a=3&p.b>4.1&p.boolean=true&f.createdAt>0&p.testString=string,\"5\"";
    PropertiesQuery pq = Query.parsePropertiesQuery(URIquery, "", false);
    assertEquals("1 OR block is expected", 1, pq.size());

    PropertyQueryList pql = pq.get(0);
    assertEquals("5 AND blocks are expected.", 5, pql.size());

    PropertyQuery query = pql.stream().filter(q -> q.getKey().equals("properties.a")).findFirst().get();
    assertEquals(PropertyQueryOp.EQUALS, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));

    // properties.b
    query = pql.stream().filter(q -> q.getKey().equals("properties.b")).findFirst().get();
    assertEquals(PropertyQueryOp.GREATER_THAN, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(4.1d, query.getValues().get(0));

    // properties.boolean
    query = pql.stream().filter(q -> q.getKey().equals("properties.boolean")).findFirst().get();
    assertEquals(PropertyQueryOp.EQUALS, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(true, query.getValues().get(0));

    // createdAt
    query = pql.stream().filter(q -> q.getKey().equals("properties.@ns:com:here:xyz.createdAt")).findFirst().get();
    assertEquals(PropertyQueryOp.GREATER_THAN, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(0L, query.getValues().get(0));

    // testString
    query = pql.stream().filter(q -> q.getKey().equals("properties.testString")).findFirst().get();
    assertEquals(PropertyQueryOp.EQUALS, query.getOperation());
    assertEquals(2, query.getValues().size());
    assertEquals("string", query.getValues().get(0));
    assertEquals("5", query.getValues().get(1));

  }

  @Test
  public void parsePropertiesQuerySpace() {
    // equals
    String URISpaceQuery = "a=1&b=2&contentUpatedAt=3";
    PropertiesQuery pq = Query.parsePropertiesQuery(URISpaceQuery, "contentUpatedAt", true);
    assertEquals("1 OR block is expected", 1, pq.size());

    PropertyQueryList pql = pq.get(0);
    assertEquals("1 AND blocks are expected.", 1, pql.size());

    PropertyQuery query = pql.stream().filter(q -> q.getKey().equals("contentUpatedAt")).findFirst().get();
    assertEquals(PropertyQueryOp.EQUALS, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));

    // equals with OR
    URISpaceQuery = "a=1&b=2&contentUpatedAt=3,4";
    pq = Query.parsePropertiesQuery(URISpaceQuery, "contentUpatedAt", true);
    pql = pq.get(0);

    query = pql.stream().filter(q -> q.getKey().equals("contentUpatedAt")).findFirst().get();
    assertEquals(PropertyQueryOp.EQUALS, query.getOperation());
    assertEquals(2, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));
    assertEquals(4L, query.getValues().get(1));

    // not equals
    URISpaceQuery = "a=1&b=2&contentUpatedAt!=3";
    pq = Query.parsePropertiesQuery(URISpaceQuery, "contentUpatedAt", true);
    pql = pq.get(0);

    query = pql.stream().filter(q -> q.getKey().equals("contentUpatedAt")).findFirst().get();
    assertEquals(PropertyQueryOp.NOT_EQUALS, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));

    // greater
    URISpaceQuery = "a=1&b=2&contentUpatedAt>3";
    pq = Query.parsePropertiesQuery(URISpaceQuery, "contentUpatedAt", true);
    pql = pq.get(0);

    query = pql.stream().filter(q -> q.getKey().equals("contentUpatedAt")).findFirst().get();
    assertEquals(PropertyQueryOp.GREATER_THAN, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));

    // greater equals
    URISpaceQuery = "a=1&b=2&contentUpatedAt>=3";
    pq = Query.parsePropertiesQuery(URISpaceQuery, "contentUpatedAt", true);
    pql = pq.get(0);

    query = pql.stream().filter(q -> q.getKey().equals("contentUpatedAt")).findFirst().get();
    assertEquals(PropertyQueryOp.GREATER_THAN_OR_EQUALS, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));

    // less
    URISpaceQuery = "a=1&b=2&contentUpatedAt<3";
    pq = Query.parsePropertiesQuery(URISpaceQuery, "contentUpatedAt", true);
    pql = pq.get(0);

    query = pql.stream().filter(q -> q.getKey().equals("contentUpatedAt")).findFirst().get();
    assertEquals(PropertyQueryOp.LESS_THAN, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));

    // less equals
    URISpaceQuery = "a=1&b=2&contentUpatedAt<=3";
    pq = Query.parsePropertiesQuery(URISpaceQuery, "contentUpatedAt", true);
    pql = pq.get(0);

    query = pql.stream().filter(q -> q.getKey().equals("contentUpatedAt")).findFirst().get();
    assertEquals(PropertyQueryOp.LESS_THAN_OR_EQUALS, query.getOperation());
    assertEquals(1, query.getValues().size());
    assertEquals(3L, query.getValues().get(0));
  }
}
