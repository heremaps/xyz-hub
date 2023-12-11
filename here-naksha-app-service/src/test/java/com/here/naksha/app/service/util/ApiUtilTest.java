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
package com.here.naksha.app.service.util;

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.app.service.http.apis.ApiUtil;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ApiUtilTest {

  @Test
  public void testBuildOperationForTagsQueryParam() {
    final QueryParameterList params = new QueryParameterList("tags=one"
        + "&tags=two,three"
        + "&tags=four+five"
        + "&tags=six,seven,eight+nine"
        + "&tags=ten+eleven,twelve,thirteen"
        + "&tags=fourteen");
    final POp op = ApiUtil.buildOperationForTagsQueryParam(params);
    assertEquals(POpType.OR, op.op());
    final List<POp> orList = op.children();

    // ensure there are total 8 operations
    assertNotNull(orList, "Expected multiple OR operations");
    assertEquals(8, orList.size(), "Expected total 8 OR operations");
    int i = 0;
    POp crtOp = null;
    List<POp> innerOpList = null;
    // validate 1st operation uses EXISTS
    crtOp = orList.get(i++);
    assertEquals(POpType.EXISTS, crtOp.op());
    assertEquals("one", crtOp.getPropertyRef().getTagName());
    // validate 2nd operation uses OR
    crtOp = orList.get(i++);
    assertEquals(POpType.OR, crtOp.op());
    innerOpList = crtOp.children();
    assertNotNull(innerOpList, "Expected multiple operations");
    assertEquals(2, innerOpList.size());
    assertEquals(POpType.EXISTS, innerOpList.get(0).op());
    assertEquals("two", innerOpList.get(0).getPropertyRef().getTagName());
    assertEquals(POpType.EXISTS, innerOpList.get(1).op());
    assertEquals("three", innerOpList.get(1).getPropertyRef().getTagName());
    // validate 3rd operation uses AND
    crtOp = orList.get(i++);
    assertEquals(POpType.AND, crtOp.op());
    innerOpList = crtOp.children();
    assertNotNull(innerOpList, "Expected multiple operations");
    assertEquals(2, innerOpList.size());
    assertEquals(POpType.EXISTS, innerOpList.get(0).op());
    assertEquals("four", innerOpList.get(0).getPropertyRef().getTagName());
    assertEquals(POpType.EXISTS, innerOpList.get(1).op());
    assertEquals("five", innerOpList.get(1).getPropertyRef().getTagName());
    // validate 4th operation uses OR
    crtOp = orList.get(i++);
    assertEquals(POpType.OR, crtOp.op());
    innerOpList = crtOp.children();
    assertNotNull(innerOpList, "Expected multiple operations");
    assertEquals(2, innerOpList.size());
    assertEquals(POpType.EXISTS, innerOpList.get(0).op());
    assertEquals("six", innerOpList.get(0).getPropertyRef().getTagName());
    assertEquals(POpType.EXISTS, innerOpList.get(1).op());
    assertEquals("seven", innerOpList.get(1).getPropertyRef().getTagName());
    // validate 5th operation uses AND
    crtOp = orList.get(i++);
    assertEquals(POpType.AND, crtOp.op());
    innerOpList = crtOp.children();
    assertNotNull(innerOpList, "Expected multiple operations");
    assertEquals(2, innerOpList.size());
    assertEquals(POpType.EXISTS, innerOpList.get(0).op());
    assertEquals("eight", innerOpList.get(0).getPropertyRef().getTagName());
    assertEquals(POpType.EXISTS, innerOpList.get(1).op());
    assertEquals("nine", innerOpList.get(1).getPropertyRef().getTagName());
    // validate 6th operation uses AND
    crtOp = orList.get(i++);
    assertEquals(POpType.AND, crtOp.op());
    innerOpList = crtOp.children();
    assertNotNull(innerOpList, "Expected multiple operations");
    assertEquals(2, innerOpList.size());
    assertEquals(POpType.EXISTS, innerOpList.get(0).op());
    assertEquals("ten", innerOpList.get(0).getPropertyRef().getTagName());
    assertEquals(POpType.EXISTS, innerOpList.get(1).op());
    assertEquals("eleven", innerOpList.get(1).getPropertyRef().getTagName());
    // validate 7th operation uses OR
    crtOp = orList.get(i++);
    assertEquals(POpType.OR, crtOp.op());
    innerOpList = crtOp.children();
    assertNotNull(innerOpList, "Expected multiple operations");
    assertEquals(2, innerOpList.size());
    assertEquals(POpType.EXISTS, innerOpList.get(0).op());
    assertEquals("twelve", innerOpList.get(0).getPropertyRef().getTagName());
    assertEquals(POpType.EXISTS, innerOpList.get(1).op());
    assertEquals("thirteen", innerOpList.get(1).getPropertyRef().getTagName());
    // validate 8th operation uses EXISTS
    crtOp = orList.get(i++);
    assertEquals(POpType.EXISTS, crtOp.op());
    assertEquals("fourteen", crtOp.getPropertyRef().getTagName());
  }
}
