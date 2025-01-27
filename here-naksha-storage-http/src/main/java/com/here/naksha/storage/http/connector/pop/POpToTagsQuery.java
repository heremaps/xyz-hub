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
package com.here.naksha.storage.http.connector.pop;

import static com.here.naksha.lib.core.models.storage.OpType.AND;
import static com.here.naksha.lib.core.models.storage.OpType.OR;
import static com.here.naksha.lib.core.models.storage.POpType.EXISTS;

import com.here.naksha.lib.core.models.payload.events.TagList;
import com.here.naksha.lib.core.models.payload.events.TagsQuery;
import com.here.naksha.lib.core.models.storage.POp;
import java.util.List;

class POpToTagsQuery {
  static TagsQuery toTagsQuery(POp tagPOp) {
    if (tagPOp.op() == OR) {
      TagsQuery tagsQuery = new TagsQuery();
      for (POp child : tagPOp.children()) {
        if (child.op() == AND) {
          tagsQuery.add(tagListOf(child.children()));
        } else if (child.op() == EXISTS) {
          tagsQuery.add(tagListOf(child));
        } else {
          throw new IllegalArgumentException("Invalid op for Tags POp : " + child.op() + " inside AND");
        }
      }
      return tagsQuery;
    } else if (tagPOp.op() == AND) {
      TagsQuery tagsQuery = new TagsQuery();
      tagsQuery.add(tagListOf(tagPOp.children()));
      return tagsQuery;
    } else if (tagPOp.op() == EXISTS) {
      TagsQuery tagsQuery = new TagsQuery();
      tagsQuery.add(tagListOf(tagPOp));
      return tagsQuery;
    } else {
      throw new IllegalArgumentException("Invalid op for Tags POp: " + tagPOp.op());
    }
  }

  private static TagList tagListOf(POp tagPOp) {
    TagList tagList = new TagList();
    tagList.add(tagNameFrom(tagPOp));
    return tagList;
  }

  private static TagList tagListOf(List<POp> orCombinedPOps) {
    TagList tagList = new TagList();
    orCombinedPOps.forEach(pOp -> tagList.add(tagNameFrom(pOp)));
    return tagList;
  }

  private static String tagNameFrom(POp tagPOp) {
    return tagPOp.getPropertyRef().getTagName();
  }
}
