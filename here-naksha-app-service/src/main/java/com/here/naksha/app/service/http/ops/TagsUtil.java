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
package com.here.naksha.app.service.http.ops;

import static com.here.naksha.common.http.apis.ApiParamsConst.TAGS;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.*;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.payload.events.QueryDelimiter;
import com.here.naksha.lib.core.models.payload.events.QueryParameter;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.util.ValueList;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TagsUtil {
  private static final int NONE = 0;
  private static final int OR = 1;
  private static final int AND = 2;

  private TagsUtil() {}

  /**
   * Function builds Property Operation (POp) based on "tags" supplied as API query parameter.
   * We iterate through all the tag combination values provided in the query param.
   *   For every tag combination concatenated with "," (COMMA) delimiter, we prepare OR list
   *   For every tag combination concatenated with "+" (PLUS) delimiter, we prepare AND list
   *   For any tag not part of any combination, we take it as part of OR condition
   *
   * So, for example if the input tag list is like this:
   *   tags = one
   *   tags = two,three
   *   tags = four+five
   *   tags = six,seven,eight+nine
   *   tags = ten+eleven,twelve,thirteen
   *   tags = fourteen
   *   Then, we generate
   * OR operation between:
   *   - one
   *   - (two OR three)
   *   - (four AND five)
   *   - (six OR seven)
   *   - (eight AND nine)
   *   - (ten AND eleven)
   *   - (twelve OR thirteen)
   *   - fourteen
   *
   * @param queryParams API query parameter from where "tags" needs to be extracted
   * @return POp property operation that can be used as part of {@link ReadRequest}
   */
  public static @Nullable POp buildOperationForTagsQueryParam(final @Nullable QueryParameterList queryParams) {
    if (queryParams == null) return null;
    QueryParameter tagParams = queryParams.get(TAGS);
    if (tagParams == null) return null;

    // global initialization
    final List<POp> globalOpList = new ArrayList<>();

    while (tagParams != null && tagParams.hasValues()) {
      // get list of all tag tokens and respective delimiters
      final ValueList tagTokenList = tagParams.values();
      final List<QueryDelimiter> delimList = tagParams.valuesDelimiter();
      // iterate through tag tokens and add them to OR / AND / Global list depending on delimiter
      // loop variable initialization
      int crtOp = NONE;
      List<String> orList = null;
      List<String> andList = null;
      int delimIdx = 0;
      for (final Object obj : tagTokenList) {
        if (obj == null) {
          if (crtOp == NONE) { // we skip null value if it is at the start of operation
            delimIdx++;
            continue;
          } else { // null value in middle of AND/OR operation not allowed
            throw new XyzErrorException(
                XyzError.ILLEGAL_ARGUMENT, "Empty tag not allowed - " + tagTokenList);
          }
        }
        final String tag = (String) obj;
        if (tag.isEmpty()) {
          if (crtOp == NONE) { // we skip empty value if it is at the start of operation
            delimIdx++;
            continue;
          } else { // empty value in middle of AND/OR operation not allowed
            throw new XyzErrorException(
                XyzError.ILLEGAL_ARGUMENT, "Empty tag not allowed - " + tagTokenList);
          }
        }
        final QueryDelimiter delimiter = delimList.get(delimIdx++);
        if (delimiter != AMPERSAND && delimiter != END && delimiter != COMMA && delimiter != PLUS) {
          throw new XyzErrorException(
              XyzError.ILLEGAL_ARGUMENT, "Invalid delimiter " + delimiter + " for parameter " + TAGS);
        }
        // is it start of new operation?
        if (crtOp == NONE) {
          if (delimiter == AMPERSAND || delimiter == END) {
            // this is the only tag. add this tag to global list straightaway
            addTagsToGlobalOpList(OR, globalOpList, tag);
          } else if (delimiter == COMMA) {
            // open OR operation and add this tag to OR list
            crtOp = OR;
            orList = addTagToList(orList, tag);
          } else if (delimiter == PLUS) {
            // open AND operation and add this tag to AND list
            crtOp = AND;
            andList = addTagToList(andList, tag);
          }
        }
        // is current ongoing operation OR?
        else if (crtOp == OR) {
          if (delimiter == AMPERSAND || delimiter == END) {
            // add this tag to OR list, add OR list to global list
            orList = addTagToList(orList, tag);
            addTagsToGlobalOpList(crtOp, globalOpList, orList.toArray(String[]::new));
            // and reset operation
            crtOp = NONE;
            orList = null;
            andList = null;
          } else if (delimiter == COMMA) {
            // add this tag to OR list
            orList = addTagToList(orList, tag);
          } else if (delimiter == PLUS) {
            // change of operation sequence. add crt OR list to global list. reset OR list
            addTagsToGlobalOpList(crtOp, globalOpList, orList.toArray(String[]::new));
            orList = null;
            // open new AND operation and add this tag to AND list
            crtOp = AND;
            andList = addTagToList(andList, tag);
          }
        }
        // current ongoing operation is AND
        else {
          if (delimiter == AMPERSAND || delimiter == END) {
            // add this tag to AND list, add AND list to global list
            andList = addTagToList(andList, tag);
            addTagsToGlobalOpList(crtOp, globalOpList, andList.toArray(String[]::new));
            // and reset operation
            crtOp = NONE;
            orList = null;
            andList = null;
          } else if (delimiter == PLUS) {
            // add this tag to AND list
            andList = addTagToList(andList, tag);
          } else if (delimiter == COMMA) {
            // change of operation sequence. add this tag to AND list. add AND list to global list
            andList = addTagToList(andList, tag);
            addTagsToGlobalOpList(crtOp, globalOpList, andList.toArray(String[]::new));
            // reset operation
            crtOp = NONE;
            orList = null;
            andList = null;
          }
        }
      }
      tagParams = tagParams.next();
    }

    // return single operation or OR list (in case of multiple operations)
    if (globalOpList.size() > 1) {
      return POp.or(globalOpList.toArray(POp[]::new));
    }
    return globalOpList.get(0);
  }

  private static @NotNull List<String> addTagToList(final @Nullable List<String> tagList, final @NotNull String tag) {
    final List<String> retList = (tagList == null) ? new ArrayList<>() : tagList;
    retList.add(tag);
    return retList;
  }

  private static void addTagsToGlobalOpList(int crtOp, final @NotNull List<POp> gList, String... tags) {
    if (tags == null || tags.length < 1) return;
    if (crtOp != OR && crtOp != AND) return;
    // Do we have only one tag? then use EXISTS operation
    if (tags.length == 1) {
      gList.add(POp.exists(PRef.tag(XyzNamespace.normalizeTag(tags[0]))));
      return;
    }
    // We have multiple tags, so use OR / AND operation
    final POp[] tagOpArr = new POp[tags.length];
    for (int i = 0; i < tags.length; i++) {
      tagOpArr[i] = POp.exists(PRef.tag(XyzNamespace.normalizeTag(tags[i])));
    }
    if (crtOp == OR) {
      gList.add(POp.or(tagOpArr));
    } else {
      gList.add(POp.and(tagOpArr));
    }
  }
}
