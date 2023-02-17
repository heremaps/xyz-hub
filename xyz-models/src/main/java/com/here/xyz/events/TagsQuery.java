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

package com.here.xyz.events;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class TagsQuery extends ArrayList<TagList> {

  /**
   * Create the tags object from a query parameter.
   */
  @SuppressWarnings("unused")
  public static TagsQuery fromQueryParameter(String[] tagsQueryParam) {
    if (tagsQueryParam == null) {
      return new TagsQuery();
    }
    return fromQueryParameter(Arrays.asList(tagsQueryParam));

  }

  @SuppressWarnings("WeakerAccess")
  public static TagsQuery fromQueryParameter(List<String> tagsQueryParam) {
    TagsQuery result = new TagsQuery();

    if (tagsQueryParam == null || tagsQueryParam.size() == 0) {
      return result;
    }

    String operatorPlus = "-#:plus:#-";

    for (String s : tagsQueryParam) 
    {
     if (s == null || s.length() == 0) continue;

     try { s = URLDecoder.decode(s.replaceAll("\\+",operatorPlus), "utf-8"); }
     catch (UnsupportedEncodingException e) { e.printStackTrace(); }
   
     final String[] split = s.split( operatorPlus );
     result.add(new TagList(split));
    }

    return result;
  }

  public boolean containsWildcard() {
    for (TagList andTags : this) {
      if (andTags != null && andTags.size() == 1 && "*".equals(andTags.get(0))) {
        return true;
      }
    }

    return false;
  }
}
