/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util.db.pg;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.util.db.SQLQuery;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureWriterQueryBuilder {

  private Object input;
  private String inputType;
  private String author;
  private boolean returnResult = false;
  private long version = -1;
  private UpdateStrategy updateStrategy = null;
  private boolean isPartial = false;
  private Map<String, Object> queryContext;
  private boolean select = false;

  public FeatureWriterQueryBuilder() {}

  public FeatureWriterQueryBuilder withInput(String input, String inputType) {
    this.input = input;
    this.inputType = inputType;
    return this;
  }

  public FeatureWriterQueryBuilder withInput(Modification input) {
    return withModifications(List.of(input));
  }

  public FeatureWriterQueryBuilder withModifications(List<Modification> input) {
    this.input = input;
    return this;
  }

  public FeatureWriterQueryBuilder withInput(FeatureCollection input) {
    this.input = input;
    return this;
  }

  public FeatureWriterQueryBuilder withInput(List<Feature> input) {
    this.input = input;
    return this;
  }

  public FeatureWriterQueryBuilder withInput(Feature input) {
    this.input = input;
    return this;
  }

  public FeatureWriterQueryBuilder withAuthor(String author) {
    this.author = author;
    return this;
  }

  public FeatureWriterQueryBuilder withReturnResult(boolean returnResult) {
    this.returnResult = returnResult;
    return this;
  }

  public FeatureWriterQueryBuilder withVersion(long version) {
    this.version = version;
    return this;
  }

  public FeatureWriterQueryBuilder withUpdateStrategy(UpdateStrategy updateStrategy) {
    this.updateStrategy = updateStrategy;
    return this;
  }

  public FeatureWriterQueryBuilder withIsPartial(boolean isPartial) {
    this.isPartial = isPartial;
    return this;
  }

  public FeatureWriterQueryBuilder withQueryContext(Map<String, Object> queryContext) {
    this.queryContext = queryContext;
    return this;
  }

  public FeatureWriterQueryBuilder withSelect(boolean select) {
    this.select = select;
    return this;
  }

  public SQLQuery build() {
    if (input == null)
      throw new IllegalArgumentException("No input has been set for the FeatureWriter.");

    if (input instanceof String && isNullOrEmpty(inputType))
      throw new IllegalArgumentException("No input type has been set for the FeatureWriter.");

    if (author == null)
      throw new IllegalArgumentException("No author has been set for the FeatureWriter.");

    if (queryContext == null)
      throw new IllegalArgumentException("No queryContext has been set for the FeatureWriter.");

    String selectTerm = select ? "SELECT " : "";

    if (input instanceof List) {
      List inputList = (List) input;
      if (!inputList.isEmpty() && inputList.get(0) instanceof Modification) {
        return new SQLQuery(selectTerm + "write_features(#{jsonInput}, 'Modifications', #{author}, #{returnResult}, #{version})")
            .withContext(queryContext)
            .withNamedParameter("jsonInput", XyzSerializable.serialize(inputList))
            .withNamedParameter("author", author)
            .withNamedParameter("returnResult", returnResult)
            .withNamedParameter("version", version <= 0 ? null : version);
      }
    }

    return new SQLQuery(selectTerm +
        "write_features(\n" +
        "  #{jsonInput}, #{inputType}, #{author}, #{returnResult}, #{version},\n" +
        "  #{onExists}, #{onNotExists}, #{onVersionConflict}, #{onMergeConflict}, #{isPartial}\n" +
        ")\n")
        .withContext(queryContext)
        .withNamedParameter("jsonInput", input instanceof String ? input : XyzSerializable.serialize(input))
        .withNamedParameter("inputType", input instanceof String ? inputType : input instanceof List ? "Features" : input.getClass().getSimpleName())
        .withNamedParameter("author", author)
        .withNamedParameter("returnResult", returnResult)
        .withNamedParameter("version", version <= 0 ? null : version)
        .withNamedParameter("onExists", updateStrategy == null ? null : updateStrategy.onExists())
        .withNamedParameter("onNotExists", updateStrategy == null ? null : updateStrategy.onNotExists())
        .withNamedParameter("onVersionConflict", updateStrategy == null ? null : updateStrategy.onVersionConflict())
        .withNamedParameter("onMergeConflict", updateStrategy == null ? null : updateStrategy.onMergeConflict())
        .withNamedParameter("isPartial", isPartial);
  }

  public static class FeatureWriterQueryContextBuilder {

    private Map<String, Object> queryContext = new HashMap<>();

    public FeatureWriterQueryContextBuilder withSchema(String schema) {
      queryContext.put("schema", schema);
      return this;
    }

    public FeatureWriterQueryContextBuilder withTable(String table) {
      return withTables(List.of(table));
    }

    public FeatureWriterQueryContextBuilder withTables(List<String> tables) {
      queryContext.put("tables", tables);
      return this;
    }

    public FeatureWriterQueryContextBuilder withTableBaseVersions(List<Long> tableBaseVersions) {
      queryContext.put("tableBaseVersions", tableBaseVersions);
      return this;
    }

    public FeatureWriterQueryContextBuilder withSpaceContext(SpaceContext spaceContext) {
      queryContext.put("context", spaceContext);
      return this;
    }

    public FeatureWriterQueryContextBuilder withHistoryEnabled(boolean historyEnabled) {
      queryContext.put("historyEnabled", historyEnabled);
      return this;
    }

    public FeatureWriterQueryContextBuilder withBaseVersion(long baseVersion) {
      queryContext.put("baseVersion", baseVersion);
      return this;
    }

    public FeatureWriterQueryContextBuilder withBatchMode(boolean batchMode) {
      queryContext.put("batchMode", batchMode);
      return this;
    }

    public FeatureWriterQueryContextBuilder with(String key, Object value) {
      queryContext.put(key, value);
      return this;
    }

    public Map<String, Object> build() {
      if (!queryContext.containsKey("schema"))
        throw new IllegalArgumentException("No schema has been set for the queryContext.");

      if (!queryContext.containsKey("table") && (!queryContext.containsKey("tables") || ((List<String>) queryContext.get("tables")).isEmpty()))
        throw new IllegalArgumentException("No table has been set for the queryContext.");

      return queryContext;
    }
  }
}
