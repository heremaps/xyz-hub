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

package com.here.xyz.psql.query;

import static com.here.xyz.psql.query.branching.CommitManager.branchPathToTableChain;
import static com.here.xyz.responses.XyzError.CONFLICT;
import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.NOT_FOUND;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT_WITH_SEARCHABLE;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.FEATURE_NOT_EXISTS;
import static com.here.xyz.util.db.pg.SQLError.RETRYABLE_VERSION_CONFLICT;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.PARTITION_SIZE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzError;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import com.here.xyz.util.db.pg.SQLError;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WriteFeatures extends ExtendedSpace<WriteFeaturesEvent, FeatureCollection> {
  private static final Logger logger = LogManager.getLogger();
  boolean responseDataExpected;

  public WriteFeatures(WriteFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    responseDataExpected = event.isResponseDataExpected();
  }

  @Override
  protected SQLQuery buildQuery(WriteFeaturesEvent event) throws ErrorResponseException {
    List<String> tables = new ArrayList<>();
    List<Long> tableBaseVersions = null;
    SpaceContext spaceContext = null;
    String rootTableName = getDefaultTable(event);

    if (event.getNodeId() > 0) {
      tables.addAll(branchPathToTableChain(rootTableName, event.getBranchPath(), event.getNodeId()));
      tableBaseVersions = new ArrayList<>();
      tableBaseVersions.add(0l);
      tableBaseVersions.addAll(event.getBranchPath().stream().map(baseRef -> baseRef.getVersion()).toList());
    }
    else {
      if (isExtendedSpace(event)) {
        tables.add(getExtendedTable(event));
        if (is2LevelExtendedSpace(event))
          tables.add(getIntermediateTable(event));
        spaceContext = event.getContext();
      }
      tables.add(rootTableName);
    }

    FeatureWriterQueryContextBuilder queryContextBuilder = new FeatureWriterQueryContextBuilder()
        .withSchema(getSchema())
        .withTables(tables)
        .withTableBaseVersions(tableBaseVersions)
        .withSpaceContext(spaceContext)
        .withHistoryEnabled(event.getVersionsToKeep() > 1)
        .withBatchMode(true)
        .with("tableLayout", getTableLayout())
        .with("debug", "true".equals(System.getenv("FW_DEBUG")))
        .with("queryId", FunctionRuntime.getInstance().getStreamId())
        .with("PARTITION_SIZE", PARTITION_SIZE)
        .with("minVersion", event.getMinVersion())
        .with("versionsToKeep", event.getVersionsToKeep())
        .with("pw", getDataSourceProvider().getDatabaseSettings().getPassword());

    if (event.getRef() != null && event.getRef().isSingleVersion() && !event.getRef().isHead())
      queryContextBuilder.withBaseVersion(event.getRef().getVersion());

    if (getTableLayout() == NEW_LAYOUT || getTableLayout() == OLD_LAYOUT_WITH_SEARCHABLE) {

      Map<String, String> searchableProperties
              = new HashMap<>(java.util.Objects.requireNonNullElseGet(event.getSearchableProperties(), HashMap::new));

      if(getTableLayout() == NEW_LAYOUT && !searchableProperties.isEmpty()) {
        //Temporary workaround for NL connector
        searchableProperties.put("refQuad", "$.properties.refQuad");
        searchableProperties.put("globalVersion", "$.properties.globalVersion");
        //End of workaround
      }

      if (searchableProperties != null && !searchableProperties.isEmpty())
        queryContextBuilder.with("writeHooks", List.of(writeHook(searchableProperties)));
    }

    return new SQLQuery("SELECT write_features(#{modifications}, 'Modifications', #{author}, #{responseDataExpected});")
        .withLoggingEnabled(false)
        .withContext(queryContextBuilder.build())
        .withNamedParameter("modifications", XyzSerializable.serialize(event.getModifications()))
        .withNamedParameter("author", event.getAuthor())
        .withNamedParameter("responseDataExpected", event.isResponseDataExpected())
        .withRetryableErrorCodes(Set.of(RETRYABLE_VERSION_CONFLICT.errorCode));
  }

  @Override
  protected FeatureCollection run(DataSourceProvider dataSourceProvider) throws ErrorResponseException {
    try {
      return super.run(dataSourceProvider);
    }
    catch (SQLException e) {
      final String message = e.getMessage();
      String cleanMessage = message.contains("\n") ? message.substring(0, message.indexOf("\n")) : message;
      SQLError sqlError = SQLError.fromErrorCode(e.getSQLState());
      String details = message.contains("\n") && sqlError != FEATURE_EXISTS && sqlError != FEATURE_NOT_EXISTS
          ? message.substring(message.indexOf("\n") + 1)
          : null;
      throw switch (sqlError) {
        case VERSION_CONFLICT_ERROR -> {
          String[] detailLines = details.split("\n");
          yield new ErrorResponseException(CONFLICT, cleanMessage, e)
            .withInternalDetails(details)
            .withErrorResponseDetails(Map.of("hint", detailLines[1].trim()));
        }
        case FEATURE_EXISTS, MERGE_CONFLICT_ERROR, RETRYABLE_VERSION_CONFLICT -> new ErrorResponseException(CONFLICT, cleanMessage, e).withInternalDetails(details);
        case DUPLICATE_KEY -> new ErrorResponseException(CONFLICT, "Conflict while writing features.", e).withInternalDetails(details); //TODO: Handle all conflicts in FeatureWriter properly
        case FEATURE_NOT_EXISTS -> new ErrorResponseException(NOT_FOUND, cleanMessage, e).withInternalDetails(details);
        case ILLEGAL_ARGUMENT -> new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, cleanMessage, e).withInternalDetails(details);
        case XYZ_EXCEPTION, UNKNOWN -> new ErrorResponseException(EXCEPTION, e.getMessage(), e).withInternalDetails(details);
      };
    }
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    try {
      return responseDataExpected && rs.next()
          ? XyzSerializable.deserialize(rs.getString(1), FeatureCollection.class)
          : new FeatureCollection();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing query result.", e);
    }
  }

  public static String writeHook(Map<String, String> searchableProperties) {
    return """
        (feature, row) => {
          const searchableProperties = ${searchableProperties};
          let searchables = {};

          for (const alias in searchableProperties) {
            const sp = searchableProperties[alias];
            const idx = sp.lastIndexOf("::");
            let jsonPath = sp;
            let bScalar = true;
            if (idx !== -1) {
             jsonPath = sp.slice(0, idx);
             bScalar = ( sp.slice(idx + 2) === "scalar" );
            }

            try {
              if( bScalar )
               searchables[alias] = jsonpath_rfc9535.query(feature, jsonPath)[0];
              else
               searchables[alias] = jsonpath_rfc9535.query(feature, jsonPath);
            }
            catch (err) {
              throw new Error(`Error evaluating JSONPath for alias ${alias} and expression ${jsonPath} message: ${err.message}`);
            }
          }

          row.searchable = !row.searchable ? searchables : {...row.searchable, ...searchables};
        }
        """.replace("${searchableProperties}", XyzSerializable.serialize(searchableProperties));
  }
}
