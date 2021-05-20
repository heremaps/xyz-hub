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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.auth.XyzHubActionMatrix.ADMIN_SPACES;
import static com.here.xyz.hub.auth.XyzHubActionMatrix.CREATE_FEATURES;
import static com.here.xyz.hub.auth.XyzHubActionMatrix.DELETE_FEATURES;
import static com.here.xyz.hub.auth.XyzHubActionMatrix.MANAGE_SPACES;
import static com.here.xyz.hub.auth.XyzHubActionMatrix.READ_FEATURES;
import static com.here.xyz.hub.auth.XyzHubActionMatrix.UPDATE_FEATURES;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.AttributeMap;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.config.SpaceConfigClient.SpaceSelectionCondition;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.connectors.models.Space.SpaceWithRights;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.FeatureTask.ModifySpaceQuery;
import com.here.xyz.hub.task.ModifyOp.Entry;
import com.here.xyz.hub.task.ModifyOp.ModifyOpError;
import com.here.xyz.hub.task.SpaceTask.ConditionalOperation;
import com.here.xyz.hub.task.SpaceTask.ReadQuery;
import com.here.xyz.hub.task.SpaceTask.View;
import com.here.xyz.hub.task.TaskPipeline.C1;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.models.hub.Space.ConnectorRef;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SpaceTaskHandler {

  private static final Logger logger = LogManager.getLogger();

  private static final int CLIENT_VALUE_MAX_SIZE = 1024;

  static <X extends ReadQuery<?>> void readSpaces(final X task, final Callback<X> callback) {
    Service.spaceConfigClient.getSelected(task.getMarker(), task.authorizedCondition, task.selectedCondition, task.propertiesQuery)
        .onFailure(t -> {
          logger.error(task.getMarker(), "Unable to load space definitions.'", t);
          callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", t));
        })
        .onSuccess(spaces -> {
          task.responseSpaces = spaces;
          callback.call(task);
        });
  }

  static <X extends ReadQuery<?>> void readFromJWT(final X task, final Callback<X> callback) {
    final List<String> authorizedListSpacesOps = Arrays
        .asList(ADMIN_SPACES, MANAGE_SPACES, READ_FEATURES, CREATE_FEATURES, UPDATE_FEATURES, DELETE_FEATURES);
    final ActionMatrix tokenRights = task.getJwt().getXyzHubMatrix();

    task.authorizedCondition = new SpaceSelectionCondition();
    task.authorizedCondition.spaceIds = new HashSet<>();
    task.authorizedCondition.ownerIds = new HashSet<>();
    task.authorizedCondition.packages = new HashSet<>();

    final Supplier<Stream<AttributeMap>> sup = () -> tokenRights
        .entrySet()
        .stream()
        .filter(e -> authorizedListSpacesOps.contains(e.getKey()))
        .flatMap(e -> e.getValue().stream());

    boolean readAll = sup.get().anyMatch(HashMap::isEmpty);
    if (!readAll) {
      sup.get().forEach(am -> {
            if (am.get("space") instanceof String) {
              String spaceId = (String) am.get("space");
              task.authorizedCondition.spaceIds.add(spaceId);
            }
            //A filter without space ID, but with an owner.
            else if (am.get("owner") instanceof String) {
              String ownerId = (String) am.get("owner");
              task.authorizedCondition.ownerIds.add(ownerId);
            }
            //A filter for packages.
            else if (am.get("packages") instanceof String) {
              String packages = (String) am.get("packages");
              task.authorizedCondition.packages.add(packages);
            }
          }
      );
    }

    callback.call(task);
  }

  public static void preprocess(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    JsonObject input = new JsonObject(task.modifyOp.entries.get(0).input);
    input.remove("createdAt");
    input.remove("updatedAt");

    if (task.modifyOp.isCreate()) {
      String owner = task.getJwt().aid;
      String cid = task.getJwt().cid;
      task.template = getSpaceTemplate(owner, cid);

      //When the input explicitly contains {"owner": null}
      if (input.getString("owner") == null) {
        input.remove("owner");
      }

      JsonObject.mapFrom(task.template).stream()
          .filter(e -> !input.containsKey(e.getKey()))
          .forEach(e -> input.put(e.getKey(), e.getValue()));
    }
    callback.call(task);
  }

  static void processModifyOp(ConditionalOperation task, Callback<ConditionalOperation> callback) throws Exception {
    try {
      task.modifyOp.process();
      callback.call(task);
    }
    catch (ModifyOpError e) {
      logger.info(task.getMarker(), "ConditionalOperationError: {}", e.getMessage(), e);
      throw new HttpException(CONFLICT, e.getMessage());
    }
  }

  public static void validate(ConditionalOperation task, Callback<ConditionalOperation> callback) throws Exception {
    if (task.isDelete()) {
      callback.call(task);
      return;
    }

    Space space = task.modifyOp.entries.get(0).result;

    if (task.isUpdate()) {
      /**
       * Validate immutable settings which are only can get set during the space creation:
       * enableUUID, enableHistory, enableGlobalVersioning, maxVersionCount
       * */
      Space spaceHead = task.modifyOp.entries.get(0).head;

      if(spaceHead != null && spaceHead.isEnableUUID() == Boolean.TRUE && task.modifyOp.entries.get(0).input.get("enableUUID") == Boolean.TRUE )
        task.modifyOp.entries.get(0).input.put("enableUUID",true);
      else if(spaceHead != null && task.modifyOp.entries.get(0).input.get("enableUUID") != null )
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'enableUUID' can only get set on space creation!");

      /** enableHistory is immutable and it is only allowed to set it during the space creation */
      if(spaceHead != null && spaceHead.isEnableHistory() == Boolean.TRUE && task.modifyOp.entries.get(0).input.get("enableHistory") == Boolean.TRUE )
        task.modifyOp.entries.get(0).input.put("enableHistory",true);
      else if(spaceHead != null && task.modifyOp.entries.get(0).input.get("enableHistory") != null )
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'enableHistory' can only get set on space creation!");

      /** enableGlobalVersioning is immutable and it is only allowed to set it during the space creation */
      if(spaceHead != null && spaceHead.isEnableGlobalVersioning() == Boolean.TRUE && task.modifyOp.entries.get(0).input.get("enableGlobalVersioning") == Boolean.TRUE )
        task.modifyOp.entries.get(0).input.put("enableGlobalVersioning",true);
      else if(spaceHead != null && task.modifyOp.entries.get(0).input.get("enableGlobalVersioning") != null )
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'enableGlobalVersioning' can only get set on space creation!");

      /** getMaxVersionCount is immutable and it is only allowed to set it during the space creation */
      if(spaceHead != null && spaceHead.isEnableGlobalVersioning() && spaceHead.getMaxVersionCount() != null && task.modifyOp.entries.get(0).input.get("maxVersionCount") != null
              && (spaceHead.getMaxVersionCount().compareTo((Integer)task.modifyOp.entries.get(0).input.get("maxVersionCount")) == 0))
        task.modifyOp.entries.get(0).input.put("maxVersionCount" , spaceHead.getMaxVersionCount());
      else if(spaceHead != null && spaceHead.isEnableGlobalVersioning() && task.modifyOp.entries.get(0).input.get("maxVersionCount") != null )
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'maxVersionCount' can only get set, in combination of enableGlobalVersioning, on space creation!");
    }

    if (task.isCreate()) {
      //Automatic activation of enableHistory in case of enableGlobalVersioning
      if(space.isEnableGlobalVersioning() && !space.isEnableHistory())
        task.modifyOp.entries.get(0).result.setEnableHistory(true);
      //Automatic activation of UUID in case of enableHistory
      if(space.isEnableHistory() && !space.isEnableUUID())
        task.modifyOp.entries.get(0).result.setEnableUUID(true);
    }

    if(space.getMaxVersionCount() != null){
      if(!space.isEnableHistory() && !space.isEnableGlobalVersioning())
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'maxVersionCount' can only get set if 'enableHistory' is set.");
      if(space.getMaxVersionCount() < -1)
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'maxVersionCount' must be greater or equal to -1.");
    }

    if (space.getId() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'id' cannot be empty.");
    }
    if (space.getOwner() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'owner' cannot be empty.");
    }
    if (space.getStorage() == null || space.getStorage().getId() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The storage ID cannot be empty.");
    }
    if (space.getTitle() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'title' cannot be empty.");
    }
    if (space.getClient() != null && Json.encode(space.getClient()).getBytes().length > CLIENT_VALUE_MAX_SIZE) {
      throw new HttpException(
          BAD_REQUEST, "The property client is over the allowed limit of " + CLIENT_VALUE_MAX_SIZE + " bytes.");
    }
    if (space.getSearchableProperties() != null && !space.getSearchableProperties().isEmpty()) {
      Space.resolveConnector(task.getMarker(), space.getStorage().getId(), arConnector -> {
        if (arConnector.failed()) {
          callback.exception(new Exception(arConnector.cause()));
        } else {
          Connector connector = arConnector.result();
          if (!connector.capabilities.searchablePropertiesConfiguration) {
            callback.exception(new HttpException(BAD_REQUEST, "It's not supported to define the searchableProperties"
                + " on space with storage connector " + space.getStorage().getId()));
          } else {
            callback.call(task);
          }
        }
      });
    } else {
      callback.call(task);
    }
  }

  /**
   * Load the space definition and its corresponding realm for an space conditional operation.
   *
   * @param task the task for which we are called.
   * @param callback the callback to be invoked when done.
   */
  static void loadSpace(final ConditionalOperation task, final Callback<ConditionalOperation> callback) {
    final Map<String,Object> inputSpace = task.modifyOp.entries.get(0).input;
    final Object spaceId = inputSpace.get("id");
    if (!(spaceId instanceof String)) {
      callback.call(task);
      return;
    }

    Service.spaceConfigClient.get(task.getMarker(), (String)spaceId)
        .onFailure(t -> callback.exception(t))
        .onSuccess(headSpace -> {
          task.modifyOp.entries.get(0).head = headSpace;
          task.modifyOp.entries.get(0).base = headSpace;
          callback.call(task);
        });
  }

  static void modifySpaces(final ConditionalOperation task, final Callback<ConditionalOperation> callback) {
    final Entry<Space> entry = task.modifyOp.entries.get(0);
    if (!entry.isModified) {
      task.responseSpaces = Collections.singletonList(entry.result);
      callback.call(task);
      return;
    }

    if (entry.input != null && entry.result == null)
      Service.spaceConfigClient
          .delete(task.getMarker(), entry.head.getId())
          .onFailure(t -> callback.exception(t))
          .onSuccess(v -> {
            task.responseSpaces = Collections.singletonList(task.modifyOp.entries.get(0).head);
            callback.call(task);
          });
    else
      Service.spaceConfigClient
          .store(task.getMarker(), entry.result)
          .onFailure(t -> callback.exception(t))
          .onSuccess(v -> {
            task.responseSpaces = Collections.singletonList(entry.result);
            callback.call(task);
          });
  }

  private static Space getSpaceTemplate(String owner, String cid) {
    Space space = new Space();
    space.setId(RandomStringUtils.randomAlphanumeric(8));
    space.setOwner(owner);
    space.setCid(cid);
    space.setEnableUUID(false);
    space.setClient(null);
    space.setStorage(new ConnectorRef().withId(Service.configuration.DEFAULT_STORAGE_ID));
    return space;
  }

  static <X extends SpaceTask<X>> void convertResponse(X task, Callback<X> callback) {
    if (task.canReadAdminProperties) {
      task.view = View.FULL;
    } else if (task.canReadConnectorsProperties) {
      task.view = View.CONNECTOR_RIGHTS;
    }

    if (!View.BASIC_RIGHTS.equals(task.view)) {
      callback.call(task);
      return;
    }

    List<String> operations = Arrays
        .asList("readFeatures", "createFeatures", "updateFeatures", "deleteFeatures", "manageSpaces", "adminSpaces");

    final ActionMatrix accessMatrix = task.getJwt().getXyzHubMatrix();

    task.responseSpaces = task.responseSpaces.stream().map(g -> {
          final SpaceWithRights space = DatabindCodec.mapper().convertValue(g, SpaceWithRights.class);
          space.rights = new ArrayList<>();
          for (String op : operations) {
            final ActionMatrix readAccessMatrix = new ActionMatrix()
                .addAction(op, new AttributeMap().withValue("owner", g.getOwner()).withValue("space", g.getId())
                    .withValue("packages", g.getPackages()));
            if (accessMatrix.matches(readAccessMatrix)) {
              space.rights.add(op);
            }
          }
          return space;
        }
    ).collect(Collectors.toList());
    callback.call(task);
  }

  static void enforceUsageQuotas(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    if (!task.isCreate()) {
      callback.call(task);
      return;
    }

    JWTPayload jwt = task.getJwt();
    if (jwt.limits == null || jwt.limits.maxSpaces < 0) {
      callback.call(task);
      return;
    }

    Service.spaceConfigClient.getSpacesForOwner(task.getMarker(), jwt.aid)
        .onFailure(t -> {
          logger.warn(task.getMarker(), "Unable to load the space definitions.", t);
          callback.exception(new HttpException(BAD_GATEWAY, "Unable to load the resource definitions.", t));
        })
        .onSuccess(spaces -> {
          if (spaces.size() >= jwt.limits.maxSpaces)
            callback.exception(new HttpException(FORBIDDEN, "The maximum number of " + jwt.limits.maxSpaces + " spaces was reached."));
          else
            callback.call(task);
        });
  }

  static void sendEvents(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    if (!task.isCreate() && !task.isUpdate() && !task.isDelete()) {
      callback.call(task);
      return;
    }

    Operation op = task.isCreate() ? Operation.CREATE : task.isUpdate() ? Operation.UPDATE : Operation.DELETE;
    Entry<Space> entry = task.modifyOp.entries.get(0);

    Space space = task.isDelete() ? entry.head : entry.result;

    if (task.isDelete() && space.notSendDeleteMse) {
      callback.call(task);
      return;
    }

    final ModifySpaceEvent event = new ModifySpaceEvent()
        .withOperation(op)
        .withSpaceDefinition(space)
        .withStreamId(task.getMarker().getName())
        .withParams(space.getStorage().getParams())
        .withIfNoneMatch(task.context.request().headers().get("If-None-Match"))
        .withSpace(space.getId());

    ModifySpaceQuery query = new ModifySpaceQuery(event, task.context, ApiResponseType.EMPTY);
    query.space = space;
    event.setSpace(space.getId());

    C1<ModifySpaceQuery> onEventProcessed = (t) -> {
      //Currently it's not supported that the connector changes the space modification operation
      if (query.manipulatedOp != null && query.manipulatedOp != op) {
        throw new HttpException(BAD_GATEWAY, "Connector error.");
      }
      if ((task.isCreate() || task.isUpdate()) && query.manipulatedSpaceDefinition != null) {
        //Treat the manipulated space definition as a partial update.
        Map<String,Object> newInput = JsonObject.mapFrom(query.manipulatedSpaceDefinition).getMap();
        Map<String,Object> resultClone = entry.result.asMap();
        final Difference difference = Patcher.calculateDifferenceOfPartialUpdate(resultClone, newInput, null, true);
        if (difference != null) {
          entry.isModified = true;
          Patcher.patch(resultClone, difference);
          entry.result = DatabindCodec.mapper().readValue(Json.encode(resultClone), Space.class);
        }
      }

      callback.call(task);
    };

    //Send "ModifySpaceEvent" to (all) the connector(s) to do some setup, update or clean up.
    query.execute(onEventProcessed, (t, e) -> callback.exception(e));
  }

  public static void postProcess(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    //Executes the timestamp-ing only for create and update operations
    if (task.isCreate() || task.isUpdate()) {

      //Create and update operations, timestamp is always set into updatedAt field.
      final Entry<Space> entry = task.modifyOp.entries.get(0);

      //The current UTC timestamp
      entry.result.setUpdatedAt(Core.currentTimeMillis());

      //The same timestamp goes to createdAt when it's a create task
      if (task.isCreate()) {
        entry.result.setCreatedAt(entry.result.getUpdatedAt());
      }
      else if (task.isUpdate()) {
        // Do not allow updating the createdAt value
        entry.result.setCreatedAt(entry.head.getCreatedAt());

        // Do not allow removing the owner property
        if (entry.result.getOwner() == null) {
          entry.result.setOwner(entry.head.getOwner());
        }

        // Do not allow removing the cid property
        if (entry.result.getCid() == null) {
          entry.result.setCid(entry.head.getCid());
        }
      }
    }

    //Resume
    callback.call(task);
  }
}
