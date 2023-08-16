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

import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.AttributeMap;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.config.SpaceConfigClient.SpaceSelectionCondition;
import com.here.xyz.hub.config.TagConfigClient;
import com.here.xyz.hub.config.settings.SpaceStorageMatchingMap;
import com.here.xyz.hub.connectors.RpcClient;
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
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class SpaceTaskHandler {

  private static final Logger logger = LogManager.getLogger();
  private static final int CLIENT_VALUE_MAX_SIZE = 1024;

  static <X extends ReadQuery<?>> void readSpaces(final X task, final Callback<X> callback) {
    Future<List<Tag>> tagsFuture = StringUtils.isBlank(task.selectedCondition.tagId) ?
        Future.succeededFuture(Collections.emptyList()) :
        Service.tagConfigClient.getTagsByTagId(task.getMarker(), task.selectedCondition.tagId);

    tagsFuture
        .flatMap(tags -> {
          if (task.selectedCondition.tagId != null) {
            if (tags.isEmpty())
              return Future.succeededFuture(Collections.emptyList());

            task.selectedCondition.ownerIds = Collections.emptySet();
            task.selectedCondition.spaceIds = tags.stream().map(Tag::getSpaceId).collect(Collectors.toSet());
          }

          return Service.spaceConfigClient.getSelected(task.getMarker(), task.authorizedCondition, task.selectedCondition, task.propertiesQuery)
              .flatMap(spaces -> augmentWithTags(spaces, tags));
        })
        .onFailure(t -> {
          logger.error(task.getMarker(), "Unable to load space definitions.'", t);
          callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", t));
        })
        .onSuccess(spaces -> {
          task.responseSpaces = spaces;
          callback.call(task);
        });
  }

  static Future<List<Space>> augmentWithTags(List<Space> spaces, List<Tag> tags) {
    final Map<String, HashMap<String, Tag>> tagsMap = tags.stream().reduce(new HashMap<>(), (map, tag) -> {
      map.putIfAbsent(tag.getSpaceId(), new HashMap<>());
      map.get(tag.getSpaceId()).put(tag.getId(), tag);

      return map;
    }, (map1, map2) -> {
      map2.forEach((k, v) -> {
        map1.putIfAbsent(k, new HashMap<>());
        map1.get(k).putAll(v);
      });

      return map1;
    });

    return Future.succeededFuture(
        spaces
        .stream()
        .peek(space -> space.setTags(tagsMap.get(space.getId())))
        .collect(Collectors.toList())
    );
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

      String storageId = task.template.getStorage().getId();
      logger.info(task.getMarker(), "storageId from space template: " + storageId);

      if (input.getString("region") != null) {
        storageId = Service.configuration.getDefaultStorageId(input.getString("region"));
        logger.info(task.getMarker(), "default storageId from region " + input.getString("region") + ": " + storageId);

        if (input.getString("id") != null) {
          String matchedStorageId = SpaceStorageMatchingMap.getIfMatches(input.getString("id"), input.getString("region"));
          logger.info(task.getMarker(), "storageId from space/region/storage mapping: " + matchedStorageId);
          if (matchedStorageId != null) storageId = matchedStorageId;
        }

        if (storageId == null) {
          callback.exception(new HttpException(BAD_REQUEST, "No storage is available for the specified region."));
          return;
        }
      }

      task.template.setStorage(new ConnectorRef().withId(storageId));

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
    if (task.isDelete() || task.isRead()) {
      callback.call(task);
      return;
    }

    Space result = task.modifyOp.entries.get(0).result;
    Map<String, Object> input = task.modifyOp.entries.get(0).input;

    if (task.isUpdate()) {
      /*
       * Validate immutable settings which are only can get set during the space creation:
       * enableUUID, extension
       * */
      Space head = task.modifyOp.entries.get(0).head;

      if (head != null && head.isEnableUUID() == Boolean.TRUE && input.get("enableUUID") == Boolean.TRUE )
        input.put("enableUUID",true);
      else if (head != null && input.get("enableUUID") != null )
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'enableUUID' can only get set on space creation!");

      if (head != null && head.getVersionsToKeep() > 1 && input.get("versionsToKeep") != null && Objects.equals(1, input.get("versionsToKeep")))
        throw new HttpException(BAD_REQUEST, "Validation failed. The property 'versionsToKeep' cannot be changed to 1 when its value is bigger than 1");

      if (head != null && head.getVersionsToKeep() >= 1 && input.get("versionsToKeep") != null && Objects.equals(0, input.get("versionsToKeep")))
        throw new HttpException(BAD_REQUEST, "Validation failed. The property \"versionsToKeep\" cannot be set to zero");
    }

    if (task.isCreate()) {
      if (result.getVersionsToKeep() == 0)
        throw new HttpException(BAD_REQUEST, "Validation failed. The property \"versionsToKeep\" cannot be set to 0");
    }

    if (result.getId() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'id' cannot be empty.");
    }

    if (result.getOwner() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'owner' cannot be empty.");
    }

    if (result.getStorage() == null || result.getStorage().getId() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The storage ID cannot be empty.");
    }

    if (result.getTitle() == null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'title' cannot be empty.");
    }

    if (result.getClient() != null && Json.encode(result.getClient()).getBytes().length > CLIENT_VALUE_MAX_SIZE) {
      throw new HttpException(BAD_REQUEST, "The property client is over the allowed limit of " + CLIENT_VALUE_MAX_SIZE + " bytes.");
    }

    if (result.getVersionsToKeep() < 0 || result.getVersionsToKeep() > Service.configuration.MAX_VERSIONS_TO_KEEP) {
      throw new HttpException(BAD_REQUEST, "The property versionsToKeep must be equals to zero or a positive integer. Max value is " + Service.configuration.MAX_VERSIONS_TO_KEEP);
    }

    if (result.getExtension() != null && result.getSearchableProperties() != null) {
      throw new HttpException(BAD_REQUEST, "Validation failed. The properties 'searchableProperties' and 'extends' cannot be set together.");
    }

    if (task.modifyOp.entries.get(0).result.getExtension() != null) {
      // in case of create, the storage must be not set from the user (or set exactly as if the storage property in space template)
      // in case of update, the property storage is checked for modification against the head value
      Space inputSpace = task.isUpdate() ? task.modifyOp.entries.get(0).head : task.template;
      Space resultSpace = task.modifyOp.entries.get(0).result;

      Map<String, Object> inputStorage = (Map<String, Object>) inputSpace.asMap().get("storage");
      Map<String, Object> resultStorage = (Map<String, Object>) resultSpace.asMap().get("storage");

      // normalize params in case of null, so the diff calculator considers: null params == empty params
      // normalization is necessary because params from head or result are always a map, unless the user specifies params: null and params from space template is null
      if (inputStorage.get("params") == null)
        inputStorage.put("params", new HashMap<>());

      if (resultStorage.get("params") == null)
        resultStorage.put("params", new HashMap<>());

      // if there is any modification, means the user tried to submit 'storage' and 'extends' properties together
      if (Patcher.getDifference(inputStorage, resultStorage) != null) {
        throw new HttpException(BAD_REQUEST, "Validation failed. The properties 'storage' and 'extends' cannot be set together.");
      }
    }

    if (result.getSearchableProperties() != null && !result.getSearchableProperties().isEmpty() || result.getExtension() != null) {
      Space.resolveConnector(task.getMarker(), result.getStorage().getId(), arConnector -> {
        if (arConnector.failed()) {
          callback.exception(new Exception(arConnector.cause()));
        } else {
          final Connector connector = arConnector.result();
          if (result.getSearchableProperties() != null && !result.getSearchableProperties().isEmpty()
              && !connector.capabilities.searchablePropertiesConfiguration) {
            callback.exception(new HttpException(BAD_REQUEST, "It's not supported to define the searchableProperties"
                + " on space with storage connector " + result.getStorage().getId()));
          } else if (result.getExtension() != null && !connector.capabilities.extensionSupport) {
            callback.exception(new HttpException(BAD_REQUEST, "The space " + result.getId() + " cannot extend the space "
                + result.getExtension().getSpaceId() + " because its storage does not have the capability 'extensionSupport'."));
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
    Space space = new Space()
        .withRegion(Service.configuration.AWS_REGION);
    space.setId(RandomStringUtils.randomAlphanumeric(8));
    space.setOwner(owner);
    space.setCid(cid);
    space.setEnableUUID(false);
    space.setClient(null);
    space.setStorage(new ConnectorRef().withId(Service.configuration.getDefaultStorageId()));
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

  static void resolveExtensions(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    if (!task.isCreate() && !task.isUpdate()) {
      callback.call(task);
      return;
    }

    Space space = task.modifyOp.entries.get(0).result;

    if (space.getExtension() == null) {
      callback.call(task);
      return;
    }

    //Load the space being extended
    Space.resolveSpace(task.getMarker(), space.getExtension().getSpaceId())
      .onFailure(t -> onExtensionResolveError(task, t, callback))
      .onSuccess(extendedSpace -> {
        // check for existing space to be extended
        if (extendedSpace == null) {
          callback.exception(new HttpException(BAD_REQUEST, "The space " + space.getId() + " cannot extend the space "
              + space.getExtension().getSpaceId() + " because it does not exist."));
          return;
        }

        ConnectorRef extendedConnector = extendedSpace.getStorage();
        if (task.isCreate()) {
          //Override the storage config by copying it from the extended space
          space.setStorage(new ConnectorRef()
              .withId(extendedConnector.getId())
              .withParams(extendedConnector.getParams() != null ? extendedConnector.getParams() : new HashMap<>()));
        }
        else if (!Objects.equals(space.getStorage().getId(), extendedConnector.getId())) {
          callback.exception(new HttpException(BAD_REQUEST, "The storage of space " + space.getId()
              + " [storage: " + space.getStorage().getId() + "] is not matching the storage of the space to be extended "
              + "(" + extendedSpace.getId() + " [storage: " + extendedConnector.getId() + "])."));
          return;
        }

        task.resolvedExtensions = space.resolveCompositeParams(extendedSpace);

        //Check for extensions with more than 2 levels
        //((Map<String,Map<String, Object>>) extendedSpace.getStorage().getParams().get("extends")).containsKey("extends")
        if (extendedSpace.getExtension() != null) {
          Space.resolveSpace(task.getMarker(), extendedSpace.getExtension().getSpaceId())
              .onFailure(t -> onExtensionResolveError(task, t, callback))
              .onSuccess(secondLvlExtendedSpace -> {
                if (secondLvlExtendedSpace == null)
                  onExtensionResolveError(task, new Exception("Unexpected error: Extension of extended space could not be found."), callback);

                Map<String, Object> resolvedSecondLvlExtensions = extendedSpace.resolveCompositeParams(secondLvlExtendedSpace);
                if (((Map<String, Object>) resolvedSecondLvlExtensions.get("extends")).containsKey("extends"))
                  callback.exception(new HttpException(BAD_REQUEST, "The space " + space.getId() + " cannot extend the space "
                      + extendedSpace.getId() + " because the maximum extension level is 2."));
                else
                  callback.call(task);
              });
        }
        else
          callback.call(task);
      });
  }

  private static void onExtensionResolveError(ConditionalOperation task, Throwable error, Callback<ConditionalOperation> callback) {
    String errMsg = "Error during resolving extensions.";
    logger.error(task.getMarker(), errMsg, error);
    callback.exception(new HttpException(INTERNAL_SERVER_ERROR, errMsg, error));
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

    Map<String, Object> storageParams = new HashMap<>();
    if (space.getStorage().getParams() != null)
      storageParams.putAll(space.getStorage().getParams());
    //Inject the extension-map
    if (task.resolvedExtensions != null)
      storageParams.putAll(task.resolvedExtensions);

    final ModifySpaceEvent event = new ModifySpaceEvent()
        .withOperation(op)
        .withSpaceDefinition(space)
        .withStreamId(task.getMarker().getName())
        .withParams(storageParams)
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

  public static void handleReadOnlyUpdate(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    final Entry<Space> entry = task.modifyOp.entries.get(0);
    //If the readOnly flag was activated directly at space creation ...
    if (task.isCreate() && entry.result.isReadOnly()) {
      //Set the readOnlyHeadVersion on the space
      entry.result.setReadOnlyHeadVersion(0);
      callback.call(task);
    }
    //If the readOnly flag was changed ...
    else if (task.isUpdate() && entry.result.isReadOnly() != entry.head.isReadOnly()) {
      //... to active ...
      if (entry.result.isReadOnly())
        //... update the readOnlyHeadVersion on the space object
        updateReadOnlyHeadVersion(task.getMarker(), entry.result)
            .onSuccess(r -> callback.call(task))
            .onFailure(t -> callback.exception(t));
      else {
        //... if it was set to inactive, reset the readOnlyHeadVersion
        entry.result.setReadOnlyHeadVersion(-1);
        callback.call(task);
      }
    }
    else
      callback.call(task);
  }

  private static Future<Void> updateReadOnlyHeadVersion(Marker marker, Space space) {
    GetChangesetStatisticsEvent event = new GetChangesetStatisticsEvent().withSpace(space.getId());
    Promise<Void> p = Promise.promise();
    Space.resolveConnector(marker, space.getStorage().getId())
        .onSuccess(connector -> RpcClient.getInstanceFor(connector).execute(marker, event, ar -> {
          ChangesetsStatisticsResponse response = (ChangesetsStatisticsResponse) ar.result();
          space.setReadOnlyHeadVersion(response.getMaxVersion());
          p.complete();
        }))
        .onFailure(t -> p.fail(t));
    return p.future();
  }

    public static void cleanDependentResources(ConditionalOperation task, Callback<ConditionalOperation> callback) {
      if(task.isDelete()) {
        String spaceId = task.responseSpaces.get(0).getId();
        TagConfigClient.getInstance().deleteTagsForSpace(task.getMarker(), spaceId)
            .onSuccess(a-> callback.call(task))
            .onFailure(a->{
              logger.error(task.getMarker(), "Failed to delete tags for space {}", spaceId, a);
              callback.call(task);
            });
      }
      else {
        callback.call(task);
      }
    }
}
