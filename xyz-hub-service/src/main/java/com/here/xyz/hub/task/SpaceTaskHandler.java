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
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.amazonaws.util.CollectionUtils;
import com.here.xyz.Payload;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.config.SpaceConfigClient.SpaceSelectionCondition;
import com.here.xyz.hub.config.settings.SpaceStorageMatchingMap;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.connectors.models.Space.SpaceWithRights;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.TagApi;
import com.here.xyz.hub.task.FeatureTask.ModifySpaceQuery;
import com.here.xyz.hub.task.ModifyOp.Entry;
import com.here.xyz.hub.task.ModifyOp.ModifyOpError;
import com.here.xyz.hub.task.SpaceTask.ConditionalOperation;
import com.here.xyz.hub.task.SpaceTask.ConnectorMapping;
import com.here.xyz.hub.task.SpaceTask.ReadQuery;
import com.here.xyz.hub.task.SpaceTask.View;
import com.here.xyz.hub.task.TaskPipeline.C1;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.models.hub.jwt.ActionMatrix;
import com.here.xyz.models.hub.jwt.AttributeMap;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.util.Async;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.web.JobWebClient;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
  private static final Async ASYNC = new Async(10, SpaceTaskHandler.class);
  private static final Logger logger = LogManager.getLogger();
  private static final int CLIENT_VALUE_MAX_SIZE = 1024;
  private static final String DRY_RUN_SUPPORT_VERSION = "0.7.0";

  static <X extends ReadQuery<?>> void readSpaces(final X task, final Callback<X> callback) {
    Service.spaceConfigClient.getSelected(task.getMarker(),
                task.authorizedCondition, task.selectedCondition, task.propertiesQuery)
        .compose(spaces -> {
          if (StringUtils.isBlank(task.selectedCondition.tagId) || spaces.isEmpty()) {
            return Future.succeededFuture(spaces);
          }

          List<String> spaceIds = spaces.stream().map(Space::getId).toList();
          return Service.tagConfigClient.getTags(task.getMarker(), task.selectedCondition.tagId, spaceIds)
                  .compose(tags -> {
                    List<String> spaceIdsFromTag = tags.stream().map(Tag::getSpaceId).toList();
                    List<Space> spacesFilteredByTag = spaces.stream().filter(space -> spaceIdsFromTag.contains(space.getId())).toList();
                    return augmentWithTags(spacesFilteredByTag, tags);
                  });
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

  static <X extends ReadQuery<?>> void readExtendingSpaces(final X task, final Callback<X> callback) {
    if(task.selectedCondition.spaceIds == null){
      callback.call(task);
      return;
    }
    String spaceId = task.selectedCondition.spaceIds.stream().findFirst().orElse(null);

    getAllExtendingSpaces(task.getMarker(), spaceId)
            .onFailure(t -> {
              logger.error(task.getMarker(), "Unable to load space definitions.'", t);
              callback.exception(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", t));
            })
            .onSuccess(spaces -> {
              task.responseSpaces = spaces;
              callback.call(task);
            });

  }

  static <X extends ReadQuery<?>> void checkSpaceExists(final X task, final Callback<X> callback) {
    if (task.responseType == ApiResponseType.SPACE && CollectionUtils.isNullOrEmpty(task.responseSpaces)) {
      callback.exception(new HttpException(NOT_FOUND, "The requested resource does not exist."));
      return;
    }

    callback.call(task);
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
    task.authorizedCondition = new SpaceSelectionCondition();
    task.authorizedCondition.spaceIds = new HashSet<>();
    task.authorizedCondition.ownerIds = new HashSet<>();
    task.authorizedCondition.packages = new HashSet<>();

    if (task.responseType == ApiResponseType.SPACE) {
      callback.call(task);
      return;
    }

    final List<String> authorizedListSpacesOps = Arrays
        .asList(ADMIN_SPACES, MANAGE_SPACES, READ_FEATURES, CREATE_FEATURES, UPDATE_FEATURES, DELETE_FEATURES);
    final ActionMatrix tokenRights = Authorization.getXyzHubMatrix(task.getJwt());

    if (tokenRights != null) {
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

      String spaceId = input.getString("id");
      String region = input.getString("region");

      String storageId = task.template.getStorage().getId();
      logger.info(task.getMarker(), "storageId from space template: " + storageId);

      if (region != null) {
        storageId = Service.configuration.getDefaultStorageId(region);
        logger.info(task.getMarker(), "default storageId from region " + region + ": " + storageId);

        if (task.modifyOp.connectorMapping == ConnectorMapping.SPACESTORAGEMATCHINGMAP) {
          if (spaceId != null) {
            String matchedStorageId = SpaceStorageMatchingMap.getIfMatches(spaceId, region);
            logger.info(task.getMarker(), "SpaceStorageMatchingMap from space/region/storage mapping: {}/{}/{}", spaceId, region, matchedStorageId);
            if (matchedStorageId != null) storageId = matchedStorageId;
          }
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
       * Validate immutable settings which only can get set during the space creation:
       * extension
       * */
      Space head = task.modifyOp.entries.get(0).head;

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
      inputStorage.computeIfAbsent("params", k -> new HashMap<>());

      resultStorage.computeIfAbsent("params", k -> new HashMap<>());

      // if there is any modification, means the user tried to submit 'storage' and 'extends' properties together
      if (Patcher.getDifference(inputStorage, resultStorage) != null && !task.modifyOp.forceStorage ) {
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
        .onFailure(callback::exception)
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

    if (task.modifyOp.dryRun) {
      task.responseSpaces = Collections.singletonList(task.isDelete() ? entry.head : entry.result);
      callback.call(task);
      return;
    }

    if (entry.input != null && entry.result == null) {
      logger.warn("DEBUG: Deleting space: {}", entry.head.getId());
      Service.spaceConfigClient
          .delete(task.getMarker(), entry.head.getId())
          .onFailure(callback::exception)
          .onSuccess(v -> {
            task.responseSpaces = Collections.singletonList(task.modifyOp.entries.get(0).head);
            callback.call(task);
          });
    }
    else {
      if (areSearchablePropertiesChanged(task, entry))
        createMaintenanceJob(task.getMarker(), entry.result.getId());

      logger.warn("DEBUG: Storing space: {}", entry.result.getId());
      Service.spaceConfigClient
          .store(task.getMarker(), entry.result)
          .onFailure(callback::exception)
          .onSuccess(v -> {
            task.responseSpaces = Collections.singletonList(entry.result);
            callback.call(task);
          });
    }
  }

  private static boolean areSearchablePropertiesChanged(ConditionalOperation task, Entry<Space> entry) {
    // Only take care about space updates where properties were modified
    if(!task.isUpdate() || !entry.isModified){
      return false;
    }

    Map<String, Boolean> headProperties = entry.head.getSearchableProperties();
    Map<String, Boolean> resultProperties = entry.result.getSearchableProperties();

    if(headProperties != null && !headProperties.equals(resultProperties)
            || headProperties == null && resultProperties != null){
      //skip check if the space has an extension
      return entry.result.getExtension() == null;
    }
    return false;
  }

  static void createMaintenanceJob(Marker marker, String spaceId){
    // If the searchable properties were modified, trigger the OnDemand Index Creation Job
    logger.info(marker, "Trigger OnDemand Index Creation Job");

    JsonObject maintenanceJob = new JsonObject("""              
            {
                "description": "Maintain indices for the space $SPACE_ID",
                "source": {
                    "type": "Space",
                    "id": "$SPACE_ID"
                },
                "process": {
                    "type": "Maintain"
                }
            }
            """.replaceAll("\\$SPACE_ID", spaceId));

    ASYNC.run(() -> JobWebClient.getInstance(Config.instance.JOB_API_ENDPOINT).createJob(maintenanceJob))
            .onFailure(e -> {
              logger.error(marker, "Creation of maintenance Job", e);
            }).onSuccess(job -> {
              logger.info(marker, "Creation of maintenance Job succeeded: {}", job);
            });
  }

  private static Space getSpaceTemplate(String owner, String cid) {
    Space space = new Space()
        .withRegion(Service.configuration.AWS_REGION);
    space.setId(RandomStringUtils.randomAlphanumeric(8));
    space.setOwner(owner);
    space.setCid(cid);
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

    final ActionMatrix accessMatrix = Authorization.getXyzHubMatrix(task.getJwt());

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

    if (space.getId().equals(space.getExtension().getSpaceId())) {
      callback.exception(new HttpException(BAD_REQUEST, "The space " + space.getId() + " cannot extend itself."));
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

        if (!StringUtils.equalsIgnoreCase(space.getRegion(), extendedSpace.getRegion())) {
          callback.exception(new HttpException(BAD_REQUEST, "Unable to extend a layer from another region. The current space's region is " + space.getRegion() + " and the extende "
              + space.getExtension().getSpaceId() + " because it does not exist."));
          return;
        }

        ConnectorRef extendedConnector = extendedSpace.getStorage();
        if (task.isCreate()) {
          //Store searchableProperties from base (but without storing them later)
          task.resolvedSearchableProperties = extendedSpace.getSearchableProperties();
          //Override the storage config by copying it from the extended space
          space.setStorage(new ConnectorRef()
                  .withId(extendedConnector.getId())
                  .withParams(extendedConnector.getParams() != null ? extendedConnector.getParams() : new HashMap<>()));
        }
        else if (!Objects.equals(space.getStorage().getId(), extendedConnector.getId()) && !task.modifyOp.forceStorage) {
          callback.exception(new DetailedHttpException("E318408",
                  Map.of("spaceId", space.getId(),
                          "storageId", space.getStorage().getId(),
                          "extendedSpaceId", extendedSpace.getId(),
                          "extendedStorageId", extendedConnector.getId()))
          );
          return;
        }

        task.resolvedExtensions = space.resolveCompositeParams(extendedSpace);

        //Check for extensions with more than 2 levels
        if (extendedSpace.getExtension() != null) {
          if (space.getId().equals(extendedSpace.getExtension().getSpaceId())) {
            callback.exception(new HttpException(BAD_REQUEST, "Cyclical reference on the extension " + extendedSpace.getId() + " for the space " + space.getId()));
          }

          Space.resolveSpace(task.getMarker(), extendedSpace.getExtension().getSpaceId())
              .onFailure(t -> onExtensionResolveError(task, t, callback))
              .onSuccess(secondLvlExtendedSpace -> {
                if (secondLvlExtendedSpace == null)
                  onExtensionResolveError(task, new Exception("Unexpected error: Extension of extended space could not be found."), callback);

                Map<String, Object> resolvedSecondLvlExtensions = extendedSpace.resolveCompositeParams(secondLvlExtendedSpace);
                if (((Map<String, Object>) resolvedSecondLvlExtensions.get("extends")).containsKey("extends"))
                  callback.exception(new HttpException(BAD_REQUEST, "The space " + space.getId() + " cannot extend the space "
                      + extendedSpace.getId() + " because the maximum extension level is 2."));
                else{
                  //Store searchableProperties from base (but without storing them later)
                  if (task.isCreate())
                    task.resolvedSearchableProperties = secondLvlExtendedSpace.getSearchableProperties();
                  callback.call(task);
                }
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

    if (task.modifyOp.dryRun && task.getEvent().getVersion().compareTo(DRY_RUN_SUPPORT_VERSION) < 0) {
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

    if(task.resolvedSearchableProperties != null)
      space.withSearchableProperties(task.resolvedSearchableProperties);

    if (entry.isModified) {
      final ModifySpaceEvent event = new ModifySpaceEvent()
              .withOperation(op)
              .withSpaceDefinition(space)
              .withStreamId(task.getMarker().getName())
              .withParams(storageParams)
              .withIfNoneMatch(task.context.request().headers().get("If-None-Match"))
              .withSpace(space.getId())
              .withDryRun(task.modifyOp.dryRun);

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
        //remove searchable properties from the result if the space has an extension
        if(entry.result != null)
          entry.result.withSearchableProperties(entry.result.getExtension() != null ? null : entry.result.getSearchableProperties());
        callback.call(task);
      };
      //Send "ModifySpaceEvent" to (all) the connector(s) to do some setup, update or clean up.
      query.execute(onEventProcessed, (t, e) -> callback.exception(e));

      return;
    }
    callback.call(task);
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
            .onFailure(callback::exception);
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
          if( response != null )
          { space.setReadOnlyHeadVersion(response.getMaxVersion());
            p.complete();
          }
          else p.fail("failed to get Changeset statistics");
        }))
        .onFailure(p::fail);
    return p.future();
  }

  public static void resolveDependencies(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    if (task.isDelete()) {
      resolveDependenciesForDeletion(task, callback);
      return;
    }

    if (task.isCreate()) {
      resolveDependenciesForCreation(task, callback);
      return;
    }

    callback.call(task);
  }

  private static void resolveDependenciesForDeletion(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    final String spaceId = task.responseSpaces.get(0).getId();

    final Future<List<Tag>> tagsFuture = Service.tagConfigClient.deleteTagsForSpace(task.getMarker(), spaceId)
        .onFailure(e->logger.error(task.getMarker(), "Failed to delete tags for space {}", spaceId, e));

    final Future<Void> deactivateFuture = getAllExtendingSpaces(task.getMarker(), spaceId)
        .map(spaces -> spaces.stream().map(space -> Service.spaceConfigClient.store(task.getMarker(), (Space) space.withActive(false))).collect(Collectors.toList()))
        .map(Future::all)
        .mapEmpty();

    Future.all(tagsFuture, deactivateFuture)
        .onComplete(v -> {
          if (v.failed())
            logger.error(task.getMarker(), "Failed to complete clean dependent resources for space {}", spaceId, v.cause());
          callback.call(task);
        });
  }

  private static void resolveDependenciesForCreation(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    final String spaceId = task.responseSpaces.get(0).getId();

    String author = BaseHttpServerVerticle.getAuthor(task.context);
    // check if there are subscriptions with source pointing to the space id being created
    Service.subscriptionConfigClient.getBySource(task.getMarker(), spaceId)
        .compose(subscriptions -> {
          if (!subscriptions.isEmpty()) {
            return TagApi.createTag(task.getMarker(), spaceId, Service.configuration.SUBSCRIPTION_TAG, author);
          }

          return Future.succeededFuture();
        })
        .onComplete(handler -> {
          if (handler.failed()) {
            logger.error(task.getMarker(), "Failed to resolve dependencies during creation of {}", spaceId, handler.cause());
          }
          callback.call(task);
        });
  }

  private static Future<List<Space>> getAllExtendingSpaces(Marker marker, String spaceId) {
    return Service.spaceConfigClient.getSpacesFromSuper(marker, spaceId)
        .compose(spaces -> {
          final List<Future<List<Space>>> childrenFutures = spaces.stream().map(space ->
              Service.spaceConfigClient.getSpacesFromSuper(marker, space.getId())).toList();

          return Future.all(childrenFutures).map(cf -> {
            spaces.addAll(cf.<List<Space>>list().stream().flatMap(Collection::stream).toList());
            return spaces;
          });
        });
  }

  static void invokeConditionally(final ModifySpaceQuery task, final Callback<ModifySpaceQuery> callback) {
    if (task.getEvent().isDryRun() && Payload.compareVersions(task.storage.getRemoteFunction().protocolVersion, DRY_RUN_SUPPORT_VERSION) < 0) {
      callback.call(task);
    }
    else {
      FeatureTaskHandler.invoke(task, callback);
    }
  }
}
