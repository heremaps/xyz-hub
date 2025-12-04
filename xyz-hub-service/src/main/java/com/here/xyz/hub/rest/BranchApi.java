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

package com.here.xyz.hub.rest;

import static com.here.xyz.models.hub.Ref.HEAD;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.vertx.core.http.HttpMethod.GET;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.task.BranchHandler;
import com.here.xyz.models.hub.Branch;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.List;

public class BranchApi extends SpaceBasedApi {
  private static final String BRANCH_ID = "branchId";

  public BranchApi(RouterBuilder rb) {
    rb.getRoute("getBranches").setDoValidation(false).addHandler(handle(this::getBranches));
    rb.getRoute("postBranch").setDoValidation(false).addHandler(handle(this::postBranch));
    rb.getRoute("getBranch").setDoValidation(false).addHandler(handle(this::getBranch));
    rb.getRoute("patchBranch").setDoValidation(false).addHandler(handle(this::patchBranch));
    rb.getRoute("deleteBranch").setDoValidation(false).addHandler(handle(this::deleteBranch));
    rb.getRoute("execBranchOperation").setDoValidation(false).addHandler(handle(this::execBranchOperation));
  }

  public void activateAdminRoutes(Router router, AuthenticationHandler auth) {
    router.route(GET, "/hub/admin/spaces/:spaceId/branches")
        .handler(auth)
        .handler(handleInternal(this::getBranches));

    router.route(GET, "/hub/admin/spaces/:spaceId/branches/:branchId")
            .handler(auth)
            .handler(handleInternal(this::getBranch));
  }

  private Future<List<Branch>> getBranches(RoutingContext context) {
    return BranchHandler.loadBranchesOfSpace(getSpaceId(context));
  }

  private Future<Branch> postBranch(RoutingContext context) throws HttpException {
    String spaceId = getSpaceId(context);
    Branch branch = getBranchPayload(context);
    return checkExistingAlias(getMarker(context), spaceId, branch.getId())
        .compose(v -> BranchHandler.createBranch(getMarker(context), getSpaceId(context), branch));
  }

  private Future<Branch> getBranch(RoutingContext context) {
    String spaceId = getSpaceId(context);
    String branchId = getBranchId(context);

    return BranchHandler.loadBranch(spaceId, branchId)
        .compose(branch -> {
          if (branch == null)
            return Future.failedFuture(new HttpException(NOT_FOUND, "No branch was found for space " + spaceId + " and ID " + branchId));
          return Future.succeededFuture(branch);
        });
  }

  private Future<Branch> patchBranch(RoutingContext context) throws HttpException {
    String spaceId = getSpaceId(context);
    String branchId = getBranchId(context);
    Branch branch = getBranchPayload(context);
    return BranchHandler.loadBranch(spaceId, branchId)
        .compose(existingBranch -> {
          if (existingBranch == null)
            return Future.failedFuture(new HttpException(NOT_FOUND, "No branch was found for space " + spaceId + " and ID " + branchId));

          return BranchHandler.upsertBranch(getMarker(context), spaceId, branchId, branch);
        });
  }

  private Future<Void> deleteBranch(RoutingContext context) {
    return BranchHandler.deleteBranch(getMarker(context), getSpaceId(context), getBranchId(context));
  }

  private Future<Branch> execBranchOperation(RoutingContext context) throws HttpException {
    BranchOperation branchOperation = getBranchOperation(context);
    if (branchOperation instanceof Merge mergeOperation)
      return BranchHandler.mergeBranch(getMarker(context), getSpaceId(context), getBranchId(context), mergeOperation.targetBranchId());
    else
      throw new HttpException(BAD_REQUEST, "Unknown branch operation: " + branchOperation.getClass().getSimpleName());
  }

  private Branch getBranchPayload(RoutingContext context) throws HttpException {
    byte[] body = context.body().buffer().getBytes();
    if (body == null || body.length == 0)
      throw new HttpException(BAD_REQUEST, "Unable to parse an empty body.");

    try {
      Branch branch = XyzSerializable.deserialize(body, Branch.class);
      if (branch.getBaseRef() == null)
        branch.setBaseRef(new Ref(HEAD));
      return branch;
    }
    catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Unable to parse the body.", e);
    }
  }

  private BranchOperation getBranchOperation(RoutingContext context) throws HttpException {
    byte[] body = context.body().buffer().getBytes();
    if (body == null || body.length == 0)
      throw new HttpException(BAD_REQUEST, "Unable to parse an empty body.");

    try {
      return XyzSerializable.deserialize(body, BranchOperation.class);
    }
    catch (JsonProcessingException e) {
      throw new HttpException(BAD_REQUEST, "Unable to parse the body.");
    }
  }

  protected final String getBranchId(RoutingContext context) {
    return context.pathParam(BRANCH_ID);
  }

  @JsonSubTypes({
      @JsonSubTypes.Type(value = Merge.class, name = "Merge")
  })
  private interface BranchOperation extends Typed {}

  private record Merge(String targetBranchId) implements BranchOperation {}
}
