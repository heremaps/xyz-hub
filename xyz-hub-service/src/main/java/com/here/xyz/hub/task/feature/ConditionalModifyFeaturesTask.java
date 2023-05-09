package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.ModifyFeatureOp;
import com.here.xyz.hub.task.ModifyOp;
import com.here.xyz.hub.task.TaskPipeline;
import com.here.xyz.hub.util.diff.Patcher;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.responses.XyzResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ConditionalModifyFeaturesTask extends AbstractFeatureTask<ModifyFeaturesEvent> {

  public @Nullable ModifyFeatureOp modifyOp;
  public @Nullable ModifyOp.IfNotExists ifNotExists;
  public @Nullable ModifyOp.IfExists ifExists;
  public boolean transactional;
  public @Nullable Patcher.ConflictResolution conflictResolution;
  public List<Feature> unmodifiedFeatures;
  public final boolean requireResourceExists;
  public List<String> addTags;
  public List<String> removeTags;
  public String prefixId;
  public Map<Object, Integer> positionById;
  public LoadFeaturesEvent loadFeaturesEvent;
  public boolean hasNonModified;

  public ConditionalModifyFeaturesTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public @NotNull ModifyFeaturesEvent createEvent() {
    return new ModifyFeaturesEvent();
  }

  @Override
  public void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);

  }

  @Override
  protected @NotNull XyzResponse execute() throws Exception {
    // TODO: Send LoadFeaturesEvent
    //
    pipeline.addSpaceHandler(getSpace());
    return sendAuthorizedEvent(event, requestMatrix());
  }

  /*


  public ConditionalModifyFeaturesTask(
      @NotNull ModifyFeaturesEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType,
      @Nullable ModifyOp.IfNotExists ifNotExists,
      @Nullable ModifyOp.IfExists ifExists,
      boolean transactional,
      @Nullable Patcher.ConflictResolution conflictResolution,
      boolean requireResourceExists
  ) {
    this(event, context, apiResponseTypeType, null, requireResourceExists);
    this.ifNotExists = ifNotExists;
    this.ifExists = ifExists;
    this.transactional = transactional;
    this.conflictResolution = conflictResolution;
  }


  @Override
  public void initPipeline(@NotNull TaskPipeline<ModifyFeaturesEvent, ConditionalModifyFeaturesTask> pipeline) {
    pipeline
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureTaskHandler::registerRequestMemory)
        .then(FeatureTaskHandler::throttle)
        .then(FeatureTaskHandler::injectSpaceParams)
        .then(FeatureTaskHandler::checkPreconditions)
        .then(FeatureTaskHandler::prepareModifyFeatureOp)
        .then(FeatureTaskHandler::preprocessConditionalOp)
        .then(FeatureTaskHandler::loadObjects)
        .then(FeatureTaskHandler::verifyResourceExists)
        .then(FeatureTaskHandler::updateTags)
        .then(FeatureTaskHandler::monitorFeatureRequest)
        .then(FeatureTaskHandler::processConditionalOp)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::enforceUsageQuotas)
        .then(FeatureTaskHandler::extractUnmodifiedFeatures)
        .then(FeatureTaskHandler::invoke);
  }

  private ConditionalModifyFeaturesTask buildConditionalOperation(
      ModifyFeaturesEvent event,
      RoutingContext context,
      ApiResponseType apiResponseTypeType,
      List<Map<String, Object>> featureModifications,
      IfNotExists ifNotExists,
      IfExists ifExists,
      boolean transactional,
      ConflictResolution cr,
      boolean requireResourceExists,
      int bodySize) {
    if (featureModifications == null)
      return new ConditionalModifyFeaturesTask(event, context, apiResponseTypeType, ifNotExists, ifExists, transactional, cr, requireResourceExists, bodySize);

    final ModifyFeatureOp modifyFeatureOp = new ModifyFeatureOp(featureModifications, ifNotExists, ifExists, transactional, cr);
    return new ConditionalModifyFeaturesTask(event, context, apiResponseTypeType, modifyFeatureOp, requireResourceExists, bodySize);
  }

   */
}
