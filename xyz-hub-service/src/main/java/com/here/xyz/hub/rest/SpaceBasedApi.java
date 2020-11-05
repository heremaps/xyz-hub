package com.here.xyz.hub.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.here.xyz.hub.task.FeatureTaskHandler.InvalidStorageException;
import com.here.xyz.hub.task.Task;
import com.here.xyz.responses.ErrorResponse;

public abstract class SpaceBasedApi extends Api {
  /**
   * Send an error response to the client when an exception occurred while processing a task.
   *
   * @param task the task for which to return an error response.
   * @param e the exception that should be used to generate an {@link ErrorResponse}, if null an internal server error is returned.
   */
  @Override
  public void sendErrorResponse(final Task task, final Exception e) {
    if (e instanceof InvalidStorageException) {
      super.sendErrorResponse(task.context, new HttpException(BAD_REQUEST, "The resource definition contains an invalid storage ID."));
    }
    else {
      super.sendErrorResponse(task.context, e);
    }
  }
}
