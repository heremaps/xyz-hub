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

package com.here.xyz.hub.connectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.AWSLambdaException;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class LambdaFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();
  private static final int CONNECTION_ESTABLISH_TIMEOUT = 5_000;
  private static final int CLIENT_REQUEST_TIMEOUT = 28_000;
  private static final int CONNECTION_TTL = 60_000;

  /**
   * The maximal response size in bytes that can be sent back without relocating the response.
   */
  private AWSLambdaAsync asyncClient;
  private AWSCredentialsProvider awsCredentialsProvider;

  /**
   * @param connectorConfig The connector configuration.
   */
  LambdaFunctionClient(final Connector connectorConfig) {
    super(connectorConfig);
  }

  @Override
  synchronized void setConnectorConfig(final Connector newConnectorConfig) throws NullPointerException, IllegalArgumentException {
    super.setConnectorConfig(newConnectorConfig);
    shutdownLambdaClient(asyncClient);
    createClient();
  }

  private void createClient() {
    final Connector connectorConfig = getConnectorConfig();
    final RemoteFunctionConfig remoteFunction = connectorConfig.remoteFunction;
    if (!(remoteFunction instanceof AWSLambda)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of AWSLambda");
    }
    int maxConnections = connectorConfig.getMaxConnectionsPerInstance();
    asyncClient = AWSLambdaAsyncClientBuilder
        .standard()
        .withRegion(extractRegionFromArn(((AWSLambda) remoteFunction).lambdaARN))
        .withCredentials(getAWSCredentialsProvider())
        .withClientConfiguration(new ClientConfiguration()
            .withMaxConnections(maxConnections)
            .withConnectionTimeout(CONNECTION_ESTABLISH_TIMEOUT)
            .withRequestTimeout((int) REQUEST_TIMEOUT)
            .withMaxErrorRetry(0)
            .withClientExecutionTimeout(CLIENT_REQUEST_TIMEOUT)
            .withConnectionTTL(CONNECTION_TTL))
        .build();
  }

  private static void shutdownLambdaClient(AWSLambdaAsync lambdaClient) {
    if (lambdaClient == null) return;
    //Shutdown the lambda client after the request timeout
    //TODO: Use CompletableFuture.delayedExecutor() after switching to Java 9
    new Thread(() -> {
      try {
        Thread.sleep(REQUEST_TIMEOUT);
      }
      catch (InterruptedException ignored) {}
      lambdaClient.shutdown();
    }).start();
  }

  @Override
  synchronized void destroy() {
    super.destroy();
    shutdownLambdaClient(asyncClient);
  }

  /**
   * Invokes the remote lambda function and returns the decompressed response as bytes.
   */
  @Override
  protected void invoke(final Marker marker, byte[] bytes, final Handler<AsyncResult<byte[]>> callback) {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().remoteFunction;
    logger.debug(marker, "Invoking remote lambda function with id '{}' Event size is: {}", remoteFunction.id, bytes.length);

    InvokeRequest invokeReq = new InvokeRequest()
        .withFunctionName(((AWSLambda) remoteFunction).lambdaARN)
        .withPayload(ByteBuffer.wrap(bytes));

    asyncClient.invokeAsync(invokeReq, new AsyncHandler<InvokeRequest, InvokeResult>() {
      @Override
      public void onError(Exception exception) {
        callback.handle(Future.failedFuture(getWHttpException(marker, exception)));
      }

      @Override
      public void onSuccess(InvokeRequest request, InvokeResult result) {
        try {
          //TODO: Refactor to move decompression into the base-class RemoteFunctionClient as it's not Lambda specific
          byte[] responseBytes = new byte[result.getPayload().remaining()];
          result.getPayload().get(responseBytes);
          checkResponseSize(responseBytes);
          callback.handle(Future.succeededFuture(getDecompressed(responseBytes)));
        } catch (IOException | HttpException e) {
          callback.handle(Future.failedFuture(e));
        }
      }
    });
  }

  /**
   * Returns the AWS credentials provider for this lambda executor service.
   *
   * @return the AWS credentials provider.
   */
  private String extractRegionFromArn(String lambdaARN) throws NullPointerException {
    return lambdaARN.split(":")[3];
  }

  /**
   * Returns the AWS credentials provider for this lambda executor service.
   *
   * @return the AWS credentials provider.
   */
  private AWSCredentialsProvider getAWSCredentialsProvider() {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().remoteFunction;
    if (awsCredentialsProvider == null) {
      if (((AWSLambda) remoteFunction).roleARN != null) {
        awsCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(((AWSLambda) remoteFunction).roleARN,
            "" + this.hashCode())
            .withStsClient(AWSSecurityTokenServiceClientBuilder.defaultClient())
            .build();
      } else {
        awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
      }
    }
    return awsCredentialsProvider;
  }

  private HttpException getWHttpException(Marker marker, Throwable e) {
    logger.info(marker, "Unexpected exception while contacting lambda provider", e);

    if (e instanceof HttpException) {
      return (HttpException) e;
    }
    if (e instanceof AWSLambdaException) {
      AWSLambdaException le = (AWSLambdaException) e;
      if (le.getStatusCode() == 413) {
        return new HttpException(REQUEST_ENTITY_TOO_LARGE, "The compressed request must be smaller than 6291456 bytes.");
      }
    }

    return new HttpException(BAD_GATEWAY, "Unable to parse the response of the connector.", e);
  }
}
