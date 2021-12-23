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

package com.here.xyz.hub.connectors;

import static com.here.xyz.hub.rest.Api.CLIENT_CLOSED_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.http.exception.HttpRequestTimeoutException;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.AWSLambdaException;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ResourceNotFoundException;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.util.concurrent.ForwardingExecutorService;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.util.ARN;
import com.here.xyz.hub.util.LimitedOffHeapQueue.PayloadVanishedException;
import com.here.xyz.hub.util.RetryUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class LambdaFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();
  private static final int CONNECTION_ESTABLISH_TIMEOUT = 5_000;
  //  private static final int CLIENT_REQUEST_TIMEOUT = REQUEST_TIMEOUT + 3_000;
  private static final int CONNECTION_TTL = 60_000; //ms
  private static final int MIN_THREADS_PER_CLIENT = 5;
  private static final int MAX_RETRIES = 2;
  private static final int RETRY_TIMEOUT = 1000;
  private static final Function<Throwable, Integer> RETRY_BACKOFF_HANDLER = t -> PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION.shouldRetry(null, (AmazonClientException) t, 0) ? 50 : -1;

  private AWSLambdaAsync asyncClient;
  private static ConcurrentHashMap<String, AWSLambdaAsync> lambdaClients = new ConcurrentHashMap<>();
  private static Map<AWSLambdaAsync, List<String>> clientReferences = new HashMap<>();
  private static ExecutorService executors = new ForwardingExecutorService() {
    private ExecutorService threadPool = new ThreadPoolExecutor(
        Service.configuration.REMOTE_FUNCTION_MAX_CONNECTIONS,
        Service.configuration.REMOTE_FUNCTION_MAX_CONNECTIONS,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(), Core.newThreadFactory("lambdaRfcs"));

    @Override
    protected ExecutorService delegate() {
      return threadPool;
    }

    @Override
    public List<Runnable> shutdownNow() {
      return Collections.emptyList();
    }
  };

  /**
   * @param connectorConfig The connector configuration.
   */
  LambdaFunctionClient(final Connector connectorConfig) {
    super(connectorConfig);
  }

  @Override
  synchronized void setConnectorConfig(final Connector newConnectorConfig) throws NullPointerException, IllegalArgumentException {
    Connector oldConnectorConfig = getConnectorConfig();
    super.setConnectorConfig(newConnectorConfig);
    updateClient(oldConnectorConfig);
  }

  private void releaseClient(String clientKey) {
    AWSLambdaAsync client = lambdaClients.get(clientKey);
    List<String> references = clientReferences.get(client);
    references.remove(getConnectorConfig().id);
    if (references.isEmpty()) {
      logger.info("Destroying Lambda Function Client for client key {}", clientKey);
      //No connector client is referencing the lambda client it can be destroyed
      lambdaClients.remove(clientKey);
      clientReferences.remove(client);
      shutdownLambdaClient(client);
    }
  }

  private void updateClient(Connector oldConnectorConfig) {
    RemoteFunctionConfig remoteFunction = getConnectorConfig().getRemoteFunction();
    if (!(remoteFunction instanceof AWSLambda)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of AWSLambda");
    }
    asyncClient = getLambdaClient((AWSLambda) remoteFunction, getConnectorConfig().id);
    if (oldConnectorConfig != null)
      releaseClient(getClientKey((AWSLambda) oldConnectorConfig.getRemoteFunction()));
  }

  private static AWSLambdaAsync createClient(AWSLambda remoteFunction) {
    logger.info("Creating Lambda Function Client for function {} lambda ARN {}, role ARN: {}",
        remoteFunction.id, remoteFunction.lambdaARN, remoteFunction.roleARN);

    return AWSLambdaAsyncClientBuilder
        .standard()
        .withRegion(ARN.fromString(remoteFunction.lambdaARN).getRegion())
        .withCredentials(getAWSCredentialsProvider(remoteFunction))
        .withClientConfiguration(new ClientConfiguration()
            .withTcpKeepAlive(true)
            .withMaxConnections(Service.configuration.REMOTE_FUNCTION_MAX_CONNECTIONS)
            .withConnectionTimeout(CONNECTION_ESTABLISH_TIMEOUT)
            .withRequestTimeout(remoteFunction.getTimeout())
            .withMaxErrorRetry(0)
//            .withClientExecutionTimeout(CLIENT_REQUEST_TIMEOUT)
            .withConnectionTTL(CONNECTION_TTL))
        .withExecutorFactory(() -> executors)
        .build();
  }

  private static void shutdownLambdaClient(AWSLambdaAsync lambdaClient) {
    if (lambdaClient == null)
      return;
    //Shutdown the lambda client after the request timeout
    //TODO: Use CompletableFuture.delayedExecutor() after switching to Java 9
    new Thread(() -> {
      try {
        Thread.sleep(MAX_REQUEST_TIMEOUT);
      }
      catch (InterruptedException ignored) {}
      lambdaClient.shutdown();
    }).start();
  }

  @Override
  synchronized void destroy() {
    super.destroy();
    releaseClient(getClientKey((AWSLambda) getConnectorConfig().getRemoteFunction()));
  }

  /**
   * Invokes the remote lambda function and returns the decompressed response as bytes.
   */
  @Override
  protected void invoke(final FunctionCall fc, final Handler<AsyncResult<byte[]>> callback) {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().getRemoteFunction();
    Marker marker = fc.marker;
    Context context = fc.context;
    logger.debug(marker, "Invoking remote lambda function with id '{}' Event size is: {}", remoteFunction.id, fc.getByteSize());

    try {
      InvokeRequest invokeReq = new InvokeRequest()
          .withFunctionName(((AWSLambda) remoteFunction).lambdaARN)
          .withPayload(ByteBuffer.wrap(fc.consumePayload()))
          .withInvocationType(fc.fireAndForget ? InvocationType.Event : InvocationType.RequestResponse);

      Future<byte[]> future = RetryUtil.<byte[]>executeWithRetry(task -> {
        Promise<byte[]> p = Promise.promise();
        java.util.concurrent.Future<InvokeResult> lambdaFuture = asyncClient.invokeAsync(invokeReq, new AsyncHandler<InvokeRequest, InvokeResult>() {

          @Override
          public void onError(Exception exception) {
            p.fail(exception);
          }

          @Override
          public void onSuccess(InvokeRequest request, InvokeResult result) {
            byte[] responseBytes = new byte[result.getPayload().remaining()];
            result.getPayload().get(responseBytes);
            p.complete(responseBytes);
          }
        });
        fc.setCancelHandler(() -> {
          lambdaFuture.cancel(true);
          task.cancelRetries();
        });
        return p.future();
      }, RETRY_BACKOFF_HANDLER, MAX_RETRIES, RETRY_TIMEOUT).future();

      future
          .onSuccess(responseBytes -> context.runOnContext(v -> callback.handle(Future.succeededFuture(responseBytes))))
          .onFailure(t -> {
            if (callback == null) {
              logger.error(marker, "Error sending event to remote lambda function", t);
            }
            else {
              fc.context.runOnContext(v -> callback.handle(Future.failedFuture(getHttpException(marker, t))));
            }
          });
    }
    catch (PayloadVanishedException e) {
      callback.handle(Future.failedFuture(new HttpException(TOO_MANY_REQUESTS, "Remote function is busy or cannot be invoked.")));
      return;
    }
  }

  /**
   * Returns the AWS credentials provider for this lambda executor service.
   *
   * @param remoteFunction The remote function config containing the credentials information
   * @return the AWS credentials provider.
   */
  private static AWSCredentialsProvider getAWSCredentialsProvider(AWSLambda remoteFunction) {
    if (remoteFunction.roleARN != null) {
      return new STSAssumeRoleSessionCredentialsProvider.Builder(remoteFunction.roleARN, Node.OWN_INSTANCE.id)
          .withStsClient(AWSSecurityTokenServiceClientBuilder.defaultClient())
          .build();
    }
    else {
      return new DefaultAWSCredentialsProviderChain();
    }
  }

  private HttpException getHttpException(Marker marker, Throwable t) {
    if (t instanceof HttpException) {
      return (HttpException) t;
    }
    if (t instanceof AWSLambdaException) {
      AWSLambdaException le = (AWSLambdaException) t;
      if (le.getStatusCode() == 413) {
        return new HttpException(REQUEST_ENTITY_TOO_LARGE, "The compressed request must be smaller than 6291456 bytes.", t);
      }
      else if (t instanceof ResourceNotFoundException) {
        logger.warn(marker, "Lambda function does not exist.", t);
        return new HttpException(BAD_GATEWAY, "Error while contacting lambda function.", t);
      }
    }
    if (t instanceof HttpRequestTimeoutException || t instanceof SdkClientException && t.getCause() instanceof HttpRequestTimeoutException)
      return new HttpException(GATEWAY_TIMEOUT, "The connector did not respond in time.", t);
    if (t instanceof AbortedException) {
      String msg = "Lambda function call was aborted.";
      logger.warn(marker, "Lambda function call was aborted.", t);
      return new HttpException(CLIENT_CLOSED_REQUEST, msg, t);
    }

    logger.error(marker, "Unexpected exception while contacting lambda function", t);
    return new HttpException(BAD_GATEWAY, "Error while contacting lambda function.", t);
  }

  private static AWSLambdaAsync getLambdaClient(AWSLambda remoteFunction, String forConnectorId) {
    String clientKey = getClientKey(remoteFunction);
    AWSLambdaAsync client;
    if (!lambdaClients.containsKey(clientKey)) {
      client = createClient(remoteFunction);
      if (lambdaClients.putIfAbsent(clientKey, client) != null) {
        client.shutdown();
        client = lambdaClients.get(clientKey);
      }
    }
    else
      client = lambdaClients.get(clientKey);

    //Register the connector to reference the client
    if (!clientReferences.containsKey(client)) {
      clientReferences.put(client, new LinkedList<>());
    }
    clientReferences.get(client).add(forConnectorId);

    return client;
  }

  private static String getClientKey(AWSLambda remoteFunction) {
    ARN lambdaArn = ARN.fromString(remoteFunction.lambdaARN);
    return lambdaArn.getRegion() + (remoteFunction.roleARN != null ? "_" + remoteFunction.roleARN : "");
  }

}
