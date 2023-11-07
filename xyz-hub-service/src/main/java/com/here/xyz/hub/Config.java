package com.here.xyz.hub;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.hub.auth.AuthorizationType;
import com.here.xyz.hub.task.SpaceTask.ConnectorMapping;
import com.here.xyz.hub.util.ARN;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The service configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Config {

  /**
   * The global maximum number of http client connections.
   */
  public int MAX_GLOBAL_HTTP_CLIENT_CONNECTIONS;

  /**
   * Size of the in-memory cache in megabytes. (Maximum 2GB)
   */
  public int CACHE_SIZE_MB;

  /**
   * The port of the HTTP server.
   */
  public int HTTP_PORT;

  /**
   * The hostname, which under instances can use to contact the this service node.
   */
  public String HOST_NAME;

  /**
   * The initial number of instances.
   */
  public int INSTANCE_COUNT;

  /**
   * The S3 Bucket which could be used by connectors with transfer limitations to relocate responses.
   */
  public String XYZ_HUB_S3_BUCKET;

  /**
   * The public endpoint.
   */
  public String XYZ_HUB_PUBLIC_ENDPOINT;

  /**
   * The public health-check endpoint, i.e. /hub/
   */
  public String XYZ_HUB_PUBLIC_HEALTH_ENDPOINT = "/hub/";

  /**
   * The redis host.
   */
  @Deprecated
  public String XYZ_HUB_REDIS_HOST;

  /**
   * The redis port.
   */
  @Deprecated
  public int XYZ_HUB_REDIS_PORT;

  /**
   * The redis connection string.
   */
  public String XYZ_HUB_REDIS_URI;

  /**
   * The redis auth token.
   */
  public String XYZ_HUB_REDIS_AUTH_TOKEN;

  /**
   * The list of default storage IDs.
   */
  public List<String> defaultStorageIds;

  /**
   * Return a default storage ID.
   *
   * @return the ID of one of the default storage connectors
   */
  public String getDefaultStorageId() {
    return getDefaultStorageId(AWS_REGION);
  }

  public String getDefaultStorageId(String region) {
    List<String> storageIds = getDefaultStorageIds(region);
    if (storageIds == null) {
      storageIds = defaultStorageIds;
    }

    return storageIds == null ? null : storageIds.get((int) (Math.random() * storageIds.size()));
  }

  /**
   * Adds backward-compatibility for the deprecated environment variables XYZ_HUB_REDIS_HOST & XYZ_HUB_REDIS_PORT.
   */
  //TODO: Remove this workaround after the deprecation period
  @JsonIgnore
  public String getRedisUri() {
    if (XYZ_HUB_REDIS_HOST != null) {
      String protocol = XYZ_HUB_REDIS_AUTH_TOKEN != null ? "rediss" : "redis";
      int port = XYZ_HUB_REDIS_PORT != 0 ? XYZ_HUB_REDIS_PORT : 6379;
      return protocol + "://" + XYZ_HUB_REDIS_HOST + ":" + port;
    } else {
      return XYZ_HUB_REDIS_URI;
    }
  }

  /**
   * The urls of remote hub services, separated by semicolon ';'
   */
  public String XYZ_HUB_REMOTE_SERVICE_URLS;
  private List<String> hubRemoteServiceUrls;

  public List<String> getHubRemoteServiceUrls() {
    if (hubRemoteServiceUrls == null) {
      hubRemoteServiceUrls = XYZ_HUB_REMOTE_SERVICE_URLS == null ? null : Arrays.asList(XYZ_HUB_REMOTE_SERVICE_URLS.split(";"));
    }
    return hubRemoteServiceUrls;
  }

  /**
   * The authorization type.
   */
  public AuthorizationType XYZ_HUB_AUTH;

  /**
   * The public key used for verifying the signature of the JWT tokens.
   */
  public String JWT_PUB_KEY;

  /**
   * Adds backward-compatibility for public keys without header & footer.
   */
  //TODO: Remove this workaround after the deprecation period
  @JsonIgnore
  public String getJwtPubKey() {
    String jwtPubKey = JWT_PUB_KEY;
    if (jwtPubKey != null) {
      if (!jwtPubKey.startsWith("-----")) {
        jwtPubKey = "-----BEGIN PUBLIC KEY-----\n" + jwtPubKey;
      }
      if (!jwtPubKey.endsWith("-----")) {
        jwtPubKey = jwtPubKey + "\n-----END PUBLIC KEY-----";
      }
    }
    return jwtPubKey;
  }

  /**
   * If set to true, the connectors configuration will be populated with connectors defined in connectors.json.
   */
  public boolean INSERT_LOCAL_CONNECTORS;

  /**
   * If set to true, the connectors will receive health checks. Further an unhealthy connector gets deactivated automatically if the
   * connector config does not include skipAutoDisable.
   */
  public boolean ENABLE_CONNECTOR_HEALTH_CHECKS;

  /**
   * If true HERE Tiles are get handled as Base 4 encoded. Default is false (Base 10).
   */
  public boolean USE_BASE_4_H_TILES;

  /**
   * A comma separate list of IDs of the default storage connectors.
   */
  public String DEFAULT_STORAGE_ID;

  /**
   * The PostgreSQL URL.
   */
  public String STORAGE_DB_URL;

  /**
   * The database user.
   */
  public String STORAGE_DB_USER;

  /**
   * The database password.
   */
  public String STORAGE_DB_PASSWORD;

  /**
   * The ARN of the space table in DynamoDB.
   */
  public String SPACES_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the connectors table in DynamoDB.
   */
  public String CONNECTORS_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the packages table in DynamoDB.
   */
  public String PACKAGES_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the subscriptions table in DynamoDB.
   */
  public String SUBSCRIPTIONS_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the tags table in DynamoDB.
   */
  public String TAGS_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the settings table in DynamoDB.
   */
  public String SETTINGS_DYNAMODB_TABLE_ARN;

  /**
   * The ARN of the admin message topic.
   */
  public ARN ADMIN_MESSAGE_TOPIC_ARN;

  /**
   * The JWT token used for sending admin messages.
   */
  public String ADMIN_MESSAGE_JWT;

  /**
   * The port for the admin message server.
   */
  public int ADMIN_MESSAGE_PORT;

  /**
   * The total size assigned for remote functions queues.
   */
  public int GLOBAL_MAX_QUEUE_SIZE; //MB

  /**
   * The default timeout for remote function requests in seconds.
   */
  public int REMOTE_FUNCTION_REQUEST_TIMEOUT; //seconds

  /**
   * OPTIONAL: The maximum timeout for remote function requests in seconds. If not specified, the value of
   * {@link #REMOTE_FUNCTION_REQUEST_TIMEOUT} will be used.
   */
  public int REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT; //seconds

  /**
   * @return the value of {@link #REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT} if specified. The value of {@link #REMOTE_FUNCTION_REQUEST_TIMEOUT}
   * otherwise.
   */
  @JsonIgnore
  public int getRemoteFunctionMaxRequestTimeout() {
    return REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT > 0 ? REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT : REMOTE_FUNCTION_REQUEST_TIMEOUT;
  }

  /**
   * The maximum amount of RemoteFunction connections to be opened by this node.
   */
  public int REMOTE_FUNCTION_MAX_CONNECTIONS;

  /**
   * The amount of memory (in MB) which can be taken by incoming requests.
   */
  public int GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE_MB;

  /**
   * A value between 0 and 1 defining a threshold as percentage of utilized RemoteFunction max-connections after which to start prioritizing
   * more important connectors over less important ones.
   *
   * @see Config#REMOTE_FUNCTION_MAX_CONNECTIONS
   */
  public float REMOTE_FUNCTION_CONNECTION_HIGH_UTILIZATION_THRESHOLD;

  /**
   * A value between 0 and 1 defining a threshold as percentage of utilized service memory for in-flight request after which to start
   * prioritizing more important connectors over less important ones.
   */
  public float GLOBAL_INFLIGHT_REQUEST_MEMORY_HIGH_UTILIZATION_THRESHOLD;

  /**
   * A value between 0 and 1 defining a threshold as percentage of utilized service memory which depicts a very high utilization of the the
   * memory. The service uses that threshold to perform countermeasures to protect the service from overload.
   */
  public float SERVICE_MEMORY_HIGH_UTILIZATION_THRESHOLD;

  /**
   * The remote function pool ID to be used to select the according remote functions for this Service environment.
   */
  public String REMOTE_FUNCTION_POOL_ID;

  /**
   * The web root for serving static resources from the file system.
   */
  public String FS_WEB_ROOT;

  /**
   * The name of the upload limit header
   */
  public String UPLOAD_LIMIT_HEADER_NAME;

  /**
   * The code which gets returned if UPLOAD_LIMIT is reached
   */
  public int UPLOAD_LIMIT_REACHED_HTTP_CODE;

  /**
   * The message which gets returned if UPLOAD_LIMIT is reached
   */
  public String UPLOAD_LIMIT_REACHED_MESSAGE;

  /**
   * The name of the health check header to instruct for additional health status information.
   */
  public String HEALTH_CHECK_HEADER_NAME;

  /**
   * The value of the health check header to instruct for additional health status information.
   */
  public String HEALTH_CHECK_HEADER_VALUE;

  /**
   * An identifier for the service environment.
   */
  public String ENVIRONMENT_NAME;

  /**
   * Whether to publish custom service metrics like JVM memory utilization or Major GC count.
   */
  public boolean PUBLISH_METRICS;

  /**
   * The AWS region this service is running in. Value is <code>null</code> if not running in AWS.
   */
  public String AWS_REGION;

  /**
   * The verticles class names to be deployed, separated by comma
   */
  public String VERTICLES_CLASS_NAMES;

  /**
   * The default ECPS phrase. (Mainly for testing purposes)
   */
  public String DEFAULT_ECPS_PHRASE;

  /**
   * The topic ARN for Space modification notifications. If no value is provided no notifications will be sent.
   */
  public String MSE_NOTIFICATION_TOPIC;

  /**
   * The maximum size of an event transiting between connector -> service -> client. Validation is only applied when
   * MAX_SERVICE_RESPONSE_SIZE is bigger than zero.
   *
   * @deprecated Use instead MAX_UNCOMPRESSED_RESPONSE_SIZE
   */
  public int MAX_SERVICE_RESPONSE_SIZE;

  /**
   * The maximum uncompressed request size in bytes supported on API calls. If uncompressed request size is bigger than
   * MAX_UNCOMPRESSED_REQUEST_SIZE, an error with status code 413 will be sent.
   */
  public long MAX_UNCOMPRESSED_REQUEST_SIZE;

  /**
   * The maximum uncompressed response size in bytes supported on API calls. If uncompressed response size is bigger than
   * MAX_UNCOMPRESSED_RESPONSE_SIZE, an error with status code 513 will be sent.
   */
  public long MAX_UNCOMPRESSED_RESPONSE_SIZE;

  /**
   * The maximum http response size in bytes supported on API calls. If response size is bigger than MAX_HTTP_RESPONSE_SIZE, an error with
   * status code 513 will be sent. Validation is only applied when MAX_HTTP_RESPONSE_SIZE is bigger than zero.
   *
   * @deprecated Use instead MAX_UNCOMPRESSED_RESPONSE_SIZE
   */
  public int MAX_HTTP_RESPONSE_SIZE;

  /**
   * Whether to activate pipelining for the HTTP client of the service.
   */
  public boolean HTTP_CLIENT_PIPELINING;

  /**
   * Whether to activate TCP keepalive for the HTTP client of the service.
   */
  public boolean HTTP_CLIENT_TCP_KEEPALIVE = true;

  /**
   * The idle connection timeout in seconds for the HTTP client of the service. Setting it to 0 will make the connections not timing out at
   * all.
   */
  public int HTTP_CLIENT_IDLE_TIMEOUT = 120;

  /**
   * List of fields, separated by comma, which are optional on feature's namespace property.
   */
  public List<String> FEATURE_NAMESPACE_OPTIONAL_FIELDS = Collections.emptyList();
  private Map<String, Object> FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP;

  /**
   * When set, modifies the Stream-Info header name to the value specified.
   */
  public String CUSTOM_STREAM_INFO_HEADER_NAME;

  /**
   * Whether the service should use InstanceProviderCredentialsProfile with cached credential when utilizing AWS clients.
   */
  public boolean USE_AWS_INSTANCE_CREDENTIALS_WITH_REFRESH;

  public boolean containsFeatureNamespaceOptionalField(String field) {
    if (FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP == null) {
      FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP = new HashMap<String, Object>() {{
        FEATURE_NAMESPACE_OPTIONAL_FIELDS.forEach(k -> put(k, null));
      }};
    }

    return FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP.containsKey(field);
  }

  /**
   * Global limit for the maximum amount of versions to keep per space.
   */
  public long MAX_VERSIONS_TO_KEEP = 1_000_000_001;

  /**
   * Flag indicating whether the author should be retrieved from the custom header Author.
   */
  public boolean USE_AUTHOR_FROM_HEADER = false;

  /**
   * Endpoint which includes Maintenance and JOB-API.
   */
  public String HTTP_CONNECTOR_ENDPOINT;

  /**
   * If set to true, the service responses will include headers with information about the decompressed size of the request and response
   * payloads.
   */
  public boolean INCLUDE_HEADERS_FOR_DECOMPRESSED_IO_SIZE = true;

  /**
   * The name of the header for reporting the decompressed size of the response payload.
   */
  public String DECOMPRESSED_INPUT_SIZE_HEADER_NAME = "X-Decompressed-Input-Size";

  /**
   * The name of the header for reporting the decompressed size of the response payload.
   */
  public String DECOMPRESSED_OUTPUT_SIZE_HEADER_NAME = "X-Decompressed-Output-Size";

  /**
   * Name of the tag created for spaces with subscription.
   */
  public String SUBSCRIPTION_TAG = "xyz_ntf";

  /**
   * The port of the HTTPS listener.
   */
  public int XYZ_HUB_HTTPS_PORT;

  /**
   * The PEM encoded private key for server side TLS including header & footer.
   */
  public String XYZ_HUB_SERVER_TLS_KEY;

  /**
   * The PEM encoded public certificate(-chain) for server side TLS including header & footer.
   */
  public String XYZ_HUB_SERVER_TLS_CERT;

  /**
   * The PEM encoded certificate(-chain) to be used as truststore for client TLS authentication (mTLS) including header & footer.
   */
  public String XYZ_HUB_CLIENT_TLS_TRUSTSTORE;
  /**
   * S3/CW/Dynamodb localstack endpoints
   */
  public String LOCALSTACK_ENDPOINT;

  /**
   * A JSON String which holds the regional cluster mapping.
   */
  public Map<String, Object> XYZ_HUB_DEFAULT_STORAGE_REGION_MAPPING;

  public List<String> getDefaultStorageIds(String region) {
    if (XYZ_HUB_DEFAULT_STORAGE_REGION_MAPPING == null) {
      return null;
    }
    return (List<String>) XYZ_HUB_DEFAULT_STORAGE_REGION_MAPPING.get(region);
  }

  /**
   * If set to true, the settings configuration will be populated with settings defined in settings.json.
   */
  public boolean INSERT_LOCAL_SETTINGS;

  /**
   * When creating space, the default strategy is used if not informed on the request
   */
  public String DEFAULT_CONNECTOR_MAPPING_STRATEGY = ConnectorMapping.RANDOM.name();
}
