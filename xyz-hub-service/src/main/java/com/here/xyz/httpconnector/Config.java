package com.here.xyz.httpconnector;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * The http-connector configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Config {

  /**
   * Whether the service should use InstanceProviderCredentialsProfile with cached credential when utilizing AWS clients.
   */
  public boolean USE_AWS_INSTANCE_CREDENTIALS_WITH_REFRESH;

  /**
   * The arn of the secret (in Secret Manager) that contains bot credentials.
   */
  public String JOB_BOT_SECRET_ARN;
  /**
   * The port of the HTTP server.
   */
  public int HTTP_PORT;
  /**
   * ECPS_PHRASE of Default Connector
   */
  public String ECPS_PHRASE;
  /**
   * Max number of parallel running Maintenance Tasks
   */
  public int MAX_CONCURRENT_MAINTENANCE_TASKS;
  /**
   * Defines the time threshold in which a maintenance should be finished. If its reached a warning gets logged.
   */
  public int MISSING_MAINTENANCE_WARNING_IN_HR;
  /**
   * ARN of DynamoDB Table for JOBs
   */
  public String JOBS_DYNAMODB_TABLE_ARN;
  /**
   * S3/CW/Dynamodb localstack endpoints
   */
  public String LOCALSTACK_ENDPOINT;
  /**
   * S3 Bucket for imports/exports
   */
  public String JOBS_S3_BUCKET;
  /**
   * Region in which components are running/hosted
   */
  public String JOBS_REGION;
  /**
   * Set interval for JobQueue processing
   */
  public int JOB_CHECK_QUEUE_INTERVAL_MILLISECONDS;
  /**
   * List of "connectorId:cloudWatchDBInstanceIdentifier:MaxCapacityUnits"
   */
  public List<String> JOB_SUPPORTED_RDS;
  /**
   * RDS maximum ACUs
   */
  public int JOB_MAX_RDS_CAPACITY;
  /**
   * RDS maximum CPU Load in percentage
   */
  public int JOB_MAX_RDS_CPU_LOAD;
  /**
   * RDS maximum allowed import bytes
   */
  public long JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES;
  /**
   * RDS maximum allowed idx creations in parallel
   */
  public int JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS;
  /**
   * RDS maximum allowed imports in parallel
   */
  public int JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES;
  /**
   * RDS maximum allowed imports in parallel
   */
  public int JOB_MAX_RDS_MAX_RUNNING_EXPORT_QUERIES;
  /**
   * RDS maximum allowed imports in parallel
   */
  public Long JOB_DYNAMO_EXP_IN_DAYS;
  /**
   * DB Pool size per client
   */
  public Integer JOB_DB_POOL_SIZE_PER_CLIENT;

  /** ############## Database related ##################### */
  /**
   * Statement Timeout in Seconds
   */
  public int DB_STATEMENT_TIMEOUT_IN_S;
  /**
   * Initial Connection-Pool Size
   */
  public int DB_INITIAL_POOL_SIZE;
  /**
   * Min size of Connection-Pool
   */
  public int DB_MIN_POOL_SIZE;
  /**
   * Max size of Connection-Pool
   */
  public int DB_MAX_POOL_SIZE;
  /**
   * How many connections should get acquired if the pool runs out of available connections.
   */
  public int DB_ACQUIRE_INCREMENT;
  /**
   * How many times will try to acquire a new Connection from the database before giving up.
   */
  public int DB_ACQUIRE_RETRY_ATTEMPTS;
  /**
   * Max Time to wait for a connection checkout - in Seconds
   */
  public int DB_CHECKOUT_TIMEOUT;
  /**
   * Test on checkout if connection is valid
   */
  public boolean DB_TEST_CONNECTION_ON_CHECKOUT;

  /** Store Jobs inside DB - only possible if no JOBS_DYNAMODB_TABLE_ARN is defined */
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
   * Hub-Endpoint
   */
  public String HUB_ENDPOINT;
}
