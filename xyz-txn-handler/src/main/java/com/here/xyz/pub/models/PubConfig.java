package com.here.xyz.pub.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PubConfig {

  // Existing XYZ configuration
  public String DEFAULT_STORAGE_ID;
  public String STORAGE_DB_URL;
  public String STORAGE_DB_USER;
  public String STORAGE_DB_PASSWORD;

  // Transaction Publisher specific configuration
  public boolean ENABLE_TXN_PUBLISHER;
  public long TXN_PUB_JOB_INITIAL_DELAY_MS;
  public long TXN_PUB_JOB_SUBSEQUENT_DELAY_MS;
  // One thread per subscription
  public int TXN_PUB_TPOOL_CORE_SIZE;
  public int TXN_PUB_TPOOL_MAX_SIZE;
  public long TXN_PUB_TPOOL_KEEP_ALIVE_SEC;
  // AWS connection
  public String AWS_ACCESS_KEY_ID;
  public String AWS_SECRET_ACCESS_KEY;
  public String AWS_DEFAULT_REGION = "us-east-1"; // default region

  // Transaction Sequencer specific configuration
  public boolean ENABLE_TXN_SEQUENCER;
  public long TXN_SEQ_JOB_INITIAL_DELAY_MS;
  public long TXN_SEQ_JOB_SUBSEQUENT_DELAY_MS;
  // One thread per connector (i.e. per SpaceDB)
  public int TXN_SEQ_TPOOL_CORE_SIZE;
  public int TXN_SEQ_TPOOL_MAX_SIZE;
  public long TXN_SEQ_TPOOL_KEEP_ALIVE_SEC;

  public static String XYZ_ADMIN_DB_CFG_SCHEMA = "xyz_config"; // default config schema
}
