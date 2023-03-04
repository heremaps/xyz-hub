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

    // Transaction Sequencer specific configuration
    public boolean ENABLE_TXN_SEQUENCER;
    public long TXN_SEQ_JOB_INITIAL_DELAY_MS;
    public long TXN_SEQ_JOB_SUBSEQUENT_DELAY_MS;
    // One thread per connector (i.e. per SpaceDB)
    public int TXN_SEQ_TPOOL_CORE_SIZE;
    public int TXN_SEQ_TPOOL_MAX_SIZE;
    public long TXN_SEQ_TPOOL_KEEP_ALIVE_SEC;

    // XYZ Config Schema
    public static String XYZ_ADMIN_DB_CFG_SCHEMA = "xyz_config";
}
