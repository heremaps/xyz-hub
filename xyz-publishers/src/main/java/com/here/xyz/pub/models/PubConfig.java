package com.here.xyz.pub.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PubConfig {

    // Existing XYZ configuration
    public String STORAGE_DB_URL;
    public String STORAGE_DB_USER;
    public String STORAGE_DB_PASSWORD;

    // Publisher specific new configuration
    public boolean ENABLE_TXN_PUBLISHER;
    public long TXN_PUB_JOB_FREQ_MS;
    public int TXN_PUB_TPOOL_CORE_SIZE;
    public int TXN_PUB_TPOOL_MAX_SIZE;
    public long TXN_PUB_TPOOL_KEEP_ALIVE_SEC;

    // XYZ Config Schema
    public static String XYZ_ADMIN_DB_CFG_SCHEMA = "xyz_config";
}
