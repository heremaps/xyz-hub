package com.here.xyz.pub.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PubConfig {

    public boolean ENABLE_TXN_PUBLISHER;

    public long TXN_PUB_JOB_FREQ_MS;

}
