package com.here.mapcreator.ext.naksha;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** The database code will send notifications in this format. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PsqlNotification {

    public static final String TXN = "txn";
    public static final String TXI = "txi";

    /** The channel on which notifications are send. */
    public static final String CHANNEL = "naksha:notifications";

    /** The transaction that happened. */
    @JsonProperty(TXN)
    public String txn;

    /** Unique transaction identifier. */
    @JsonProperty(TXI)
    public String txi;
}
