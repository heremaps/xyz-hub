package com.here.xyz.models.hub.psql;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.models.hub.TransactionCommitMessage;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class PsqlTransactionCommitMessage extends TransactionCommitMessage {

}
