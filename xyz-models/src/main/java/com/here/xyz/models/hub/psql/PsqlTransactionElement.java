package com.here.xyz.models.hub.psql;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.TransactionElement;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class PsqlTransactionElement extends TransactionElement {

  /**
   * The unique row identifier.
   */
  @JsonProperty
  public long i;

  /**
   * The PostgresQL transaction identifier (can be used internally in PostgresQL to verify if the transaction is eventually consistent).
   */
  @JsonProperty
  public long txid;

  /**
   * The transaction number as it will be stored within the {@link XyzNamespace}.
   */
  @JsonProperty
  public String txn;
}
