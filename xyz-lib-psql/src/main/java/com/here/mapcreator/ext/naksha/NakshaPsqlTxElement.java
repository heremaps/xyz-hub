package com.here.mapcreator.ext.naksha;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import com.here.xyz.models.hub.TxItem;
import org.jetbrains.annotations.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class NakshaPsqlTxElement extends TxItem {

  public NakshaPsqlTxElement(@NotNull String id) {
    super(id);
  }

  /**
   * The unique row identifier.
   */
  @JsonProperty
  @JsonView(View.Private.class)
  public long i;

  /**
   * The PostgresQL transaction identifier (can be used internally in PostgresQL to verify if the transaction is eventually consistent).
   */
  @JsonProperty
  @JsonView(View.Private.class)
  public long txid;

  /**
   * The database schema that was affected.
   */
  @JsonProperty
  @JsonView(View.Private.class)
  public String schema;
}