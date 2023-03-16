package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import com.here.xyz.models.geojson.implementation.XyzNamespace;

/**
 * The basic properties that all transactions will have.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class TransactionElement {
  /**
   * The unique transaction identifier.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public long txi;

  /**
   * The transaction connector-id, so the identifier of the connector that created the transaction.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public long txcid;

  /**
   * The Epoch timestamp in milliseconds when the transaction started.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public long txts;

  /**
   * The transaction number (UUID) as it will be stored within the XYZ namespace {@link XyzNamespace#getTxn() txn} property.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public String txn;

  /**
   * The space identifier, set as soon as the transaction becomes visible.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public String space;

  /**
   * The unique sequential identifier, set as soon as the transaction becomes visible. This is a sequential number without holes with the
   * lowest valid number being 1.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public long id;

  /**
   * The Epoch timestamp in milliseconds of when the transaction became visible (end of transaction).
   */
  @JsonProperty
  @JsonView(View.All.class)
  public long ts;
}

/*

xyz-psql -> Low Level code to access management and space database including transactions, features, history, ...
            CRUD
xyz-psql-processor -> Implements Event processing, so translation of events into calls into the low level psql code
xyz-hub-service    -> Implementation of the HUB REST API and some business logic like Auto-Merge on Conflict aso and generates
                      Events sent to the Processor
                      Manages spaces, subscriptions and stuff using directly the low level xyz-psql package

PsqlProcessor (xyz-psql-connector -> xyz-psql-processor)
  -> implementation to translate events to xyz-psql CRUD operations (eventually)

PsqlProcessorSequencer (requires one thread per xyz-psql)
  -> static init() (called from XYZ-Hub-Service)
  -> Thread that picks up all Connectors from the Connector-Cache (filled from XYZ-Hub-Service)
  -> Check if they use PsqlProcessor
  -> If they do, fork a new thread and start listen/fix loop
    -> ensure that when the connector config was modified, update PsqlPoolConfig
    -> optimization: avoid multiple threads for the same PsqlPoolConfig

Publisher (requires one thread per subscription part of xyz-txn-handler)
   -> reads the transactions, reads the features, and published
   -> getTransactions(..., limit ?) -> List<Transaction>
     -> getFeaturesOfTransaction(... limit 50) <-- List<Feature>
     -> publishing
     -> update our management database with what you have published

 */