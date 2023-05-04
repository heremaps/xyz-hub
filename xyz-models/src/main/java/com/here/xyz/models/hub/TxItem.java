package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import org.jetbrains.annotations.NotNull;

/**
 * A transaction persists out of multiple transaction items. Each item represents something that has happened to a collection.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class TxItem {

  @JsonCreator
  public TxItem(@JsonProperty @NotNull String id) {
    this.id = id;
  }

  /**
   * The unique identifier of this transaction item; zero for not persisted transaction items. Except for the
   * {@link TxAction#COMMIT_MESSAGE}, this normally is the same as {@link #collection}.
   */
  @JsonProperty
  @JsonView(All.class)
  public final @NotNull String id;

  /**
   * The action that this transaction item represents.
   */
  @JsonProperty
  @JsonView(All.class)
  public TxAction action;

  /**
   * The collection that this transaction item is related to.
   */
  @JsonProperty
  @JsonView(All.class)
  public String collection;

  /**
   * The space identifier as known by the client when performing the transaction.
   */
  @JsonProperty
  @JsonView(All.class)
  public String space;

  /**
   * The unique transaction number (UUID) as it will be stored within the XYZ namespace {@link XyzNamespace#getTxn() txn}. All items of a
   * transaction have the same transaction number.
   */
  @JsonProperty
  @JsonView(All.class)
  public String txn;

  /**
   * The connector-number of the connector that created the transaction.
   */
  @JsonProperty("cn")
  @JsonView(All.class)
  public long connectorNumber;

  /**
   * The Epoch timestamp in milliseconds when the transaction started.
   */
  @JsonProperty
  @JsonView(All.class)
  public long startTs;

  /**
   * An optional message added to this item; only used for the action {@link TxAction#COMMIT_MESSAGE}.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  public String message;

  /**
   * An optional JSON attachment added to this item; only used for the action {@link TxAction#COMMIT_MESSAGE}.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  public Object attachment;

  /**
   * The unique sequential publishing identifier of the transaction, set as soon as the transaction becomes visible. This is a sequential
   * number without holes, with the lowest valid number being 1. It is set by the maintenance client. All items of a transaction will have
   * the same publish identifier.
   */
  @JsonProperty
  @JsonView(All.class)
  public long publishId;

  /**
   * The Epoch timestamp in milliseconds of when the transaction became publicly visible, set by the maintenance client.
   */
  @JsonProperty
  @JsonView(All.class)
  public long publishTs;
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