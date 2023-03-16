package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.models.geojson.implementation.XyzNamespace;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class TransactionElement {

  /**
   * The unique transaction identifier.
   */
  @JsonProperty
  public long txi;

  /**
   * The Epoch timestamp in milliseconds when the transaction started.
   */
  @JsonProperty
  public long txts;

  /**
   * The transaction number as it will be stored within the {@link XyzNamespace}.
   */
  @JsonProperty
  public String txn;

  /**
   * The space, set by the transaction fix job as soon as the transaction becomes visible.
   */
  @JsonProperty
  public String space;

  /**
   * The unique sequential identifier, set by the transaction fix job as soon as the transaction becomes visible. This is a sequential
   * number without holes.
   */
  @JsonProperty
  public long id;

  /**
   * The Epoch timestamp in milliseconds of when the transaction became visible for the transaction fix job.
   */
  @JsonProperty
  public long ts;


//  ||'i           BIGSERIAL PRIMARY KEY NOT NULL, '
//      ||'txid        int8 NOT NULL, '
//      ||'txi         int8 NOT NULL, '
//      ||'txcid       int8 NOT NULL, '
//      ||'txts        timestamptz NOT NULL, '
//      ||'txn         uuid NOT NULL, '
//      ||'"schema"    text COLLATE "C" NOT NULL, '
//      ||'"table"     text COLLATE "C" NOT NULL, '
//      ||'commit_msg  text COLLATE "C", '
//      ||'commit_json jsonb, '
//      ||'space       text COLLATE "C", '
//      ||'id          int8, '
//      ||'ts          timestamptz'
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