# TODO (internal discussion)

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

## Processors

We will implement processors as a pipeline, where each processor can handle the event going to the
storage and handle the response coming from the storage. They must not store data in a database,
except for caching or configuration purpose. We modify the IEventProcessor so that it receives
a context instead of the event directly to allow this.

Space change planed:

```json
{
  "storage": {
    "id": "psql",
    "params": {},
    "processors": [
      {
        "id": "utm",
        "params": {}
      },
      {
        "id": "validateSchema",
        "params": {}
      }
    ]
  }
}
```

This builds up a pipeline like:

`EventContext <-> Utm <-> EventContext <-> ValidateSchema <-> EventContext <-> PsqlProcessor` 


