# Naksha-Hub

This document describes the basic concepts of the Naksha-Hub with a concrete [reference implementation](./PSQL.md) based upon [PostgresQL database](https://www.postgresql.org/docs/).

## Terminology

Naksha uses the following terms:

- **Feature**: An object that is compatible to the [GeoJSON](https://tools.ietf.org/html/rfc79460) format.
- **Collection**: A set of features with history and transaction log.
- **Storage**: A physical storage to persist collections. Naksha accesses storages directly, therefore storage drivers need to be open source and part of the Naksha repository, implementing the IStorage interface.
- **EventHandler**: Some code that statically linked into the Naksha-Hub, referred to via the class-name. 
- **Connector**: A configuration declaring, which class implements the IEventHandler interface. An event handler normally implements some business logic. If the “remote” property set, then the configuration declares that the code is located on a remote service. This causes a proxy class to be instantiated, which will forward the event to the remote service. In this case it will embed the configuration (the connector) into the event, before sending it to the remote host. 
- **PipelineComponent**: A component processing events via an event-pipeline. The event-pipeline is part of the configuration of the pipeline-component.
- **Space**: A pipeline-component with some configuration and a public name. It receives an event from a REST API, sending this event through the attached event-pipeline. A space can be used for any purpose, mostly used to store and modify features in a storage.
- **Subscription**: A component configuration to react upon content changes of a **storage**. When a storage driver creates a new event to inform Naksha-Hub about a change of the underlying storage, then Naksha will review all subscription to this storage and process these events in the corresponding event-pipelines. A subscription will be executed for all change-events and guaranteed to be called at least ones. That means if an error occurs while processing an event, the subscription pipeline receives the event again. All subscriptions are independent of each other. Naksha calls individual subscriptions in parallel, but each subscription receive all events in order. A subscription is guaranteed to be called at least ones at one Naksha instance at a time, but it can be moved between instances on demand.
- **AID/AppId**: The identifier of the acting application, read from the JWT token.
- **Author**: A user or application identifier read from the JWT token.

## Spaces

A space is an external identifier used to create event-pipelines to process events created through the REST API. The relation between a **space** and a **collection** is transparent and an `n:m`, therefore multiple spaces can refer to a single collection or one space can refer to multiple collections.

Examples:

- A space does not need to be backed by any storage at all, it can as well just implement business logic, for example posting features to the space may only perform a transformation and then just return the modified features (`n:0`).
- A space can be created to enable read-only access to a collection, while another space allows read-write access to the same collection. This helps with making the read-only space publicly accessible for anyone, while preventing arbitrary write access (`n:1`).
- A space can refer to multiple collections using the [view](./VIEW.md). A view combines multiple spaces into one logical space, therefore any query to this view effectively distributed the query across multiple spaces and thereby optionally as well across multiple collections (`n:m`).
- A space can use a connector that internally splits features by type into different collections and then stores them in multiple collections (`1:n`).

## Storage

A storage will physically persist features. All storages backing should logically operate the same way. The PostgresQL reference implementation can be used as a starting point, when doing own implementations. All storages of Naksha should have two basic elements:

- Transaction logs.
- Feature collections.

A storage is an abstract entity that has a 40-bit unsigned integer as unique identifier. A storage can be used by connectors, subscriptions or jobs.

## Collections

A collection is a logical set of features including their history, as reflected through the transaction log. Every collection **must** have a HEAD state. The HEAD state represents the live features stored in this collection.

Every collection does have two sub-sets, the historic feature states and the deleted features. The deleted features are used in some special cases, for example the [view concept](./VIEWS.md).

A collection can be exposed through multiple ways and must be concurrency safe. So changes done through multiple different ways must not harm each other. In other words, multiple spaces can refer to the same storage, even using different connectors with different implementations.

Every collection does have the following meta-data that the storage somehow need to support:

- `maxAge`: The maximum age of features in the history of this collection, given in days. A value
            between `0` and `9,223,372,036,854,775,807` (`2^63-1`).
- `history`: A boolean to enable or disable writing of the history (default: `true`). This can be
             used to temporally disable history writing, for example for bulk-loads.
- `deleteAt`: An UNIX Epoch timestamp in milliseconds when to delete this collection. A value of 
              zero means "do not delete". If set, then the storage can (but does not need to) drop
              all data (HEAD, history, deletions, transactions ...) after this time. Normally,
              a deletion of a collection is done soft, so that it can be un-done within a
              certain timeframe (which is why a `deleteAt` is normally set into the future).

### @ns:com:here:xyz

Every feature has at least one property within `properties`, being a map with the key `@ns:com:here:xyz`. The map stored under this key must have the following layout:

- **action**: The operation has been performed being `CREATE`, `UPDATE`, `DELETE` or `PURGE`.
- **version**: The version of the features, a sequentially consistent numbering.
- **author**: The author of the feature.
- **appId**: The application-id of the application that created the state.
- **uuid**: The unique state identifier, stored as UUID (must not be `null`).
- **puuid**: The UUID of the previous state, `null` if `CREATE`.
- **muuid**: When this feature state is the result of an automatic merge, then the UUID of merged state, `null` if `CREATE`.
- **txn**: The transaction UUID (must not be `null`).
- **createdAt**: The unix epoch timestamp in millis of when the feature has been created.
- **updatedAt**: The unix epoch timestamp in millis of when this state has been created.
- **rtcts**: The real-time unix epoch timestamp in millis of when the feature has been created.
- **rtuts**: The real-time unix epoch timestamp in millis of when this state has been updated.
- **tags**: An array of strings representing searchable tags. They are used to limit operations.
- ~~space~~: This property is dynamically added by the Naksha-Hub and must not be part of the storage. The storage **must** remove this property.

The difference between **createdAt**/**updatedAt** and **rtcts**/**rtuts** is that the former reflect the transaction start time (so when the transaction started). This time is searchable and reflects in which partition the feature is stored, while the latter reflects the clock time, when the update was really been performed. Therefore, all creations and updates in the same transaction have the same **createdAt** and **updatedAt** time, while their **rtcts**/**rtuts** can differ drastically, when looking at long-running transactions.

Naksha-Hub does not support searching for the real-time, therefore storages do not need to support this either, it is informative only. The reason is the data partitioning by the transaction time (reflected in **createdAt**/**updatedAt**).

## Transaction Logs

All transactions are sets of transaction events. Each transaction event is either related to a collection, or it is a comment. The following event actions do exist:

- `TxComment`: An arbitrary comment added to a transaction.
- `TxModifyFeatures`: An event to signal that the features within a collection have been modified.
- `TxModifyCollection`: An event to signal that a collection has been modified.

All transaction events are groups in transaction-sets in which all events must have the same transaction number (`txn`).

## UUIDs

Naksha uses `UUID`s to uniquely identify every state of a feature stored in a collection and to uniquely identify transactions. Within the PostgresQL implementation they are generated using triggers to ensure that even queries executed in the database directly update the `UUID`s of the features and transactions.

The **UUID** will be in the format of a [random UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier), so being version 4, variant 1 (big endian encoded). However, they are not random, but reflect a guaranteed unique identifier. The format chosen matches perfectly well with how PostgresQL [compares UUIDs](https://doxygen.postgresql.org/uuid_8c.html#aae2aef5e86c79c563f02a5cee13d1708).

As seen in the source code, the compare of **UUID**s is done as:

```C
return memcmp(arg1->data, arg2->data, UUID_LEN);
```

[memcp](https://cplusplus.com/reference/cstring/memcmp/) does compare simply the bytes in order (so basically using big-endian order). Note that can be done the same in all other programming languages and implementations.

### UUID Encoding

All **UUID**s encode the following values:

```
   1   2     3    e    4    5    6    7 -   e    8    9    b-   4    2    d    3  -   a    4    5     6-   4    2    6    6     1    4    1    7    4    0    0    0
|          time low                   |  |    time mid     | |ve| | time high  |    va|  clock_seq     |              node                                         |
7654_3210-7654_3210-7654_3210-7654_3210--7654_3210-7654_3210-7654_3210-7654_3210----7654_3210-7654_3210-7654_3210-7654_3210--7654_3210-7654_3210-7654_3210-7654_3210
iiii_iiii-iiii_iiii-iiii_iiii-iiii_iiii--iiii_iiii-iiii_iiii-1000_iiii-iiii_iiii----10YY_YYYY-YYYY_mmmm-DDDD_Dttt-SSSS_SSSS--SSSS_SSSS-SSSS_SSSS-SSSS_SSSS-SSSS_SSSS
[                   object-id                              ] ver  [    id      ]    va[   year   ][mon ][day ][t] [               storage-id                       ] 
                                                             =4                     =1

object-id ::60 = The object identifier.
ver       ::4  = The fixed UUID version, 4-bit binary value 4 (binary 1000).
va        ::2  = The fixed variant, 2-bit binary value 2 (binary 10).
year      ::10 = The biased year (year - 2000), resulting in possible years 2000 to 3023.
month     ::4  = The month of the year (1 to 12).
day       ::5  = The day of the month (1 to 31).
t         ::3  = The object type. 
storage-id::40 = The storage identifier.
```

**Note**:
- The version (`ver`) is always 4 (binary `1000`).
- The variant (`va`) is always variant one (big endian) with value being decimal 2 (binary `10`).

Therefore, every Naksha **UUID** refers to an object of a specific type and additionally encodes the storage identifier that was used to create the state. The storage-id is important when for example the transaction **UUID** is given to Naksha, with the request to fetch the transaction. In this case Naksha needs to know where the transaction logs are located, and it will be able to do so using the storage-id encoded in the transaction **UUID**. If no such storage exists anymore, Naksha will be unable to fulfill the request.

The objects that can be a target are currently only two:

- `0`: Transaction
- `1`: Feature

The values `2` and `3` are reserved for future use.

### Date (day,month,year)

The date encoded in the **UUID** is used to partition data and when searching within a certain time window.

### Transaction UUID

All changes being part of the same transaction need to have the same unique transaction number. Transaction numbers are globally unique.

### Feature UUID

Every state of every feature stored in a collection will have a globally unique feature **UUID**.
