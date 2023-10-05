# PostgresQL Reference Implementation

This part of the documentation explains details about the reference Naksha Storage-API implementation based upon PostgresQL database.

## Basics

The admin database of Naksha will (currently) be based upon a PostgresQL database. For users, we will allow the usage of the same implementation for their collections that the Naksha-Hub itself uses to store administrative features.

## Collections

Within PostgresQL a collection is a set of database tables. All these tables are prefixed by the `collection` identifier. The tables are:

- `{collection}`: The HEAD table with all features in their live state. This table is the only one that should be directly accessed for manual SQL queries and has triggers attached that will ensure that the history is written accordingly.
- `{collection}_del`: The HEAD deletion table holding all features that are deleted from the HEAD table. This can be used to read zombie features (features that have been deleted, but are not yet fully unrecoverable dead)
- `{collection}_hst`: The history view, this is partitioned table, partitioned by `txn`.
- `{collection}_hst_{YYYY}_{MM}_{YY}`: The history partition for a specific day (`txn >= naksha_txn('2023-09-29',0) AND txn < naksha_txn('2023-09-30',0)`.
- `{collection}_meta`: The meta-data sub-table, used for arbitrary meta-information and statistics.

## Triggers

Naksha PSQL-Storage will add two triggers to the HEAD table to ensure the desired behavior, even when direct SQL queries are executed. This trigger implements the following behavior (it basically exists in two variants: with history enabled or disabled):

1. On INSERT
   * Fix the XYZ namespace `txn=naksha_txn(), txn_next=0, app_id=, author=, ...`
   * Update the transaction-log
   * Delete the inserted feature (by id) from the `{collection}_del`
2. On UPDATE
   * Fix the XYZ namespace `txn=naksha_txn(), txn_next=0, app_id=, author=, ...`
   * Update the transaction-log
   * Delete the updated feature (by id) from the `{collection}_del`
   * If history is enabled:
     * Ensure that the history partition exists
     * Update XYZ namespace of OLD: `txn_next=naksha_txn()` (only this!)
     * INSERT a copy of OLD into `{collection}_hst`
     * This basically creates a backup of the old state, linked to the new HEAD state.
3. On DELETE
  * If history is enabled:
    * Ensure that the history partition exists
    * Update XYZ namespace of OLD: `txn_next=naksha_txn()` (only this!)
    * INSERT a copy of OLD into `{collection}_hst`
    * This basically creates a backup of the old state (in action UPDATE or INSERT), linked to the new HEAD state.
  * Update XYZ namespace of OLD: `action=DELETED, txn_next=0, uuid=, puuid=, app_id=, author=, ...` (full update)
  * UPSERT a copy of OLD state into the `{collection}_del`
    * This boils down to creating a new state and then copy it into the deletion table
    * Therefore: When the client requests deleted features, we can simply read them from the deletion table
  * If history is enabled:
    * Update XYZ namespace of OLD: `txn_next=naksha_txn()` (only this!)
    * INSERT a copy of OLD into `{collection}_hst`
    * This basically creates a backup of the DELETE state.

Beware, part of the XYZ-namespace fixing is the calculation of the **hrid** from the _geometry_ using [`ST_Centroid(coalesc(geo, ST_Point(0,0)))`](https://postgis.net/docs/ST_Centroid.html).

Naksha will implement a special PURGE operation, which will remove elements from the special deletion table (which is a special HEAD table).

## Transaction-Number [`txn`]

All features stored by the Naksha storage engine are part of a transaction. The data is stored partitioned, because of the huge amount of data that normally need to be handled. To instantly know where a feature is located, so in which partition, we need to ensure that the unique identifier of a transaction holds the partition key. Partitioning is done using the _next transaction-number_.

The transaction-number is a 64-bit integers, split into four parts:

* _Year_: The year in which the transactions started (e.g. 2023).
* _Month_: The month of the year in which the transaction started (e.g. 9 for September).
* _Day_: The day of the month in which the transaction started (1 to 31).
* _Sequence_: The local sequence-number in this day.

The local sequence-number is stored in a sequence named `naksha_tx_object_id_seq`. Every day starts with the sequence-number reset to zero. The final value is calculated by **year** x _1,000,000,000,000,000_ + **month** x _10,000,000,000,000_ **day** x _100,000,000,000_ + **sequence-number**.

This concept allows up to 100 billion transactions per day. It will work up until the year 9222, where it will overflow. Should there be more than 100 billion transaction in a single day, this will overflow into the next day and potentially into an invalid day, should it happen at the last day of a given month. We ignore this situation, it seems currently impossible.

Demo calculation:

```
Date: 2023-01-30 (YYYY-MM-DD)
Max value int8:                  9,223,372,036,854,775,807
  2023 * 1,000,000,000,000,000 = 2,023,000,000,000,000,000
     1 *    10,000,000,000,000 =        10,000,000,000,000
    30 *       100,000,000,000 =         3,000,000,000,000
                               = 2,023,013,000,000,000,000
Human-Readable:                  2023,01,30,0
uuid:                            "{storageId}:{collectionId}:2023:1:30:0"
```

Normally, when a new unique identifier is requested, the method `naksha_txn()` will read the sequence (`naksha_tx_object_id_seq`) and verify it, so, if the year, month and day of the current transaction start-time (`now()`) matches the one stored in the sequence-number. If this is not the case, it will enter an advisory lock and retry the operation, if the sequence-number is still invalid, it will reset the sequence-number to the correct date, with the _sequence_ part being set to `1`, so that the next `naksha_uid(collection)` method that enters the lock or queries the _sequence_, receives a correct number, starting at _sequence_ `1`. The method itself will use the _sequence_ value `0` in the rollover case.

**Note**: We store the current transaction-number for the current transaction in a transaction local config value, so that we only need to read it ones, when needed the first time in a transaction. Additionally, there is a helper methods to be used to create arbitrary bounds for a unique identifier being `naksha_txn(timestamptz, bigint)`.

## GUID _(aka UUID)_

Traditionally XYZ-Hub used UUIDs as state-identifiers, but exposed them as strings in the JSON. Basically all known clients ignore the format of the UUID, so none of them expected to really find a UUID in it. This is good, because in Naksha we decided to change the format, but to stick with the name for downward compatibility.

The new format is called GUID (global unique identifier), returning to the roots of the Geo-Space-API. The syntax for a GUID in the PSQL-storage is:

`{storageId}:{collectionId}:{year}:{month}:{day}:{id}`

**Note**: This format holds all information needed for Naksha to know in which storage a feature is located, of which it only has the _GUID_. The PSQL storage knows from this _GUID_ exactly in which database table the features is located, even taking partitioning into account. The reason is, that partitioning is done by transaction start date, which is contained in the _GUID_. Therefore, providing a _GUID_, directly identifies the storage location of a feature, which in itself holds the information to which transaction it belongs to (`txn`). Beware that the transaction-number as well encodes the transaction start time and therefore allows as well to know exactly where the features of a transaction are located (including finding the transaction details itself).

## Table layout

The table layout for all the tables of a collection is the same and 100% downward compatible with what previously used in XYZ-Hub:

| Column    | Type                      | Modifiers            | Description                                                     |
|-----------|---------------------------|----------------------|-----------------------------------------------------------------|
| i         | int8                      | PRIMARY KEY NOT NULL | Primary row identifier.                                         |
| geo       | geometry(GeometryZ, 4326) |                      | The geometry of the features, extracted from `feature->>'geo'`. |
| jsondata  | jsonb                     |                      | The Geo-JSON feature.                                           |

The XYZ-Hub later added a new column `deleted` as `BOOLEAN DEFAULT FALSE`, this is no longer supported and simply ignored by Naksha-Hub.

The only minor change is that `i` becomes a primary key and is no `BIGSERIAL` any longer, because the triggers we add now increment `i` by them self. It will not harm, if it is till an auto-sequence, just this will consume one additional number here and there, not needed otherwise. Another changes implied is how `i` is used, it will be incremented with any change done to a row, so it no longer uniquely identifies a row, but a state identifier. This is a major semantic difference that implies, you can move existing tables into Naksha and back to XYZ-Hub, but you can't possibly use Naksha and XYZ-Hub in parallel with the same table, because they have different semantics for `i`.

## Collection-Info

All collections do have a comment on the HEAD table, which is a JSON objects with the following setup:

| Property              | Type   | Meaning                                                                                                                           |
|-----------------------|--------|-----------------------------------------------------------------------------------------------------------------------------------|
| id                    | String | The collection name again.                                                                                                        |
| estimatedFeatureCount | long   | The estimated total amount of features in the collection.                                                                         |
| byteSize              | long   | The maximum size of the raw JSON of features in this collection.                                                                  |
| compressedSize        | long   | The maximum compressed size of the JSON of features in this collection.                                                           |
| maxAge                | long   | The maximum amount of days of history to keep (defaults to 9,223,372,036,854,775,807).                                            |
| historyEnabled        | bool   | If `true`, history will be written (defaults to _true_).                                                                          |
| idPrefix              | String | If not `null` and a feature is inserted, updated or deleted with an **id** that starts with this string, the **id** is truncated. |
| author                | String | If not `null` and a feature is inserted without an author, this value will be used.                                               |

## Optimal Partitioning

Naksha supports optimal partitioning. It creates as a background job a statistic for every new version appearing in the transaction table. The statistic basically creates an optimal partitioning distribution. It will try to merge as many features into each partition, so that not more than 10mb of raw-json is stored in each partition (using the `byteSize` information of the _collection-info_ to decide for the optimal number of features per partition). It will basically perform the partitioning based upon the spatial distribution, using the same schema that the HERE tiles are using. Therefore the auto-partitioning will use as well the HERE-ref-ids, but in mixed level, automatically detected by feature count and size.

The optimal partitioning is basically just a special feature that is stored within the meta-table of a collection. The properties of this features should basically hold a property `by_hrid`, which is a map like `Map<HereRefIdPrefix, FeatureCount>` and `by_crid`, which is a map like `Map<CustomRefIdPrefix, FeatureCount>`.

## Indices and Queries

The indices are all created only directly on the partitions. To keep the documentation, we use the shortcut `xyz` as alias for `jsondata->'properties'->'@ns:com:here:xyz'`.

* The index on `i` is created automatically, because it is a primary key.
  * Used to search for a specific state of a feature, when given the **uuid** of the feature.
  * The **uuid** contains the transaction time, this can be used to optimize the query to only look up the HEAD partition and one specific history partition.
  * `SELECT * FROM ${table} WHERE i = i UNION ALL SELECT * FROM ${table}_hst WHERE txn >= naksha_txn($date,0) AND txn < naksha_txn($date+1, 0) AND i = $i`
* `CREATE INDEX ... USING btree (id text_pattern_ops ASC, xyz.txn DESC, xyz.txn_next DESC)`
  * Used to search features by **id**, optionally in a specific version.
* `CREATE INDEX ... USING btree (xyz.crid text_pattern_ops ASC, xyz.txn DESC, xyz.txn_next DESC) INCLUDE id WHERE xyz.crid IS NOT NULL`
  * Used to search for all features customer ref-ids.
  * Used to calculate the optimal partitioning.
* `CREATE INDEX ... USING btree (xyz.href text_pattern_ops ASC, xyz.txn DESC, xyz.txn_next DESC) INCLUDE id`
  * Used to search for HERE ref-ids.
  * Used to calculate the optimal partitioning.
* `CREATE INDEX ... USING gist[-sp] (geo, xyz.txn, xyz.txn_next)`
  * Search for features intersecting a geometry, optionally in a specific version.
  * We use `gist-sp` only when there are only point features in the collection.
* `CREATE INDEX ... USING btree (xyz.action text_pattern_ops ASC, xyz.author text_pattern_ops DESC, xyz.txn DESC, xyz.txn_next)`
  * Search for features with a specific action (CREATE, UPDATE or DELETE). Because the cardinality is very low, there are sub-indices.
  * Search for features that where updated by a specific author, optionally limited to a specific version.
* `CREATE INDEX ... USING gin (xyz.tags array_ops, xyz.txn, xyz.txn_next)`
  * Used to search for tags, optionally in a specific version.

## Transaction Logs

The transaction logs are stored in the `naksha_tx` table. Each transaction persists out of **signals**, grouped by the transaction-number (`tnx`). The table layout is:

| Column     | Type         | Modifiers            | Description                                                                                                        |
|------------|--------------|----------------------|--------------------------------------------------------------------------------------------------------------------|
| i          | bigserial    | PRIMARY KEY NOT NULL | Primary unique signal identifier.                                                                                  |
| ts         | timestamptz  |                      | The time when the transaction started. The year, month and day are encoded as well in the `txn`.                   |
| psql_id    | int8         |                      | The PostgresQL transaction id. Can be used to detect consistency.                                                  |
| publish_id | int8         |                      | The publication identifier, set by the sequencer of `lib-naksha-psql`, as soon as the transaction becomes visible. |
| publish_ts | timestamptz  |                      | The publication time, set by the sequencer of `lib-naksha-psql`, as soon as the transaction becomes visible.       |
| txn        | int8         | NOT NULL             | The transaction-number to which this signal belongs.                                                               |
| action     | text         |                      | The action that this signal represents.                                                                            |
| id         | text         |                      | The unique transaction local identifier of this signal.                                                            |
| app_id     | text         |                      | The application identifier used for the transaction.                                                               |
| author     | text         |                      | The author used for the transaction.                                                                               |
| msg_text   | text         |                      | If this signal is a message, the text of the message.                                                              |
| msg_json   | jsonb        |                      | If this signal is a message, the JSON attachment.                                                                  |
| msg_binary | bytea        |                      | If this signal is a message, the binary attachment.                                                                |

The transaction-log should have a combined unique index on (**txn**, **action**, **id**).

**Note**: The transaction table itself is partitioned by month, so there will be child tables aka `transactions_YYYY_MM`.

### Actions

| Action            | Meaning                                                                              |
|-------------------|--------------------------------------------------------------------------------------|
| MODIFY_FEATURES   | Features in the collection are modified. The **id** is the collection name.          |
| MODIFY_COLLECTION | A collection itself was modified. The **id** is the collection name.                 |
| MESSAGE           | An arbitrary message added into the transaction. The **id** is a message identifier. |

## History creation

To manage the history the Naksha PostgresQL library will add a “before” trigger, and an “after” trigger to all main HEAD tables. The “before” trigger will ensure that the XYZ namespace is filled correctly, while the “after” trigger will write the transaction into the history, update the transaction table and move deleted features into the deletion HEAD table.

The history can be enabled and disabled for every space using the following methods:

- `naksha_collection_disable_history('collection')`
- `naksha_collection_enable_history('collection')`

## Sequencer

The last step is a background job added into the `lib-naksha-psql` that will “publish” the transactions. The job will set the `publish_id` and `publish_ts` to signal the visibility of a transaction and to generate a sequential numeric identifier. The job guarantees that the `pubish_id` has no holes (is continues) and is unique for every transaction. Note that the `publish_id` itself is not unique, multiple events in the transaction table can belong to the same `publish_id`. 

The author and application identifier must be set by the client before starting any transaction via `SELECT naksha_tx_start('{app_id}', '{author}');`. Note that the **author** is optional and can be `null`, but the application identifier **must not** be `null` or an empty string. If the author is `null`, then the current author stays the author for all updates or deletes. New objects in this case, are set to the application identifier, so that the application gains authorship.

In the context of [HERE](https://here.com), the **author** and **app_id** are set to the **UPM user-identifier** as received from the **Wikvaya** service, therefore coming from the **UPM** (*User Permission Management*). Technically the `lib-naksha-psql` will treats all these values just as strings and does not imply and meaning to them, so the service can be used for any other authentication system. However, in the context of [HERE](https://here.com) it is a requirement to use UPM-identifiers.
