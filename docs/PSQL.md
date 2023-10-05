# PostgresQL Reference Implementation

This part of the documentation explains details about the reference Naksha Storage-API implementation based upon PostgresQL database.

## Basics

The admin database of Naksha will (currently) be based upon a PostgresQL database. For users, we will allow the usage of the same implementation for their collections that the Naksha-Hub itself uses to store administrative features.

## Collections

Within PostgresQL a collection is a set of database tables. All these tables are prefixed by the `collection` identifier. The tables are:

- `{collection}`: The HEAD table partition with all features in their live state (`txn_next == 0`). This table is the only one that should be directly accessed for manual SQL queries and has triggers attached that will ensure that the history is written accordingly.
- `{collection}_del`: The HEAD deletion table holding all features that are deleted from the HEAD table, but are not yet purged. When using a table in moderation mode or in a view, the deleted features are read together with those from the HEAD table.
- `{collection}_hst`: The history table, that partitions history using `txn_next`.
- `{collection}_hst_{YYYY}_{MM}_{YY}`: The history partition for the days (`txn_next >= naksha_txn('2023-09-29',0) AND txn_next < naksha_txn('2023-09-30',0)`.
- `{collection}_meta`: The meta-data sub-table, used for arbitrary meta-information and statistics.

## Triggers

Naksha PSQL-Storage will add a trigger to the HEAD table to ensure the desired behavior, even when direct SQL queries are executed.This trigger implements the following behavior (it basically exists in two variants: with history enabled or disabled):

1. On INSERT
   * Fix the XYZ namespace `txn=naksha_txn(), txn_next=0, app_id=, author=, ...`
   * Update the transaction-log
   * If history is enabled:
     * Ensure that the history partition exists
   * Delete the inserted feature (by id) from the `{collection}_del`
2. On UPDATE
   * Fix the XYZ namespace `txn=naksha_txn(), txn_next=0, app_id=, author=, ...`
   * Update the transaction-log
   * If history is enabled:
     * Ensure that the history partition exists
   * Delete the updated feature (by id) from the `{collection}_del`
   * If history is enabled:
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

Naksha will implement a special PURGE operation, which will remove elements from the special deletion table (which is a special HEAD table).

**Note**: Part of the XYZ-namespace fixing is the calculation of the **hrid** from the _geometry_ using [`ST_Centroid(coalesc(geo, ST_Point(0,0)))`](https://postgis.net/docs/ST_Centroid.html).

## Unique-Identifier

All features stored by the Naksha storage engine are part of a transaction. The data is stored partitioned, because of the huge amount of data that normally need to be handled. To instantly know where a feature is located, so in which partition, we need to ensure that the unique identifier of a transaction **and** the collection local row identifier hold the partition key.

Both, the transaction-number **and** the row-number are 64-bit integers, logically split into four parts:

* _Year_: The year in which the transactions started (e.g. 2023).
* _Month_: The month of the year in which the transaction started (e.g. 9 for September).
* _Day_: The day of the month in which the transaction started (1 to 31).
* _Sequence_: The local sequence-number in this day.

The local sequence-number is stored in a sequence named `{collection}_i_seq`. For the transaction table this is named `naksha_tx_i_seq`. Every day starts with the sequence-number reset to zero. The final value is calculated by **year** x _1,000,000,000,000,000_ + **month** x _10,000,000,000,000_ **day** x _100,000,000,000_ + **sequence-number**.

This concept allows up to 100 billion transactions per day and up to 100 billion features modified in a single collection per day (around 1,000,000 modifications or transaction per second). It will work up until the year 9222, where it will overflow. Should there be more than 100 billion transaction in a single day, this will overflow into the next day and potentially into an invalid day, should it happen at the last day of a given month. We ignore this situation, it seems currently impossible.

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

Normally, when a new unique identifier is requested, the method `naksha_uid(collection)` will read the sequence and verify it, so, if the year, month and day of the current transaction start-time matches the one stored in the sequence-number. If this is not the case, it will enter an advisory lock and retry the operation, if the sequence-number is still invalid, it will reset the sequence-number to the correct date, with the _sequence_ part being set to `1`, so that the next `naksha_uid(collection)` method that enters the lock or queries the _sequence_, receives a correct number, starting at _sequence_ `1`. The method itself will use the _sequence_ value `0` in the rollover case.

**Note**: We store the current transaction-number for the current transaction in a transaction local config value, so that we only need to read it ones, when needed the first time in a transaction. Additionally, there is a helper methods to be used to create arbitrary bounds for a unique identifier being `naksha_uid(timestamptz, bigint)`.

## GUID _(historically UUID)_

Traditionally XYZ-Hub used UUIDs as state-identifiers, but exposed them as strings in the JSON. Basically all known clients ignore the format of the UUID, so none of them expected to really find a UUID in it. This is good, because in Naksha we decided to change the format, but to stick with the name for downward compatibility.

The new format is called GUID (global unique identifier), returning to the roots of the Geo-Space-API. The syntax for a GUID in the PSQL-storage is:

`{storageId}:{collectionId}:{uid}`

The **uid** always refer to the `i` column of the table.

**Note**: This format holds all information needed for Naksha to know in which storage a feature is located, of which it only has the UUID. The PSQL storage knows from this UUID exactly in which database table the features is located, even taking partitioning into account. The reason is, that partitioning is done by transaction start date, which is contained in the **uid**. Therefore, providing a _UUID_, directly identifies the storage location of a feature, which in itself holds the information to which transaction it belongs to (`txn`). Beware that the transaction-number as well encodes the transaction start time and therefore allows as well to know exactly where the features of a transaction are located (including finding the transaction details itself).

## Table layout

The table layout for all the tables of a collection is the same and 100% downward compatible with what previously used in XYZ-Hub:

```sql
CREATE TABLE IF NOT EXISTS ${table}
(
    i         int8 PRIMARY KEY NOT NULL,
    geo       geometry(GeometryZ, 4326),
    jsondata  jsonb
);
CREATE SEQUENCE IF NOT EXISTS "${table}_i_seq" AS int8 OWNED BY "${table}".i;
```

The XYZ-Hub later added a new column `deleted` as `BOOLEAN DEFAULT FALSE`, this is no longer supported and simply ignored by Naksha-Hub.

So, for new tables the only minor change is that `i` becomes a primary key and is no `BIGSERIAL` any longer, because the triggers we add now increment `i` by them self. It will not harm, if it is till an auto-sequence, just this will consume one additional number here and there, not needed otherwise. Another changes implied is how `i` is used, it will be incremented with any change done to a row, so it no longer uniquely identifies a row, but a state identifier. This is a major semantic difference that implies, you can move existing tables into Naksha and back to XYZ-Hub, but you can't possibly use Naksha and XYZ-Hub in parallel with the same table, because they have different semantics for `i`.

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

## XYZ-Metadata [`@ns:com:here:xyz`]

The Naksha storage engine does, to stay downward compatible to XYZ-Hub, use the same namespace for metadata storage: `@ns:com:here:xyz`. The properties in this namespace are:

| Property  | Type   | Meaning                                                                                                |
|-----------|--------|--------------------------------------------------------------------------------------------------------|
| createdAt | long   | The epoch-timestamp in milliseconds when the transaction started, that created the feature originally. |
| updatedAt | long   | The epoch-timestamp in milliseconds when the transaction started, this state was created.              |
| rtuts     | long   | The real-time epoch-timestamp in milliseconds when this state was created.                             |
| txn       | long   | The transaction-number.                                                                                |
| txn_next  | long   | The transaction-number of the next state, being `0` if this is the HEAD (latest) state.                |
| txuuid    | GUID   | The transaction-number as GUID.                                                                        |
| uuid      | GUID   | The state identifier.                                                                                  |
| puuid     | GUID   | The previous state identifier.                                                                         |
| muuid     | GUID   | The merge state identifier.                                                                            |
| app_id    | String | The identifier of the application that created this state (last-updated-by).                           |
| author    | String | The author of this state (last-updated-by).                                                            |
| action    | String | The action that lead to this state: CREATE, UPDATE, DELETE.                                            |
| version   | long   | The version of this feature (change counter).                                                          |
| tags      | array  | The tags.                                                                                              |
| crid      | String | An arbitrary custom-ref-id. If this is set, the feature is added into this partition, ignoring `hrid`. |
| hrid      | String | The 31 character long HERE-ref-id, based upon the center of the geometry.                              |

In the tags we store:

* References via: `ref_<urn>`, for example `ref_urn:here:...`
* The MOM feature-type as read from `properties->featureType` or `momType` into: `mom_type={feature-type}`
* The rule code of violations into: `violation_rule_code={rule-code}`
* The status of a violation into: `violation_state={violations-status}`
* The validation type into: `violation_val_type={val-type}`

The **hrid** is automatically set by the Naksha storage engine at part of the normal triggers.

## MOM-Metadata [`@ns:com:here:meta`]

## Map-Creator-Metadata [`@ns:com:here:delta`]

## Optimal Partitioning

Naksha supports optimal partitioning. It creates as a background job a statistic for every new version appearing in the transaction table. The statistic basically creates an optimal partitioning distribution. It will try to merge as many features into each partition, so that not more than 10mb of raw-json is stored in each partition (using the `byteSize` information of the _collection-info_ to decide for the optimal number of features per partition). It will basically perform the partitioning based upon the spatial distribution, using the same schema that the HERE tiles are using. Therefore the auto-partitioning will use as well the HERE-ref-ids, but in mixed level, automatically detected by feature count and size.

The optimal partitioning is basically just a special feature that is stored within the meta-table of a collection. The properties of this features should basically hold a property `by_hrid`, which is a map like `Map<HereRefIdPrefix, FeatureCount>` and `by_crid`, which is a map like `Map<CustomRefIdPrefix, FeatureCount>`.

## Indexes and Queries

Map Creator Requirements

- Search for all features that a specific author modified (query by author)
- Search for all versions of a specific features (query by id from history)
- Get one specific version (query by uuid)
- Get all versions which have the property source-id set to a specific value (handler that copies source-id into tags, then query by tag?)
- All returned versions should have a reverse patch added (query puuid and generated reverse patch)

The PSQL-storage will create indices about the following properties:

* `btree(i DESC)`: This index is used, when looking up UUIDs as it reflects the unique state identifier of a feature.
* `gist-st(geo, $xyz->>txn, $xyz->>txn_next)`: This index is used, when searching for feature by geometry. The index is only useful, when the bounding box is small enough, in that case the cardinality .
* `btree[text_pattern_ops](jsondata->>id)`: This index can be used, when the **id** of the feature is known, it should reduce the cardinality by that much, that any other property can be filtered.
* `btree[text_pattern_ops]($xyz->>crid, $xyz->>hrid)`: This index can be used to query tiles.
* `btree($xyz->>txn, $xyz->>txn_next)`: This index is used to query for all features being part of a specific transaction.
* `btree($xyz->>updatedAt DESC, $xyz->>author)`: This index is used to query for all features being part of a specific transaction.
* `btree($xyz->>createdAt DESC, $xyz->>author)`: This index is used to query for all features that are created.
* `gin($xyz->tags)`: This index is used to query for all features being part of a specific transaction.

**Note**: The shortcut `$xyz` refers to `jsondata->'properties'->'@ns:com:here:xyz'`. We as well ignore exact types and simply assume implicit casting, for example `int8` for `txn` and `txn_next`.

To be reviewed:

```
jsonb_ops_path = only works with objects
                 it always creates just a hash-map per row being: hash(key,value)

-- The first argument is basically jsondata->'properties'->'@ns:com:here:xyz'->'tags'
-- We can make this the return of a helper function: naksha_tags(jsonb) -> jsonb
select jsonb_object('{"a","b","3"}', array_fill('true'::text, ARRAY[3])) as tagsmap;

-- We make a helper function: naksha_tag(key)
select jsonb_build_object('a', 'true');

-- Search for tags: SELECT * FROM features WHERE naksha_tags(jsondata) @> naksha_tag('a')
--                  SELECT * FROM features WHERE jsondata->'properties'->'@ns:com:here:com'->'tags' ? 'a';
-- Should work perfect with a GIN index using jsonb_ops_path
-- https://bitnine.net/blog-postgresql/postgresql-internals-jsonb-type-and-its-indexes/
-- https://pganalyze.com/blog/gin-index
```

## Transaction Logs

The transaction logs are stored in a `transactions` table, which persists out of events, grouped by transaction **UUID** (`tnx`). The table layout is:

| Column | Type      | Modifiers            | Description             |
|--------|-----------|----------------------|-------------------------|
| i      | BIGSERIAL | PRIMARY KEY NOT NULL | Primary row identifier. |
| txn    | TEXT      |                      | Transaction-number .    |


```sql
CREATE TABLE IF NOT EXISTS transactions
(
    -- Unique index of the row.
    i                    BIGSERIAL PRIMARY KEY NOT NULL,
    -- The action that has been performed:
    -- MODIFY_FEATURES: The features of a collection are modified, "id" refers to the collection name.
    -- MODIFY_COLLECTION: A collection was modified, "id" refers to the collection name.
    -- MESSAGE: A message was set, the "id" some arbitrary message identifier.
    action               text COLLATE "C"      NOT NULL,
    -- The target of the action.
    target               text COLLATE "C",
    -- The application identifier of the application performing the change.
    appId                text                  NOT NULL,
    -- The author used for the transaction (optional); if any.
    author               text                  NOT NULL,
    -- The unique transaction identifier, encoded in the `txn` as **object_id**.
    object_id            int8                  NOT NULL,
    -- The unique storage identifier, encoded in the `txn` as **storage_id**.
    storage_id           int8                  NOT NULL,
    -- The full timestamp when the transaction started, the year, month and day, encoded as well in the `txn` in a short form.
    ts                   timestamptz           NOT NULL,
    -- The UUID of the transaction as stored in the XYZ namespace property `txn`.
    txn                  uuid                  NOT NULL,
    -- The PostgresQL transaction id.
    psql_id              int8                  NOT NULL,
    -- The commit messages, if the action is COMMIT_MESSAGE.
    msg_text             text COLLATE "C",
    -- The JSON attachment for commit messages.
    msg_json             jsonb,
    -- The binary attachment for commit messages.
    msg_attachment       bytea,
    -- The unique publishing identifier, set by the transaction fix job as soon as the transaction becomes visible.
    publish_id           int8,
    -- The publishing timestamp of when the transaction became visible.
    publish_ts           timestamptz
);
```

The transaction table itself is partitioned by month, so there will be child tables aka `transactions_YYYY_MM`.

## History creation

To manage the history the Naksha PostgresQL library will add a “before” trigger, and an “after” trigger to all main HEAD tables. The “before” trigger will ensure that the XYZ namespace is filled correctly, while the “after” trigger is optional and will write the transaction into the history and transaction table.

The history can be enabled and disabled for every space using the following methods:

- `naksha_col_disable_history('collection')`
- `naksha_col_enable_history('collection')`

## Transaction Fix Job

The last step is a background job added into Naksha-Hub that will “fix” the transaction table. It will set the `publish_id` and `publish_ts` to signal the visibility of a transaction and to generate a sequential numeric identifier, that has no holes and is unique for every transaction. Note that the `publish_id` itself is not unique, multiple events in the transaction table can belong to the same `publish_id`, but it is guaranteed that the lowest `publish_id`. 

The author and application identifier must be set by the client before starting any transaction via `SELECT naksha_tx_start('{appId}', '{author}');`. Note that the **author** is optional and can be `null`, but the application identifier **must not** be `null` or an empty string. If the author is `null`, then the current author stays the author for all updates or deletes. New objects in this case, are set to the application identifier, so that the application gains authorship.

In the context of [HERE](https://here.com), the **author** and **appId** are set to the `userMapHubId` as received from the **Wikvaya** service, therefore coming from the **UPM** (*User Permission Management*). Technically Naksha treats all these values just as strings and does not imply and meaning to them, so the service can be used for any other authentication system. 





