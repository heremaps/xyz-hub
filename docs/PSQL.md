# PostgresQL Reference Implementation

This part of the documentation explains details about the reference storage implementation based upon PostgresQL database.

## Basics

The admin database of Naksha will (currently) be based upon a PostgresQL database. For users, we will allow the usage of the same implementation for their collections that the Naksha-Hub itself uses to store administrative features.

## Collections

Within PostgresQL a collection is a set of database tables. All these tables are prefixed by the `collection` identifier. The tables are:

- `{collection}`: The HEAD table holding all features in their live state. This table is the only one that should be directly accessed for manual SQL queries and has triggers attached that will ensure that the history is written accordingly.
- `{collection}_hst`: The main history table, a partitioned table that partitions based upon the transaction date.
- `{collection}_hst_{YYYY}_{MM}_{YY}`: The history partition for the given day.
- `{collection}_del`: The HEAD deletion table holding all features that are deleted from the HEAD table, but are not yet purged. When using a table in moderation mode or in a view, the deleted features are read together with those from the HEAD table. 

### Table layout

The table layout for all the tables of a collection is the same and 100% downward compatible with what previously used in XYZ-Hub:

```sql
CREATE TABLE IF NOT EXISTS ${table}
(
    jsondata jsonb,
    geo      geometry(GeometryZ, 4326),
    i        int8 PRIMARY KEY NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS "${table}_i_seq" AS int8 OWNED BY "${table}".i;
```

The XYZ-Hub later added a new column `deleted` as `BOOLEAN DEFAULT FALSE`, this is no longer supported and simply ignored by Naksha-Hub.

So, for new tables the only minor change is that `i` becomes a primary key and is no `BIGSERIAL` any longer, because the triggers we add now increment `i` by them self. It will not harm, if it is till an auto-sequence, just this will consume one additional number here and there, not needed otherwise. Another changes implied is how `i` is used, it will be incremented with any change done to a row, so it no longer uniquely identifies a row, but a state. This is a major semantic difference that implies, you can move existing tables into Naksha and back to XYZ-Hub, but you can't possibly use Naksha and XYZ-Hub in parallel with the same table, because they have different semantics for `i`.

## Transaction Logs

The transaction logs are stored in a `transactions` table, which persists out of events, grouped by transaction **UUID** (`tnx`). The table layout is:

```sql
CREATE TABLE IF NOT EXISTS transactions
(
    -- Unique index of the row.
    i                    BIGSERIAL PRIMARY KEY NOT NULL,
    -- The action: COMMIT_MESSAGE, MODIFY_FEATURES, CREATE_COLLECTION, UPDATE_COLLECTION, DELETE_COLLECTION or RESTORE_COLLECTION.
    action               text COLLATE "C"      NOT NULL,
    -- The unique transaction identifier encoded in the `txn` as **object_id**.
    id                   int8                  NOT NULL,
    -- The local identifier of the event within the transaction, normally "{collection}" or "msg:{commit-msg-id}".
    name                 text COLLATE "C"      NOT NULL,
    -- The collection effected (the table-set prefix), if any.
    collection           text COLLATE "C",
    -- The application identifier of the application performing the change.
    appId                text                  NOT NULL,
    -- The author used for the transaction (optional); if any.
    author               text                  NOT NULL,
    -- The UUID of the transaction as stored in the XYZ namespace property `txn`.
    tx_uuid              uuid                  NOT NULL,
    -- The PostgresQL transaction id.
    tx_psql_id           int8                  NOT NULL,
    -- The unique storage identifier encoded in the `txn` as ** **.
    tx_storage_id        int8                  NOT NULL,
    -- The full timestamp when the transaction started, the year, month and day encoded as well in the `txn`.
    tx_ts                timestamptz           NOT NULL,
    -- The commit messages, if the action is COMMIT_MESSAGE.
    commit_msg           text COLLATE "C",
    -- The JSON attachment for commit messages.
    commit_json          jsonb,
    -- The binary attachment for commit messages.
    commit_attachment    bytea,
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

