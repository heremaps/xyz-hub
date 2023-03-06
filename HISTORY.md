# HISTORY

This document describes the new feature of implementing a real-time logical replicable history. The
history allows to review every state a feature in a space had in the past and to rewind back to any
previous state. It supports snapshots (including database wide snapshots), tagging, branching and
views.

## Table layout

The table layout is 100% compatible with what previously existed in XYZ-Hub:

```sql
CREATE TABLE IF NOT EXISTS ${schema}.${table}
(
    jsondata jsonb,
    geo      geometry(GeometryZ, 4326),
    i        BIGSERIAL
);
```

This was optionally appended by `deleted BOOLEAN DEFAULT FALSE`, what is no longer supported.

For new tables the only minor change is that `i` becomes a primary key and is not auto-sequence
any longer, because a trigger now does the job. It will not harm if it is an auto-sequence, just 
this will consume one additional number not needed otherwise. The new table therefore looks like:

```sql
CREATE TABLE IF NOT EXISTS "${schema}"."${table}"
(
    jsondata      jsonb,
    geo           geometry(GeometryZ, 4326),
    i             int8 PRIMARY KEY NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS "${schema}"."${table}_i_seq" AS int8 OWNED BY "${table}".i;
```

The sequence has the same name as before, therefore not impacting the existing tables.

## @ns:com:here:xyz

Every space now has at least a trigger attached, that modifies the XYZ namespace before inserting
or updating an existing feature in the **head** table. This namespace now looks like following:

- **action**: The operation has been performed being `CREATE`, `UPDATE`, `DELETE` or `PURGE`.
- **version**: The version of the features, a sequentially consistent numbering.
- **author**: The author of the feature; if any.
- **app_id**: The application-id of the application that created the feature state; if any.
- **puuid**: The UUID of the previous state, `null` if `CREATE`.
- **uuid**: The unique state identifier, stored as UUID (must not be `null`).
- **txn**: The transaction number, being as well a UUID (must not be `null`).
- **createdAt**: The unix epoch timestamp in millis of when the feature has been created.
- **updatedAt**: The unix epoch timestamp in millis of when this state has been created.
- **rtcts**: The real-time unix epoch timestamp in millis of when the feature has been created.
- **rtuts**: The real-time unix epoch timestamp in millis of when this state has been created.

The difference between **createdAt**/**updatedAt** and **rtcts**/**rtuts** is that the former 
reflect the transaction time. This time is searchable and reflects in which partition the row
is stored, while the latter reflects the clock time, when the update was really performed. 
Therefore, all creations and updates in the same transaction have the same **createdAt** and
**updatedAt** time, while their **rtcts**/**rtuts** can differ drastically, when looking at a 
long running transaction.

We do not support searching for the real-time, it is informative only. The reason is the
partitioning by the **updatedAt** time.

The application identifier and optionally the author must be set by the client for any transaction 
as first actions via: 
- `SELECT xyz_config.naksha_tx_set_app_id('{userMapHubId}');`
- `SELECT xyz_config.naksha_tx_set_author('{userMapHubId}');`

Note that only the author can set to `null` (or not set), the application identifier **must not** 
be `null`. If the author is `null`, then the current author stays the author for updates or deletes 
and for new objects, the application gains authorship.

## UUIDs

XYZ-Hub uses `UUID`s to uniquely identify every state of a feature stored in a database.

Naksha-Hub does use `UUID`s too, but for two purpose. To uniquely identify features and 
transactions. Both are generated using triggers to ensure that even queries executed in the 
database directly update the `UUID` of the features and transactions.

The **UUID** will be in the format of a
[random UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier), so being version 4,
variant 1 (big endian encoded). However, they are not random, but reflect a guaranteed unique
identifier. The format chosen matches perfectly well with how PostgresQL  
[compares UUIDs](https://doxygen.postgresql.org/uuid_8c.html#aae2aef5e86c79c563f02a5cee13d1708).
As seen in the source code, the compare of **UUID**s is done as:

```C
return memcmp(arg1->data, arg2->data, UUID_LEN);
```

[memcp](https://cplusplus.com/reference/cstring/memcmp/) does compare simply the bytes in order
(so basically using big-endian order).

All **UUID**s being will basically encode the following values:

```
   1   2     3    e    4    5    6    7 -   e    8    9    b-   4    2    d    3  -   a    4    5     6-   4    2    6    6     1    4    1    7    4    0    0    0
|          time low                   |  |    time mid     | |ve| | time high  |    va|  clock_seq     |              node                                         |
7654_3210-7654_3210-7654_3210-7654_3210--7654_3210-7654_3210-7654_3210-7654_3210----7654_3210-7654_3210-7654_3210-7654_3210--7654_3210-7654_3210-7654_3210-7654_3210
iiii_iiii-iiii_iiii-iiii_iiii-iiii_iiii--iiii_iiii-iiii_iiii-1000_iiii-iiii_iiii----10YY_YYYY-YYYY_mmmm-DDDD_Dttt-dddd_dddd--dddd_dddd-dddd_dddd-dddd_dddd-dddd_dddd
[                   id                                     ] ver  [    id      ]    va[   year   ][mon ][day ][t] [                 db-id                          ] 
                                                             =4                     =1

iiii::60 = The object identifier.
ver ::4  = The fixed UUID version, 4-bit binary value 4 (binary 1000).
va  ::2  = The fixed variant, 2-bit binary value 2 (binary 10).
YYYY::10 = The biased year (year - 2000), resulting in possible years 2000 to 3023.
mmmm::4  = The month of the year (1 to 12).
DDDD::5  = The day of the month (1 to 31).
t   ::3  = The object type. 
dddd::40 = The database identifier.
```

**Note**:
- The version (`ver`) is always four with value being 4 (binary `1000`).
- The variant (`va`) is always variant one (big endian) with value being decimal 2 (binary `10`).

Therefore, every Naksha **UUID** refers to a specific object of a specific type in a specific 
globally registered database at a given time.

The objects that can be a target are currently only two:

- `0`: Transaction
- `1`: Feature

## Transaction UUID

All changes being part of the same transaction need to have the same unique transaction number. 
Transaction numbers are globally unique.

Naksha requires within the target database a single special schema named `xyz_config`. In this 
schema certain maintenance spaces will be created, together with a couple of sequences and 
functions used to generate the unique transaction number.

The date is used to find the features of the transaction in the history partition. 

## Feature UUID

Every state of every feature stored in a space will have a globally unique UUID. 

The date is used to find the features of the transaction in the history partition.

## Transaction History

To enable the history we need a shared transaction table in the `xyz_config` schema:

```sql
CREATE TABLE IF NOT EXISTS xyz_config.transactions
(
    i           BIGSERIAL PRIMARY KEY NOT NULL,
    txid        int8                  NOT NULL,
    txi         int8                  NOT NULL,
    txts        timestamptz           NOT NULL,
    txdb        int8                  NOT NULL,
    txn         uuid                  NOT NULL,
    "schema"    text COLLATE "C"      NOT NULL,
    "table"     text COLLATE "C"      NOT NULL,
    commit_msg  text COLLATE "C",
    commit_json jsonb,
    -- Columns managed by the transaction fix job. 
    space       text COLLATE "C",
    id          int8,
    ts          timestamptz
);
```

- `i`: Unique index of the row.
- `txid`: The PostgresQL transaction id, can be used to review if previous transactions are still 
          pending to detect eventual consistency.
- `txi`: The unique transaction identifier encoded in the `txn` as **object_id**.
- `txts`: The full timestamp when the transaction started, the year, month and day encoded as well 
          in the `txn`.
- `txn`: The UUID of the transaction as stored in the XYZ namespace property `txn`.
- `schema` + `table`: The schema and table affected by the transaction.
- `commit_msg` + `commit_json`: Arbitrary commit messages, they all have `schema` set to `COMMIT_MSG`. 
- `space`: The space, set by the transaction fix job as soon as the transaction becomes visible.
- `id`: The unique sequential identifier, set by the transaction fix job as soon as the transaction becomes visible.
- `ts`: The timestamp of when the transaction became visible for the transaction fix job.

## Create history

To manage the history we will add a “before” trigger, and an “after” trigger to all spaces. The
“before” trigger will ensure that the XYZ namespace filled correctly while the “after” trigger
is optional and will write the transaction into the history and transaction table.

The history can be enabled and disabled for every space using the following methods:
- `xyz_config.naksha_space_disable_history(_schema, _table)`
- `xyz_config.naksha_space_enable_history(_schema, _table)`

## Transaction Fix Job

The last step is a background job added into Naksha-Hub that will fix the transaction table. It
will set the `seqid` and `seqts` to signal the visibility of a transaction and to generate a 
sequential numeric identifier, that has no holes, for every transaction.
