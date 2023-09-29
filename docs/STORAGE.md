# Storage API

Naksha operates on storages. The storages are transparent, Naksha only requires an implementation of the Naksha Storage-API. This document
describes the storage API and usage.

## Storage access

There are three ways to access a storage.

1. Using the Naksha Host Interface to query for an existing Host managed instance: `naksha.getStorage(id)`.
2. Create an instance through abstraction. This requires manual configuration, basically: `new PsqlStorage(storage)`. The `storage` 
   object must be an instance of the `Storage` feature, which expects the configuration as otherwise done by the user.
3. Directly instantiate a driver, this actually is the same as step, just a little more comfortable: 
   `new PsqlStorage(new PsqlConfigBuilder().parseUrl("jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test&app=Demo").build())`.

All of these methods will return an instance that implements the `IStorage` class.

## IStorage

The `IStorage` interface has only one method, being `newContext()`, which will return a new `IStorageContext`. Actually, the same storage
instance can be used to create thousands of contexts.

## IStorageContext

The storage context offers a couple of methods related to the storage.

| Method                                                         | Description                                           |
|----------------------------------------------------------------|-------------------------------------------------------|
| withAppId(String appId) : IStorageContext                      | Set the application identifier to be used.            |
| withAuthor(String author) : IStorageContext                    | Set the author.                                       |
| withStmtTimeout(long timeout, TimeUnit unit) : IStorageContext | Set the statement timeout.                            |
| withLockTimeout(long timeout, TimeUnit unit) : IStorageContext | Set the lock timeout.                                 |
| openMasterTransaction() : IMasterTransaction                   | Open a read-write transaction with the master node.   |
| openReplicaTransaction() : IReplicaTransaction                 | Open a read-only transaction with a replication node. |

The interfaces are basically defined like:

- `IMasterTransaction extends IAdminTransaction, IAdminManager, IWriteTransaction, IReadTransaction, IFeatureReader, IFeatureWriter, AutoClosable`
- `IReplicaTransaction extends IReadTransaction, IFeatureReader, AutoClosable`

## IAdminTransaction

| Method                                                                                               | Description                                                                        |
|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| initStorage()                                                                                        | Initialize the storage and ensure the latest Naksha extension is installed.        |
| maintainStorage()                                                                                    | Perform maintenance tasks, for example like purging outdated history, run vacuum.  |
| getCollections( List\<String\> ids ) : IResultSet\<StorageCollection\>                               | Return the requested collections.                                                  |
| listCollections() : IResultSet\<StorageCollection\>                                                  | Return all storage collections.                                                    |
| upsertCollection( StorageCollection collection ) : StorageCollection                                 | Insert or update the given storage collection.                                     |
| ~~deleteCollectionAt( StorageCollection collection, long time ) : StorageCollection~~                | Ask for deletion of the collection at a specific time in the future.               |
| ~~deleteCollectionIn( StorageCollection collection, long time, TimeUnit unit ) : StorageCollection~~ | Ask for deletion of the collection in the future.                                  |
| ~~restoreCollection( StorageCollection collection ) : StorageCollection~~                            | Restores a collection that was marked for deletion.                                |
| purgeCollection( StorageCollection collection ) : StorageCollection                                  | Delete the collection now (instant).                                               |

We need to add methods to create and manage indices on collections. The struck out methods are not needed initially.

## IReadTransaction

| Method                                                                 | Description                                                                                                         |
|------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| readFeatures( ReadQuery query ) : IResultSet\<XyzFeature\>             | Read features from the given collections in the given version, using the given query.                               |
| readTx( long txn ) : UniMapTx                                          | Read the transaction with the given transaction number from the transaction-log.                                    |
| readTx( long startTxn, int limit ) : IResultSet\<UniMapTx\>            | Read all transactions from the transaction-log, starting with the given transaction-number, but no more than limit. |
| readTx( String commentId ) : IResultSet\<UniMapTx\>                    | Read all transactions from the transaction-log, that have a comment with the given identifier.                      |
| readSeqTx( long seqNum, int limit ) : List\<UniMapTx\>                 | Read all transactions from the transaction-log, starting with the given sequence-number, but no more than limit.    |
| ~~calculateOptimalPartitioning( long version ) : Map\<String, Long\>~~ | Returns the last version that is guaranteed to be consistent, so there is no pending transaction.                   |
| ~~latestConsistentVersion() : long~~                                   | Returns the last version that is guaranteed to be consistent, so there is no pending transaction.                   |
| close()                                                                | Close the transaction and all open cursors.                                                                         |

The struck out methods are subject to discussion.

## IWriteTransaction

| Method                                                                | Description                                                                    |
|-----------------------------------------------------------------------|--------------------------------------------------------------------------------|
| writeFeatures( WriteQuery query ) : WriteResult                       | Write the given features and return the result.                                |
| setComment( String id, String message, Object object, byte[] binary ) | Write a comment with the given identifier, message and optional attachments.   |
| commit()                                                              | Commit all performed writes, closes all open cursors.                          |
| rollback()                                                            | Rollback (revert) all not yet committed writes, closes all open cursors.       |
| close()                                                               | Perform a rollback and then close the transaction and all open cursors.        |

## IResultSet<TYPE> extends Iterator<TYPE>, AutoClosable

| Method                                                 | Description                                            |
|--------------------------------------------------------|--------------------------------------------------------|
| withType(Class\<TYPE\> typeClass) : IResultSet\<TYPE\> | Changes the feature-type to be returned, returns this. |
| hasNext() : boolean                                    | True if this result-set has more features.             |
| next() : TYPE                                          | Returns the next feature.                              |
| close()                                                | Close the cursor.                                      |

## IFeatureReader / IFeatureWriter / IAdminManager

These interfaces will offer helper methods that not implemented by the storage itself, rather they are helper methods provided by the 
Naksha library itself, for example `getFeatureById(String id, Class<TYPE> typeClass) : TYPE`. It is not yet clear what is needed here.

## ReadQuery

| Property                     | Description                                    |
|------------------------------|------------------------------------------------|
| collections : List\<String\> | The identifiers of the collections to be read. |
| version : long               | The version to read.                           |
| spatialOp : SOp              | The (optional) spatial operation to execute.   |
| propertyOp : POp             | The (optional) property operation to execute.  |
| orderBy : OOp                | The (optional) ordering operation to execute.  |

## WriteQuery

| Property                    | Description                                                            |
|-----------------------------|------------------------------------------------------------------------|
| collection : String         | The identifier of the collection to which to apply the modifications.  |
| insert : List\<XyzFeature\> | Features to be inserted.                                               |
| update : List\<XyzFeature\> | Features to be updated.                                                |
| upsert : List\<XyzFeature\> | Features to be inserted or updated.                                    |
| delete : List\<XyzFeature\> | Features to be deleted.                                                |
| returnModified : boolean    | If the result of the write should be returned.                         |

## WriteResult

| Property                      | Description                                |
|-------------------------------|--------------------------------------------|
| writeQuery : WriteQuery       | The reference to the original write query. |
| inserted : List\<XyzFeature\> | Features that have been inserted.          |
| updated : List\<XyzFeature\>  | Features that have been updated.           |
| deleted : List\<XyzFeature\>  | Features that have been deleted.           |

## SRef, PRef, ORef, SOp and Oop

The Naksha library comes with three classes for operations:

- **SRef**: spatial reference
- **SOp**: spatial operation
- **PRef**: property reference
- **POp**: property operation
- **OOp**: order operation

They are used to query for features. The three classes are public with static public methods that return protected classes extending the
corresponding type class. The following table shows all static methods and what they return. Some methods are available for all types,
some are only available for a specific type:

| Method                                               | Description                                                            |
|------------------------------------------------------|------------------------------------------------------------------------|
| *Op.and( ops... ) : *Op                              | Creates an logical AND for the given operations and returns the AND.   |
| *Op.or( ops... ) : *Op                               | Creates an logical OR for the given operations and returns the OR.     |
| *Op.not( op ) : *Op                                  | Inverts the operation, so true becomes false and false becomes true.   |
| SRef.geometry( Geometry geo ) : SRef                 | Wraps a geometry into a ST-geometry.                                   |
| SRef.buffer( Geometry geo, radius: double ) : SRef   | Wraps a geometry with a buffer around it into a ST-geometry.           |
| SRef.tile( long x, long y, int srid ) : SRef         | Wraps a projected tile into a ST-geometry.                             |
| SRef.webTile( String id ) : SRef                     | Wraps a web-mercator tile into a ST-geometry.                          |
| SRef.hereTile( String id ) : SRef                    | Wraps a HERE tile into a ST-geometry.                                  |
| SOp.intersects2d( SRef geo ) : SOp                   | Tests if a feature intersects with the given geometry.                 |
| SOp.intersects3d( SRef geo ) : SOp                   | Tests if a feature intersects with the given geometry.                 |
| PRef.id() : PRef                                     | Returns a reference to the **id** property.                            |
| PRef.uuid() : PRef                                   | Returns a reference to the **uuid** property from the XYZ-namespace.   |
| PRef.uts() : PRef                                    | Returns a reference to the **uts** property from the XYZ-namespace.    |
| PRef.appId() : PRef                                  | Returns a reference to the **appId** property from the XYZ-namespace.  |
| PRef.author() : PRef                                 | Returns a reference to the **author** property from the XYZ-namespace. |
| PRef.tag(String name) : PRef                         | Returns a reference to a specific tag in the XYZ-namespace.            |
| ~~PRef.hereQuadRefId() : PRef~~                      | Returns a reference to the **hqrid** XYZ-namespace.                    |
| ~~PRef.webQuadRefId() : PRef~~                       | Returns a reference to the **wqrid** XYZ-namespace.                    |
| PRef.property(String path...) : PRef                 | Returns a reference to an arbitrary property using the given path.     |
| POp.exists( PRef ref ) : POp                         | Tests if a feature does have the given property.                       |
| POp.startsWith( PRef ref, String value ) : POp       | Tests if the property value stars with the string.                     |
| POp.equals( PRef ref, String value ) : POp           | Tests if the property equals the given value.                          |
| POp.equals( PRef ref, number value ) : POp           | Tests if the property equals the given value.                          |
| POp.equals( PRef ref, boolean value ) : POp          | Tests if the property equals the given value.                          |
| POp.lt( PRef ref, number value ) : POp               | Tests if the property value is less than the given value.              |
| POp.lte( PRef ref, number value ) : POp              | Tests if the property value is less than or equal the given value.     |
| POp.gt( PRef ref, number value ) : POp               | Tests if the property value is greater than the given value.           |
| POp.gte( PRef ref, number value ) : POp              | Tests if the property value is greater than or equal the given value.  |
| OOp.orderAsc( PRef ref, nullsFirst: boolean ) : OOp  | Order the results ascending by the given property.                     |
| OOp.orderDesc( PRef ref, nullsFirst: boolean ) : OOp | Order the results descending by the given property.                    |

More operations may be added later.

The stroke out methods are subject to discussion.

## Transaction-Log

Naksha expects that every storage does come with an audit-log. Logically, Naksha expects that the storage provides transaction information in the following structure.

### UniMapTx

| Property                                              | Description                                                                    |
|-------------------------------------------------------|--------------------------------------------------------------------------------|
| ts : long                                             | The time when the transaction was started.                                     |
| txn : long                                            | The database wide unique transaction number.                                   |
| seqNum : Long                                         | The sequence number, if available.                                             |
| seqTs : Long                                          | The timestamp when the sequencer set the sequence number, if yet available.    |
| comments : Map\<String, UniMapTxComment\>             | All comments added to the transaction.                                         |
| collections : Map\<String, List\<List\<TxAction\>\>\> | A map between the collection name and the actions performed to the collection. |

Note that transactions need to be prepared for publication via the subscription sub-system. For this purpose every version will receive a unique publication identifier, which is a continues number starting with 1 to n. Naksha guarantees for this identifier that there are no holes in the numeration (therefore the name sequence-number), and it guarantees that lower numbers have lower timestamps.

**Warning**: The transaction-number is not fully reliable for publication, because theoretically version 1 can be a very long-running transaction (hours) and meanwhile other transactions may be finished, having higher version numbers. Even while the transactions must be commutative logically, so technically it does not matter for the final result in which order they are executed, the order can have a big impact on subscriptions. Therefore, Naksha observes the transaction table and creates sequence-numbers assigned in the order in which transactions become visible rather than the order in which they were started. This is to guarantee that the subscriptions are re-playable in exactly the same order in which they became visible, not the order in which the transactions started. This is important to avoid that subscriptions have to wait for pending transactions, what would make them (in some cases) extremely slow. 

### UniMapTxComment

| Property          | Description                         |
|-------------------|-------------------------------------|
| id : String       | The identifier of the comment.      |
| message : String  | The message of the comment.         |
| Object : object   | The JSON deserialized object added. |
| binary : byte[]   | The binary attachment added.        |

### TxAction

An alias for strings with pre-defined values:

* MODIFY_FEATURES
* UPSERT_COLLECTION
* PURGE_COLLECTION
