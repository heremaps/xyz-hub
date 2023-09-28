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

| Method                                                                                           | Description                                                                        |
|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| initStorage()                                                                                    | Initialize the storage and ensure the latest Naksha extension is installed.        |
| maintainStorage()                                                                                | Perform maintenance tasks, for example like purging outdated history, run vacuum.  |
| getCollections( List\<String\> ids ) : IResultSet\<StorageCollection\>                           | Return the requested collections.                                                  |
| listCollections() : IResultSet\<StorageCollection\>                                              | Return all storage collections.                                                    |
| upsertCollection( StorageCollection collection ) : StorageCollection                             | Insert or update the given storage collection.                                     |
| deleteCollectionAt( StorageCollection collection, long time ) : StorageCollection                | Ask for deletion of the collection at a specific time in the future.               |
| deleteCollectionIn( StorageCollection collection, long time, TimeUnit unit ) : StorageCollection | Ask for deletion of the collection in the future.                                  |
| restoreCollection( StorageCollection collection ) : StorageCollection                            | Restores a collection that was marked for deletion.                                |
| purgeCollection( StorageCollection collection ) : StorageCollection                              | Delete the collection now (instant).                                               |

We need to add methods to create and manage indices on collections.

## IReadTransaction

| Method                                                                                                                      | Description                                                                                       |
|-----------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| readFeatures(List\<String\> collections, long version, SOp stOp, Pop propertyOp, OOp orderBy...) : IResultSet\<XyzFeature\> | Read features from the given collections in the given version, using the given query.             |
| readVersions(long start) : IResultSet\<Version\>                                                                            | Read the transaction-log, starting with the given version till the end.                           |
| readVersions(long start, long end) : IResultSet\<Version\>                                                                  | Read the transaction-log, starting with the given version till the given end version.             |
| readVersions(String commentId) : IResultSet\<Version\>                                                                      | Read the transaction-log, return all version with a comment with the given identifier.            |
| readPublicVersions(long start, int limit) : List\<Version\>                                                                 | Read the transaction-log, return all public versions greater than the given start value.          |
| calculateOptimalPartitioning(long version) : Map\<String, Long\>                                                            | Returns the last version that is guaranteed to be consistent, so there is no pending transaction. |
| latestConsistentVersion() : long                                                                                            | Returns the last version that is guaranteed to be consistent, so there is no pending transaction. |
| close()                                                                                                                     | Close the transaction and all open cursors.                                                       |

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

## SRef, PRef, ORed, SOp, POp and Oop

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
| SOp.intersects2d( ST_Geo geo ) : SOp                 | Tests if a feature intersects with the given geometry.                 |
| SOp.intersects3d( ST_Geo geo ) : SOp                 | Tests if a feature intersects with the given geometry.                 |
| PRef.id() : PRef                                     | Returns a reference to the **id** property.                            |
| PRef.uuid() : PRef                                   | Returns a reference to the **uuid** property from the XYZ-namespace.   |
| PRef.uts() : PRef                                    | Returns a reference to the **uts** property from the XYZ-namespace.    |
| PRef.appId() : PRef                                  | Returns a reference to the **appId** property from the XYZ-namespace.  |
| PRef.author() : PRef                                 | Returns a reference to the **author** property from the XYZ-namespace. |
| PRef.tag(String name) : PRef                         | Returns a reference to a specific tag in the XYZ-namespace.            |
| PRef.hereQuadRefId() : PRef                          | Returns a reference to the **hqrid** XYZ-namespace.                    |
| PRef.webQuadRefId() : PRef                           | Returns a reference to the **wqrid** XYZ-namespace.                    |
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

## WriteQuery

| Property                    | Description                                    |
|-----------------------------|------------------------------------------------|
| insert : List\<XyzFeature\> | Features to be inserted.                       |
| update : List\<XyzFeature\> | Features to be updated.                        |
| upsert : List\<XyzFeature\> | Features to be inserted or updated.            |
| delete : List\<XyzFeature\> | Features to be deleted.                        |
| read : boolean              | If the result of the write should be returned. |

## WriteResult

| Property                      | Description                                  |
|-------------------------------|----------------------------------------------|
| query : WriteQuery            | The reference to the original write query.   |
| inserted : List\<XyzFeature\> | Features to inserted.                        |
| updated : List\<XyzFeature\>  | Features to updated.                         |
| deleted : List\<XyzFeature\>  | Features to deleted.                         |

## Version

A version is a set of transaction-log entries that share the same version number.

| Property                                      | Description                                                                    |
|-----------------------------------------------|--------------------------------------------------------------------------------|
| version : long                                | The version number.                                                            |
| publicId : Long                               | The publishing identifier, if yet available.                                   |
| publicTs : Long                               | The publishing timestamp, if yet available.                                    |
| comments : Map\<String, Comment\>             | All comments added to the transaction.                                         |
| collections : Map\<String, List\<TxAction\>\> | A map between the collection name and the actions performed to the collection. |

Note that versions need to be prepared for publication via the subscription sub-system. For this purpose every version will receive a 
unique publication identifier, which is a continues number starting with 1 to n. Naksha guarantees for this identifier that there are
no holes in the numeration, and it guarantees that lower numbers have lower timestamps. This can be used to reset a subscription by 
n steps or to start at a specific state and to guarantee an order.

**Warning**: The version number is not fully reliable for publication, because theoretically version 1 can be a very long-running 
transaction (hours) and meanwhile there can other transactions appearing, that have higher version numbers. Even while the transactions
must be commutative, so technically it does not matter for the result in which order they are applied, the order can have a big impact
on subscriptions. Therefore, Naksha observes the transaction table and creates publication numbers to guarantee that the subscriptions
are re-playable in exactly the same order as they became visible, not the order in which the transactions started.

## Comment

| Property          | Description                         |
|-------------------|-------------------------------------|
| id : String       | The identifier of the comment.      |
| message : String  | The message of the comment.         |
| Object : object   | The JSON deserialized object added. |
| binary : byte[]   | The binary attachment added.        |

