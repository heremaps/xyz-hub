# Authorization

Naksha supports various out-of-box authorization checks while performing read/write operations against following resources:

- [Storage](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/models/naksha/Storage.java)
- [EventHandler](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/models/naksha/EventHandler.java)
- [Space](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/models/naksha/Space.java)
- [XyzFeature](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/models/geojson/implementation/XyzFeature.java)
- [XyzCollection](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/models/naksha/XyzCollection.java)

based on User's access profile supplied using following attributes as part of [NakshaContext](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/NakshaContext.java):

- **appId** - Mandatory
  - The unique application identifier, requesting current operation.
- **author** - Optional
  - The unique identifier of a logged-in user, who is requesting current operation.
- **urm** - Optional (but Mandatory for meaningful authorization checks)
  - The user-rights-matrix, defines resource level access authorized for current operation. 
- **su** - Optional
  - Superuser flag, if set, all authorization checks will be bypassed. This is useful where one level of authorization is already performed and we like to avoid repetitive checks on recursive/internal calls.

## 1. URM, ARM Concept

* URM = (User-Rights-Matrix) holding the rights of the requester/principal (unique combination of appId, author).
* ARM = (Access-Rights-Matrix) holding the rights necessary to perform the requested action. It depends on type of request.

Assume URM as left-side of comparison, then ARM represents the right-side of comparison indicating what rights are necessary to allow requested action against the target resource.

Both follow the Map<String, Object> format as below,
which allows to define a rights matrix of an **Action** against target **Resource** by specifying one/more **AttributeMaps** of zero/more **Attributes**, that should be compared to validate request authorization.

Thumb rule is - **All** entries in the ARM are compared against all entries in the URM per **Action**.
For each entry in the ARM at least one entry being greater or equal to the ARM entry must exist to grant access.

```json
{
    "urm": {
        "xyz-hub": { // ---> ARM is defined at this level with set of Actions 
            // zero or more Actions
            "<action>": [
                // one or more Attribute-Map's (atleast one should match against ARM)
                {
                    // zero or more access Attributes (all should match against ARM)
                    "<accessAttribute1>": "<exactValue>",
                    "<accessAttribute2>": "<valueWithWildcard*>",
                    "<accessAttribute3>": [  // one or more values (all should match against ARM)
                        "<anotherExactValue>",
                        "<anotherValueWithWildcard*>"
                    ]
                }
            ]
        }
    }
}
```

Sample URM:

```json
{
    "urm": {
        "xyz-hub": {
            // zero or more Actions
            "readFeatures": [
                // one or more Attribute-Map's (atleast one should match against each ARM AttributeMap)
                {
                    // zero or more access Attributes (all should match against ARM)
                    "id": "my-unique-feature-id",
                    "storageId": "id-with-wild-card-*",
                    "tags": [  // one or more values (all should match against ARM)
                        "my-unique-tag",
                        "some-common-tag-with-wild-card-*"
                    ]
                }
            ]
        }
    }
}
```

Sample ARM:

```json
{
    // zero or more Actions (ALL should match against URM)
    "readFeatures": [
        // one or more Attribute-Map's (ALL should match against URM)
        {
            // one or more resource Attributes (ARM can have more attributes/values than that in URM)
            "id": "my-unique-feature-id",
            "storageId": "id-with-wild-card-matching-value",
            "collectionId": "unused-id-during-checks",
            "tags": [
                "my-unique-tag",
                "some-common-tag-with-wild-card-matching-value",
                "some-additional-tag"
            ]
        }
    ]
}
```



### Action

Defines type of operation allowed against a resource. Refer later sections on this page, to know which specific actions are allowed against which resource.

But, in general:

- **useXXX**: Means that a resource can be used and **id**, **title**, **description** can be viewed, but NOT **properties** details
- **manageXXX**: Means that a resource can be read and modified (i.e. full access).
- **createXXX**: Allow the creation of a resource.
- **readXXX**: Allow reading of a resource.
- **updateXXX**: Allow to update a resource.
- **deleteXXX**: Allow to delete a resource.

Each Action can have list of one or more **AttributeMaps**, which is then compared against target Resource AttributeMaps to validate authorization.

For example:

```json
{
    "readFeatures": [
        // one or more Attribute-Map's (atleast one should match with each ARM AttributeMap)
        {
            "id": "my-unique-feature-id"
        },
        {
            "storageId": "id-with-wild-card-*"
        },
        {
            "tags": [
                "my-unique-tag",
                "some-common-tag-with-wild-card-*"
            ]
        }
    ]
}
```

Thumb rule is - **Atleast One** access AttributeMap should match with each (ARM) resource AttributeMap, to allow access for that Action.

So:

* If Action has no attribute maps - Access is NOT allowed.
* If Action has one or more attribute maps - Atleast one of the AttributeMaps should match



### Attribute Map

Every AttributeMap can have zero or more **Access Attributes**.

For example:

```json
{
    "readFeatures": [
        {
            // empty attribute map (is a MATCH)
        },
        {
            // one or more attributes (all should match against ARM)
            "id": "my-unique-feature-id",
            "storageId": "id-with-wild-card-*",
            "tags": [
                "my-unique-tag",
                "some-common-tag-with-wild-card-*"
            ]
        }
    ]
}
```

Thumb rule is - **All specified Access Attributes** should match with (ARM) resource attributes, to call an AttributeMap to have a MATCH.

So:

* If AttributeMap has zero/no attributes - Map is considered as a MATCH.
* If AttributeMap has one or more attributes - Then all attributes should match.



### Access Attribute

Every Access Attribute can be a **Scalar** or a **List** of Scalar, where Scalar can be exact value or (in some cases) value with a wild card (*).

For example:


```json
{
    "id": "my-unique-feature-id",       // exact value match
    "storageId": "id-with-wild-card-*", // startsWith match
    "tags": [                           // all should match
        "my-unique-tag",
        "some-common-tag-with-wild-card-*"  // startsWith match in list
    ]
}
```

Thumb rule is - **All specified values** should match with (ARM) resource attribute values, to call an Access Attribute a MATCH.

So:

* If AccessAttribute has an exact Scalar value - The value should match with resource attribute.
* If AccessAttribute has a wild-card Scalar value - The prefix portion of the value should match with resource attribute.
* If AccessAttribute has a List of Scalar value - Each value should match (as Scalar comparison).





---

## 2. REST API Authorization


### JWT expectations

Naksha REST App accepts [JWT token](https://datatracker.ietf.org/doc/html/rfc7519) as part of:

* Header `Authorization` = `Bearer <jwt-token>`, OR
* Query parameter `access_token` = `<jwt-token>`

The [JWT token](https://datatracker.ietf.org/doc/html/rfc7519) must be digitally signed by a trusted partner using its private key (`RS256` encryption algorithm),
and the public key of this trusted partner must be added into the Naksha configuration so that the service can validate the token's authenticity.

Standard JWT format:

```text
  base64UrlEncoded(header)
+ "."
+ base64UrlEncoded(payload)
+ "."
  signature
```

The sample encoded JWT (can be viewed on [jwt.io](https://jwt.io)):

```text
eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBJZCI6IndlYi1jbGllbnQtYXBwLWlkIiwiYXV0aG9yIjoibXktdXNlci1pZCIsInVybSI6eyJ4eXotaHViIjp7InJlYWRGZWF0dXJlcyI6W3sic3RvcmFnZUlkIjoiZGV2LSoifV19fSwiaWF0IjoxNzA0MDYzNTk5LCJleHAiOjE3MDQwNjcxOTl9.g7maKIpDQ6d8MoC7lPQDa_6BLKV5HhpN9t1BkcdFmNSetc-dHIcor_mhvc4GNpJELEMCBiTiF8RdlY_PEOooJc4Ixx5yWFoeIEaKv-aunvf6TZsOlD8F5KX8CmL8QzEO7t8YrSVz-F3WYrw1rmnl_1WC2tscMUBvfFHRifq3h7F46ZMswO6fm8AGHW0bbSeDCwK2VcjkYOwGVYWmSPodtxT7ie8uxJlAFxaCGzxV1WkVnrqIZFdPcnq3hM_FjbSw01MxOD3qdiL47HRXQnvOzsKjhi5ihClihwiua4N9xOeq2I8nX5_2YJIRWjS8pAozRp7cfnhb15Sm8JevqEwz1A
```

JWT payload is expected to have custom claims `appId`, `author` and `urm`, which is then populated into [NakshaContext](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/NakshaContext.java) whenever REST API request is picked up for processing.

```json
{
    "appId": "web-client-app-id",
    "author": "my-user-id",
    "urm": {
        "xyz-hub": {
            "readFeatures": [
                {
                    "storageId": "dev-*"
                }
            ]
        }
    },
    "iat": 1704063599,
    "exp": 1704067199
}
```

### Auth Modes

Service can be executed in two modes based on [AuthorizationType](../here-naksha-app-service/src/main/java/com/here/naksha/app/service/http/auth/Authorization.java) specified as part of startup [config](../here-naksha-lib-hub/src/main/java/com/here/naksha/lib/hub/NakshaHubConfig.java):

* `DUMMY`
  - Dummy mode
  - useful, when we want to run service in local / test / dev environment, where security is not that important
  - it will use internally generated super-user URM as part of NakshaContext to allow full access to all resources
* `JWT`
  - Real JWT mode
  - useful, for cloud / prod environment, where security is MUST
  - it will validate the JWT as part of each REST API request and extract the URM for further authorization checks
  - Absence of or Invalid JWT will result into Http error code 401 - Unauthorized




---

## 3. Supported Resource Actions and Attributes

Matrix of all supported **Actions** and **Attributes** for validating authorization against individual resource operation:

**NOTE** for **Attributes**:
  * For all, **exact** String value comparison is supported by default.
  * For some, **wild-card** value (e.g. `storage-dev*`) is also supported (explicitly mentioned where applicable)
  * For all, **List** of values is supported by default.

**NOTE** for **Actions**:
  * **Limited View** - means, read of resource is allowed BUT without exposing `properies` object (so typically one can read `id`, `title`, `description` etc but not `properties` object)
  * **Filtered** read restricted resource instances - means, instances that are read restricted, they will be filtered from the result rather than raising exception like forbidden access




### 3.1 Resource - Storage

#### Attributes

* `id` - wild-card supported
* `tags` - wild-card supported - prop path `properties.@ns:com:here:xyz.tags`
* `appId` - `properties.@ns:com:here:xyz.appId`
* `author` - `properties.@ns:com:here:xyz.author`
* `className`
* **Space** related:
  * `spaceId` - wild-card supported

#### Actions

| Action        | Allowed operations                                                                     | Remarks |
|---------------|----------------------------------------------------------------------------------------|---------|
| `useStorages` | Get Storage Implementation for a given storageId (and also check spaceId, if supplied) |         |
| `useStorages` | Limited view - Read Features from virtual space (`naksha:storages`)                    |         |
| `manageStorages` | Full control - Read/Write Features from/to virtual space (`naksha:storages`)           |         |




### 3.2 Resource - EventHandler

#### Attributes

* `id` - wild-card supported
* `tags` - wild-card supported - prop path `properties.@ns:com:here:xyz.tags`
* `appId` - `properties.@ns:com:here:xyz.appId`
* `author` - `properties.@ns:com:here:xyz.author`
* `className`

#### Actions

| Action | Allowed operations                                                                 | Remarks                                                                                                           |
|--------|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `useEventHandlers`       | Limited view - To be able to associate handler while configuring a Space           | For Update Space request, check is applicable only for new handler Ids being associated (not existing ones) |
| `useEventHandlers`       | Limited view - Read Features from virtual space (`naksha:event_handlers`)          |                                                                                                                   |
| `manageEventHandlers`    | Full control - Read/Write Features from/to virtual space (`naksha:event_handlers`) |                                                                                                                   |




### 3.3 Resource - Space

#### Attributes

* `id` - wild-card supported
* `tags` - wild-card supported - prop path `properties.@ns:com:here:xyz.tags`
* `appId` - `properties.@ns:com:here:xyz.appId`
* `author` - `properties.@ns:com:here:xyz.author`
* **EventHandler** related:
  * `eventHandlerIds` - wild-card supported

#### Actions

| Action         | Allowed operations                                                         | Remarks                                                                                           |
|----------------|----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `useSpaces`    | Allow creating pipeline for a spaceId                                      | Kind of additional check, to restrict unintentional space access via some other pipeline/handler. |
| `useSpaces`    | Limited view - Read Features from virtual space (`naksha:spaces`)          |                                                                                                   |
| `manageSpaces` | Full control - Read/Write Features from/to virtual space (`naksha:spaces`) |                                                                                                   |




### 3.4 Resource - XyzFeature

#### Attributes

* `id` - wild-card supported
* `tags` - wild-card supported - prop path `properties.@ns:com:here:xyz.tags`
* `appId` - `properties.@ns:com:here:xyz.appId`
* `author` - `properties.@ns:com:here:xyz.author`
* **Storage** related:
  * `storageId` - wild-card supported
  * `storageTags` - wild-card supported - Storage prop path `properties.@ns:com:here:xyz.tags`
* **XyzCollection** related:
  * `collectionId` - wild-card supported
  * `collectionTags` - wild-card supported - XyzCollection prop path `properties.@ns:com:here:xyz.tags`

#### Actions

| Action           | Allowed operations                                   | Remarks                                                                                                                         |
|------------------|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `readFeatures`   | Read specific XyzFeatures from Storage, Collection   |                                                                                                                                 |
| `createFeatures` | Create specific XyzFeatures in Storage, Collection   | For Upsert/PUT operation, resultant `create`/`update` can be determined only based on Feature's presence in Storage, Collection |
| `updateFeatures` | Update specific XyzFeatures in Storage, Collection   | For Upsert/PUT operation, resultant `create`/`update` can be determined only based on Feature's presence in Storage, Collection |
| `deleteFeatures` | Delete specific XyzFeatures from Storage, Collection |                                                                                                                                 |




### 3.5 Resource - XyzCollection

#### Attributes

* `id` - wild-card supported
* `tags` - wild-card supported - prop path `properties.@ns:com:here:xyz.tags`
* `appId` - `properties.@ns:com:here:xyz.appId`
* `author` - `properties.@ns:com:here:xyz.author`
* **Storage** related:
  * `storageId` - wild-card supported
  * `storageTags` - wild-card supported - Storage prop path `properties.@ns:com:here:xyz.tags`

#### Actions

| Action              | Allowed operations                             | Remarks |
|---------------------|------------------------------------------------|---------|
| `readCollections`   | Read specific Collections from Storage         |         |
| `createCollections` | Create specific Collections in Storage         |         |
| `updateCollections` | Update specific Collections in Storage         |         |
| `deleteCollections` | Delete/Drop specific Collections from Storage  |         |




