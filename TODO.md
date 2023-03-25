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

## Event Processing

### Rest API

- Optionally accept external stream-id like `streamId = routingContext.request().headers().get("Stream-Id")`
- Create new **Task** via `task = new FooTask(streamId)`
- Initialize the task from the **RoutingContext** via `task.setBar(value); ...`
- Add response listener to send the response via `task.setCallback(this::sendResponse)`
- Start the task via `task.start()`
- When start throws an exception, send back an error response.
- All this can be embedded into a helper method of the task, like `FooTask.startFromRoutingContext(routingContext)`

### Task Design

- The task itself will create a new event in the constructor, and the task methods shall initialize it.
- When the **execute** method invoked, the task shall initialize the pipeline (add all event handlers).
- Initialization of the pipeline is done in **execute**, because that time the task is bound to the thread, therefore logging works as expected.
- Finally, it will process the event via `return sendEvent(event)`
- The real processing is done by the event handlers that where attached to the pipeline.

### Child Tasks

For example for the view implementation or at other places it may be necessary to run child-tasks, 
optionally parallel. This is now simple:

- Create a child task `childTask = new FooTask()`
- Set all task params via `task.setBar()`
- Start the task: `Future<@NotNull XyzResponse> responseFuture = childTask.startWithoutLimit()`
- Do this will all child-tasks that should be executed in parallel.
- Then eventually join all of them in order via `responseFuture.get()`.

### Connectors (Event Handler)

An event handler processes an event and is part of the event-pipeline, created by a task. For every
task a new event handler instance is created and called thread safe (in a single thread). All
event handlers only implement a single method:

`@NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException;`

Within this method they have a couple of options:

- Review or modify the event (`eventContext.event()`).
- Replace the event with another one.
- Send it towards the pipeline end via `eventContext.sendUpstream(event)`.
- Reconfigure the pipeline to modify the event processing.
- Optionally, post-process the response receive from the handlers behind.
- Abort the event and send back an error response (either before sending forward or later).
- Consume the event, fulfill it and send back a valid response.
 
All event handlers need to be configured, before they can be used. This is done in the code in the 
constructor of the event handler implementation:

`protected EventHandler(@NotNull Map<@NotNull String, @Nullable Object> params) throws XyzErrorException {...}`

The params are provided from the user of the handler and there is a helper class available to parse
them (`EventHandlerParams`). Basically, this is called connecting and therefore the corresponding 
configured event handler called `connector`.

### Spaces

A space configures a collection of features, and the way to query or modify them. Every space must
have at least one storage connector, referred via `connectorId`. Optionally, an unlimited amount
of processors can be added in-front of the storage connector using the `processors` array.

Both, the storage connector and the processors, are just referred via their connector-id. Each
connector is basically a new instance of the corresponding event handler combined with the 
parameters configured. Technically the parameters are secrets, they can only be accessed by the
connector admins (**manageConnectors**), while the user of a connector (**accessConnectors**) can 
use the connector is has access to with any of his spaces.

All event handlers have access to the space parameters (`params`) as defined by the space. The
space parameters are mainly to modify certain aspects of how the handlers process the event. For
example they can be used to map a space to a different table name, when using the `psql` storage
connector.
