## Naksha_1.1.1

- Fixes:
  - PGSQL triggers fixed to handle missing `rtcts` and `version` fields for existing data migrated to newly created Naksha spaces
  - `xyz` namespace object fixed to return correct field values in API responses. We now use PGSQL based insert/update/delete feature operations.
  - Fixed Sequencer module to skip external connectors that don't have DB credentials to perform sequencing
  - Fixed Publisher module to skip SNS message attribute where it is found as empty string in message event
  - Fixed `handle` value in Iterate API response to support subsequent iterate operations using previous `handle` as offset
  - Event Publisher `fetch-newer-transactions` query fixed to improve performance (from 10 mins per call to 1 sec) for significantly bigger history table (e.g. 80+ million records or 190+ GB)

## Naksha_1.1.0

- *[MCPODS-5082](https://devzone.it.here.com/jira/browse/MCPODS-5082)* - First working version deployed in E2E, to enable migration of UTM Dev Spaces from DataHub to Naksha setup
- Introduced new **Transaction logging** with help of Postgres triggers (with transactional atomicity).
- Added Background Transaction Handler jobs to sequence and publish the SpaceDB transactions on desired AWS SNS topic, **with "Atleast-Once" messaging guarantee**.
- Segregated use of DB instances i.e. Naksha AdminDB v/s Foreign SpaceDBs.
- SNS Feature Publisher supports additional message attribues as per Subscription config params `customMsgAttributes`
- Fixes: 
    - GET /connectors API authorization fixed to correctly validate request-rights against `manageConnectors` token-rights.


## Naksha_1.0.0

- *[MCPODS-4933](https://devzone.it.here.com/jira/browse/MCPODS-4933)* - First PoC version, demo'ed in E2E, as an alternative to DataHub APIs

