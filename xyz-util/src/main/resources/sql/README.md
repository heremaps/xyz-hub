This folder contains SQL scripts that will be installed into all connector databases
automatically at initialization time.
Each script will be installed into its own schema named like the file itself (without file suffix)
with an added version number of the software version in which the script was updated the last time.
Additionally, the "latest" version of each script will be installed into the "latest" schema
(A schema without a version suffix that will always point to the latest installed version).

E.g., a script named common.sql would be installed into the following schemas if
it has changes in the current software version 1.0.1:

- common:1.0.1
- common

When changed content is installed into the "latest" schema, all functions in that schema are dropped first, with CASCADE,
so objects that depend on one of them are dropped too, also outside the schema. Then the script's functions are created
again. The schema and its other objects are kept. The scripts create their types only if they are missing, so a changed
type definition is not applied to the "latest" schema and needs its own migration there.