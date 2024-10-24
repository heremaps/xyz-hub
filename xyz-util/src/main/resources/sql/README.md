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