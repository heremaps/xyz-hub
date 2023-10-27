# Introduction

Please, disable IntelliJ Auto-Formatter, it sucks unbelievable:

- Open (File|IntellilJ IDEA on Mac)
- Settings
- Editor
- Code Style
- SQL
- General
- Check "Disable formatting"

# Things we need to remember

Due to partitioning of the big tables, we need to change **cluster** and **group** parameters:

- `show max_parallel_workers = 16`
- `show max_parallel_workers_per_gather = 16`
  - We split HEAD table into 16 partitions for big data collections, therefore we should be able to query them in parallel.
- `show max_worker_processes = 1024`
  - We should be able to at least execute 64 parallel big table queries.

# Links

## Functions
- [Locking](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)
- [Date/Time](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)
- [String Formatting](https://www.postgresql.org/docs/current/functions-formatting.html)

## PostGIS Functions
- https://postgis.net/docs/ST_Centroid.html

## Details about error handling:
- https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
- https://www.postgresql.org/docs/current/errcodes-appendix.html
- https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING

## Index optimization
- https://www.postgresql.org/docs/current/sql-createindex.html#SQL-CREATEINDEX-STORAGE-PARAMETERS

## Concurrency & Function Volatility Categories
- https://www.postgresql.org/docs/current/explicit-locking.html
- https://www.postgresql.org/docs/current/xfunc-volatility.html

In a nutshell:

- A **VOLATILE** function can do anything, including modifying the database. It can return different results on successive calls with the same arguments. The optimizer makes no assumptions about the behavior of such functions. A query using a volatile function will re-evaluate the function at every row where its value is needed.
- A **STABLE** function cannot modify the database and is guaranteed to return the same results given the same arguments for all rows within a single statement. This category allows the optimizer to optimize multiple calls of the function to a single call. In particular, it is safe to use an expression containing such a function in an index scan condition. (Since an index scan will evaluate the comparison value only once, not once at each row, it is not valid to use a VOLATILE function in an index scan condition.)
- An **IMMUTABLE** function cannot modify the database and is guaranteed to return the same results given the same arguments forever. This category allows the optimizer to pre-evaluate the function when a query calls it with constant arguments. For example, a query like SELECT ... WHERE x = 2 + 2 can be simplified on sight to SELECT ... WHERE x = 4, because the function underlying the integer addition operator is marked IMMUTABLE.

## Other useful information about PostgesQL
- https://www.postgresql.org/docs/current/catalog-pg-class.html
- https://www.postgresql.org/docs/current/catalog-pg-trigger.html

# DBeaver - Notification / Debugging
Debugging in DBeaver can be done by adding notices like:

```sql
RAISE NOTICE 'Hello';
```

See: https://www.postgresql.org/docs/16/sql-notify.html

To show the notifications switch to Output tab (Ctrl+Shift+O).

## Session / Transaction

Every connection to PostgresQL starts a new session as soon as the client authenticates. The session is wired to the underlying socket / connection. Within each session there can only be exactly one transaction at a given time. Therefore, there is a 1:1:1 relation between connection, session and transaction.

This is important to understand the configuration values. They are by default bound to the session, so `SET "naksha.x" TO 'Hello World';` will be sticky for the whole session, while `SET LOCAL "naksha.x" TO 'Hello World';` will only be available until the current transaction is either rolled-back or committed. This is important for caching!

Therefore: The effects of SET LOCAL last only till the end of the current transaction.

# Helpers

To insert some random data into do this:

```sql
WITH rnd AS (select md5(random()::text) as id, ST_Force3D(ST_GeneratePoints('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 10)) as g from generate_Series(1,5000))
INSERT INTO foo (jsondata,geo) SELECT ('{"id":"'||id||'"}')::jsonb as jsondata, g FROM rnd;
```
