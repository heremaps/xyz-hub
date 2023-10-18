/**

 A VOLATILE function can do anything, including modifying the database. It can return different results on successive calls with the same
            arguments. The optimizer makes no assumptions about the behavior of such functions. A query using a volatile function will
            re-evaluate the function at every row where its value is needed.

 A STABLE function cannot modify the database and is guaranteed to return the same results given the same arguments for all rows within a
          single statement. This category allows the optimizer to optimize multiple calls of the function to a single call. In particular,
          it is safe to use an expression containing such a function in an index scan condition. (Since an index scan will evaluate the
          comparison value only once, not once at each row, it is not valid to use a VOLATILE function in an index scan condition.)

 An IMMUTABLE function cannot modify the database and is guaranteed to return the same results given the same arguments forever. This
              category allows the optimizer to pre-evaluate the function when a query calls it with constant arguments. For example, a
              query like SELECT ... WHERE x = 2 + 2 can be simplified on sight to SELECT ... WHERE x = 4, because the function underlying
              the integer addition operator is marked IMMUTABLE.

 Using PLv8:
 - https://plv8.github.io/
 - https://github.com/plv8/plv8u/blob/master/doc/plv8.md

 Using pg_hint_plan:
 - https://pg-hint-plan.readthedocs.io/en/latest/index.html
 - https://dev.to/yugabyte/build-a-postgresql-docker-image-with-pghintplan-and-pgstatstatements-46pa

*/

if (true) {
  plv8.naksha = {
    "log": function (msg) {
      plv8.elog(WARNING, msg);
    }
  };
  plv8.naksha.log("Ready")
}