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
        },
        "naksha_txn": function () {
            /* 20230101 is just a magic number for naming lock, it looks like date without special reason, just to associate it with what we are doing here. */
            const LOCK_NAME = 20230101;
            const SEQ_DIVIDER = BigInt(100000000000);
            let txn;

            let value = plv8.execute("SELECT current_setting('naksha.txn', true) as value")[0].value;
            if (value !== '' && value !== null) {
                return BigInt(value);
            }

            /* prepare current yyyyMMdd as number i.e. 20231006 */
            let date = new Date();
            let txDate = BigInt(date.getFullYear() * 10000 + (date.getMonth() + 1) * 100 + date.getDate());

            /*
                txi should start with current date  20231006 with seq number "at the end"
                example: 2023100600000000007
             */
            let txi = plv8.execute("SELECT nextval('naksha_tx_object_id_seq') as txi")[0].txi;

            let seqDate = txi / SEQ_DIVIDER; /* returns as number seq prefix which is yyyyMMdd  i.e. 20231006 */

            /* verification, if current day is not same as day in txi we have to reset sequence to new date and counting from start.*/
            if (seqDate !== txDate) {
                try {
                    plv8.execute("SELECT pg_advisory_lock(" + LOCK_NAME + ")")
                    seqDate = txi / SEQ_DIVIDER;
                    if (seqDate !== txDate) {
                        txn = txDate * SEQ_DIVIDER;
                        plv8.execute("SELECT setval('naksha_tx_object_id_seq', " + txn + ", true)");
                    } else {
                        txn = txi;
                    }
                } finally {
                    plv8.execute("SELECT pg_advisory_unlock(" + LOCK_NAME + ")");
                }
            } else {
                txn = txi;
            }
            /* is_called set to true guarantee that next val will be +1 */
            plv8.execute("SELECT SET_CONFIG('naksha.txn', '" + txn + "', true)");
            return txn;
        }
    };
    plv8.naksha.log("Ready")
}