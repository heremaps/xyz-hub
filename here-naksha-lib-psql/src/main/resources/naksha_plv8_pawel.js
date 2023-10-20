// create transaction table ... (called only ones per storage)
plv8.naksha.init_storage = function () {
  // TODO:
};
plv8.naksha.naksha_txn = function () {
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
};
