/**
 * This file only exists to syntax highlighting.
 */
let plv8 = {
  version: "3.1.6",
  elog: function (level, msg) {
  },
  // Quote literal in single quotes.
  quote_literal: function (str) {
  },
  // Quote identifiers in double quotes.
  quote_ident: function (str) {
  },
  nullable: function (str) {
  },
  execute: function (query, args) {
  },
  find_function: function (name, arg_types) {
  },
  get_window_object: function () {
  }
};
// DEBUG1 .. DEBUG5 	Provides successively-more-detailed information for use by developers. 	DEBUG 	INFORMATION
const DEBUG5 = "DEBUG5";
const DEBUG4 = "DEBUG4";
const DEBUG3 = "DEBUG3";
const DEBUG2 = "DEBUG2";
const DEBUG1 = "DEBUG1";
// LOG 	Reports information of interest to administrators, e.g., checkpoint activity. 	INFO 	INFORMATION
const LOG = "LOG";
// INFO 	Provides information implicitly requested by the user, e.g., output from VACUUM VERBOSE. 	INFO 	INFORMATION
const INFO = "INFO";
// NOTICE 	Provides information that might be helpful to users, e.g., notice of truncation of long identifiers. 	NOTICE 	INFORMATION
const NOTICE = "NOTICE";
// WARNING 	Provides warnings of likely problems, e.g., COMMIT outside a transaction block. 	NOTICE 	WARNING
const WARNING = "WARNING";
// ERROR 	Reports an error that caused the current command to abort. 	WARNING 	ERROR
const ERROR = "ERROR";
//FATAL 	Reports an error that caused the current session to abort. 	ERR 	ERROR
//PANIC 	Reports an error that caused all database sessions to abort. 	CRIT 	ERROR

// new database row for INSERT/UPDATE operations in row-level triggers. This variable is null in statement-level triggers and for DELETE operations.
let NEW = {
  i: 0,
  geo: {},
  jsondata: {}
};

// old database row for UPDATE/DELETE operations in row-level triggers. This variable is null in statement-level triggers and for INSERT operations.
let OLD = {
  i: 0,
  geo: {},
  jsondata: {}
};

// name of the trigger which fired.
const TG_NAME = "name of the trigger which fired.";

// BEFORE, AFTER, or INSTEAD OF, depending on the trigger's definition.
const TG_WHEN = "";

// ROW or STATEMENT, depending on the trigger's definition.
const TG_LEVEL = "";

// operation for which the trigger was fired: INSERT, UPDATE, DELETE, or TRUNCATE.
const TG_OP = "";

// object ID of the table that caused the trigger invocation.
const TG_RELID = "oid";

// table that caused the trigger invocation.
const TG_TABLE_NAME = "";

// schema of the table that caused the trigger invocation.
const TG_TABLE_SCHEMA = "";

// number of arguments given to the trigger function in the CREATE TRIGGER statement.
const TG_NARGS = 1;

// arguments from the CREATE TRIGGER statement. The index counts from 0. Invalid indexes (less than 0 or greater than or equal to tg_nargs) result in a null value.
const TG_ARGV = ["text"];
