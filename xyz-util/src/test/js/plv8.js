/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

const {Client} = require("pg");
const deasync = require("deasync");

let pgClient = new Client({
  host: "localhost",
  user: "postgres",
  password: "password",
  database: "postgres",
  options: `-c search_path=public,topology,hub.common,hub.geo,hub.feature_writer,hub.h3,hub.ext`
});

let clientConnected = false;
pgClient.connect().then(result => {
  clientConnected = true;
  console.log(result);
});

class PreparedPlan {
  name;
  sql;
  parameterTypes;

  static planCounter = 0;

  constructor(sql, parameterTypes = []) {
    this.name = "prepared_statement_" + PreparedPlan.planCounter++;
    this.sql = sql;
    this.parameterTypes = parameterTypes;
  }

  /**
   * @returns {PreparedPlan}
   */
  prepare() {
    //plv8.execute(`PREPARE ${this.name}${this._parameterTypeList()} AS ${this.sql}`);
    return this;
  }

  _parameterTypeList() {
    return !this.parameterTypes.length ? "" : "(" + this.parameterTypes.join(", ") + ")";
  }

  execute(args = []) {
    //TODO: Activate prepared statements; For some reason the EXECUTE call does not work with the PG client
    //return plv8.execute(`EXECUTE ${this.name}${this._argumentList(args)}`, args);
    return plv8.execute(this.sql, args);
  }

  _argumentList(args) {
    return !args.length ? "" : "(" + args.map((element, index) => "$" + (index + 1)).join(", ") + ")";
  }

  free() {
    //plv8.execute(`DEALLOCATE ${this.name}`);
  }
}

global.NOTICE = "NOTICE";
global.ERROR = "ERROR";
global.plv8 = {
  global: global,
  elog: console.log,
  execute(sql, params) {
    if (!clientConnected)
      deasync.loopWhile(() => !clientConnected);

    let queryResult = null;
    pgClient.query(substituteSQL(sql, params)).then(result => queryResult = result);
    deasync.loopWhile(() => queryResult == null);
    return queryResult.rows;
  },
  prepare(sql, typeNames = []) {
    return new PreparedPlan(sql, typeNames).prepare();
  }
  //TODO: Support prepared / batch queries
};

//NOTE: The following helper function is necessary, because the NPN pg module does not support to have multiple SQL statements within prepared statements or queries.
/**
 * Substitute $1, $2, ... placeholders in a SQL string with values from params array
 * @param {string} sql - SQL string with $1, $2, ... placeholders
 * @param {Array} params - Array of parameter values
 * @returns {string} SQL string with parameters substituted
 */
function substituteSQL(sql, params) {
  return sql.replace(/\$(\d+)/g, (match, number) => {
    const index = parseInt(number, 10) - 1;
    if (index < 0 || index >= params.length) {
      throw new Error(`No parameter provided for placeholder ${match}`);
    }
    const value = params[index];

    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'number') return value.toString();
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    if (value instanceof Date) return `'${value.toISOString()}'`;

    // Escape single quotes for strings
    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`;
    }

    // For objects/arrays, JSON stringify
    return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
  });
}