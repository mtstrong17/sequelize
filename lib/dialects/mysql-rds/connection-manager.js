'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const Promise = require('../../promise');
const logger = require('../../utils/logger');
const debug = logger.getLogger().debugContext('connection:sqlite');
const dataTypes = require('../../data-types')['mysql-rds'];
const sequelizeErrors = require('../../errors');
const parserStore = require('../parserStore')('mysql-rds');

class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    super(dialect, sequelize);

    // We attempt to parse file location from a connection uri
    // but we shouldn't match sequelize default host.
    if (this.sequelize.options.host === 'localhost') {
      delete this.sequelize.options.host;
    }

    this.connections = {};
    this.lib = this._loadDialectModule('aws-sdk');
    console.log(this.sequelize.options.awsProfile)
    if (this.sequelize.options.awsProfile) {
      var credentials = new this.lib.SharedIniFileCredentials({ profile: this.sequelize.options.awsProfile });
      this.lib.config.credentials = credentials;
    }
    this.lib.config.update({ region: this.sequelize.options.awsRegion });
    this.refreshTypeParser(dataTypes);
  }
  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }
  refreshTypeParser(dataTypes) {
    // parserStore.refresh(dataType);
  }

  getConnection(options) {
    return new Promise((resolve, reject) => {
      resolve(new DBConnection(this.lib, this.sequelize.options))
    })
  }
}

class DBConnection {
  constructor(lib, options) {
    this.options = options
    this.rds = new lib.RDSDataService()
  }
  execute(sql, replacements, callback) {
    console.log('Replacements::', replacements)
      replacements.forEach(replacement => {
        sql = sql.replace('?', `"${replacement}"`)
      })
      let params = {
        awsSecretStoreArn: this.options.awsSecretStoreArn, /* required */
        dbClusterOrInstanceArn: this.options.dbClusterOrInstanceArn, /* required */
        sqlStatements: sql, /* required */
        database: this.options.database,
      }
      this.rds.executeSql(params)
      .promise()
      .then((result) => {
        console.log("RORO:", result.sqlStatementResults[0].numberOfRecordsUpdated)
        callback(null, {affectedRows: result.sqlStatementResults[0].numberOfRecordsUpdated})
      })
      .catch(err => {
        callback(err)
      })
  }
  query(sql, callback) {
    let params = {
      awsSecretStoreArn: this.options.awsSecretStoreArn, /* required */
      dbClusterOrInstanceArn: this.options.dbClusterOrInstanceArn, /* required */
      sqlStatements: sql.sql, /* required */
      database: this.options.database,
    }
    this.rds.executeSql(params)
    .promise()
    .then((result) => {
      if (result.sqlStatementResults[0].resultFrame) {
        let myResults = result.sqlStatementResults[0].resultFrame.records.map((record) => {
          let values = {}
          record.values.forEach((value, index) => {
            let columnLabel = result.sqlStatementResults[0].resultFrame.resultSetMetadata.columnMetadata[index].label
            let valueKeys = Object.keys(value)
            for (let i = 0; i < valueKeys.length; i++) {
              if (value[valueKeys[i]]) {
                values[columnLabel] = value[valueKeys[i]]
                break;
              }
            }
          })
          return values
        })
        callback(null, myResults)
      } else {
        callback(null, {affectedRows: result.sqlStatementResults[0].numberOfRecordsUpdated})
      }
    })
    .catch(err => {
      callback(err)
    })
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
