/*!
 * Cassandra connector for LoopBack
 */
var cassandra = require('cassandra-driver');
var Connector = require('loopback-connector').Connector;
var util = require('util');
var debug = require('debug')('loopback:connector:cassandra');
var cassandraUtil = require('../lib/cassandra_util');
var logger = console;
var Promise = require('bluebird');

var TAG = 'loopback-connector-cassandra: ';

/**
 *
 * Initialize the Cassandra connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header Cassandra.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
    if (!cassandra) {
        return;
    }

    var dbSettings = dataSource.settings || {};

    var connector = new Cassandra(cassandra, dbSettings);
    dataSource.connector = connector;
    dataSource.connector.dataSource = dataSource;

    /**
     * define DataAccessObject to mixin to model class
     * @constructor
     */
    var DataAccessObject = function() {
    };

    // Copy the methods from default DataAccessObject
    if (dataSource.constructor.DataAccessObject) {
        for (var i in dataSource.constructor.DataAccessObject) {
            DataAccessObject[i] = dataSource.constructor.DataAccessObject[i];
        }
        /* eslint-disable one-var */
        for (var i in dataSource.constructor.DataAccessObject.prototype) {
            DataAccessObject.prototype[i] = dataSource.constructor.DataAccessObject.prototype[i];
        }
        /* eslint-enable one-var */

        DataAccessObject.create = function (data, callback) {
            return connector.create(this.modelName, data, callback);
        };

        DataAccessObject.upsert = function (data, callback) {
            return connector.update(this.modelName, data, false, callback);
        };

        DataAccessObject.findOne = function (filter, columns, callback) {
            return connector.findOne(this.modelName, filter, columns, callback);
        };

        DataAccessObject.find = function (filter, columns, orderBy, limit, callback) {
            return connector.find(this.modelName, filter, columns, orderBy, limit, callback);
        };

        DataAccessObject.patch = function (data, callback) {
            return connector.update(this.modelName, data, false, callback);
        };

        DataAccessObject.update = function (data, callback) {
            return connector.update(this.modelName, data, true, callback);
        };

        DataAccessObject.delete = function (keyValues, columns, callback) {
            return connector.delete(this.modelName, keyValues, columns, callback);
        };

        DataAccessObject.eachPage = function (keyValues, options, columns, orderBy, eachCb, endCb) {
            return connector.eachPage(this.modelName, keyValues, options, columns, orderBy, eachCb, endCb);
        };

        DataAccessObject.eachPageWhere = function (whereClause, params, options, columns, orderBy, eachCb, endCb) {
            return connector.eachPageWhere(this.modelName, whereClause, params, options, columns, orderBy, eachCb, endCb);
        };

        DataAccessObject.batch = function () {
            return new BatchExecutor(this.modelName, connector)
        };
    }
    connector.DataAccessObject = DataAccessObject;

    if (callback) {
        dataSource.connecting = true;
        dataSource.connector.connect(callback);
    }

};

/**
 * Cassandra connector constructor
 *
 * @param {Cassandra} cassandra Cassandra node.js binding
 * @param settings
 * @options {Object} settings An object for the data source settings.
 * See [node-postgres documentation](https://github.com/brianc/node-postgres/wiki/Client#parameters).
 * @property {String} url URL to the database, such as 'postgres://test:mypassword@localhost:5432/devdb'.
 * Other parameters can be defined as query string of the url
 * @property {String} hostname The host name or ip address of the Cassandra DB server
 * @property {Number} port The port number of the Cassandra DB Server
 * @property {String} user The user name
 * @property {String} password The password
 * @property {String} database The database name
 * @property {String} numOfServer Num of connect point to cassandra db
 * @property {Boolean} ssl Whether to try SSL/TLS to connect to server
 *
 * @constructor
 */
function Cassandra(cassandra, settings) {
    this.constructor.super_.call(this, 'cassandra', settings);
    this.cassandra = cassandra;
    this.settings = settings;
    if (settings.debug) {
        debug('Settings %j', settings);
    }
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(Cassandra, Connector);

Cassandra.prototype.getDefaultSchemaName = function () {
    return 'public';
};

/**
 * Connect to Cassandra
 * @callback {Function} [callback] The callback after the connection is established
 */
Cassandra.prototype.connect = function (callback) {
    var self = this;
    var cassandraClient = this.cassandraClient = require('../lib/cassandra_client')(self.settings)
    process.nextTick(function () {
        callback && callback(null, cassandraClient);
    });
};

Cassandra.prototype.getTypes = function() {
    return ['db', 'nosql', 'cassandra'];
};

Cassandra.prototype.create = function (model, data, callback) {
    var self = this;
    var createInfo = this.buildCreate(model, data);
    var query = createInfo.query;
    var params = createInfo.params;

    return new Promise(function (resolve, reject) {
        self.cassandraClient.execute(query, params, {prepare: true}, function (error, results) {
                if(error) {
                    logger.log('error', TAG + 'insertRow: ' + JSON.stringify(error), {query: query, instance: params});
                    logger.log(error);
                }

                callback && callback(error, results);
                if(error) {
                    reject(error);
                } else {
                    resolve(results);
                }
            });
    })
}

Cassandra.prototype.findOne = function (model, keyValues, columns, callback) {
    if(typeof columns == 'function') {
        callback = columns;
        columns = null;
    }

    var self = this;
    var modelDefine = this.getModelDefinition(model);
    var partitionKeys = modelDefine.model.definition.settings.partitionKeys || [];
    var clustering = modelDefine.model.definition.settings.clustering || [];

    var keys = partitionKeys.concat(clustering);

    for(var i = 0; i < keys.length; i ++) {
        if(!keyValues[keys[i]]) {
            var error = {};
            error.ec = 422;
            error.message = 'partition or clustering key ' + keys[i] + ' must be defined!';
            return callback(error);
        }
    }

    var whereClause = cassandraUtil.genWhereClause(keys);
    var params = [];
    for(var i = 0; i < keys.length; i ++) {
        params.push(keyValues[keys[i]]);
    }
    var selectColumn = cassandraUtil.genColumn(columns) || '*';
    var tableName = this.getTableName(model);

    return new Promise(function (resolve, reject) {
        self.cassandraClient.execute('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : ''),
            params, {prepare: true}, function (error, results) {
                if(error) {
                    logger.log('error', TAG + 'selectRows: ' + JSON.stringify(error), {table_name: tableName, where: whereClause, params: params});
                    console.log(error);
                }

                cassandraUtil.responseOneRow(error, results, function (error, info) {
                    callback && callback(error, info);
                    if(error) {
                        reject(error);
                    } else {
                        resolve(info);
                    }
                });
            });
    })
}

Cassandra.prototype.all = Cassandra.prototype.find = function (model, where, columns, orderBy, limit, callback) {
    if(typeof columns == 'function') {
        callback = columns;
        columns = null;
        orderBy = null;
        limit = null;
    }

    var self = this;

    var whereClauseInfo = this.buildWhere(model, where);
    var whereClause = whereClauseInfo.where;
    var params = whereClauseInfo.params;

    var selectColumn = cassandraUtil.genColumn(columns) || '*';
    var tableName = this.getTableName(model);

    return new Promise(function (resolve, reject) {
        self.cassandraClient.execute('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : '') +
            (orderBy ? (' ORDER BY ' + orderBy) : '') +
            (limit ? (' LIMIT ' + limit) : ''),
            params, {prepare: true}, function (error, results) {
                if(error) {
                    logger.log('error', TAG + 'selectRows: ' + JSON.stringify(error), {table_name: tableName, where: whereClause, params: params});
                    console.log(error);
                }

                cassandraUtil.responseArray(error, results, function (error, infos) {
                    callback && callback(error, infos);
                    if(error) {
                        reject(error);
                    } else {
                        resolve(infos);
                    }
                });
            });
    })
}

Cassandra.prototype.eachPage = function (model, where, options, columns, orderBy, eachCb, endCb) {
    var whereClauseInfo = this.buildWhere(model, where);
    var whereClause = whereClauseInfo.where;
    var params = whereClauseInfo.params;

    var selectColumn = cassandraUtil.genColumn(columns) || '*';
    var tableName = this.getTableName(model);

    this.cassandraClient.eachPage('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : '') +
        (orderBy ? (' ORDER BY ' + orderBy) : ''), params, options, eachCb, endCb);
}

Cassandra.prototype.eachPageWhere = function (model, whereClause, params, options, columns, orderBy, eachCb, endCb) {
    var selectColumn = cassandraUtil.genColumn(columns) || '*';
    var tableName = this.getTableName(model);

    this.cassandraClient.eachPage('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : '') +
        (orderBy ? (' ORDER BY ' + orderBy) : ''), params, options, eachCb, endCb);
}

/**
 * update or create row
 * @param model
 * @param data
 * @param checkExists
 * @param {error} callback
 */
Cassandra.prototype.update = function (model, data, checkExists, callback) {
    if(typeof checkExists == 'function') {
        callback = checkExists;
        checkExists = false;
    }

    var self = this;

    var updateQueryInfo = this.buildUpdate(model, data, checkExists);
    if(updateQueryInfo.ec) {
        return callback(updateQueryInfo);
    }
    var query = updateQueryInfo.query;
    var params = updateQueryInfo.params;

    return new Promise(function (resolve, reject) {
        self.cassandraClient.execute(query, params, {prepare: true}, function (error, results) {
                if(error) {
                    logger.log('error', TAG + 'updateRow: ' + JSON.stringify(error), {query: query, params: params});
                    logger.log(error);
                }

                callback && callback(error, results);
                if(error) {
                    reject(error);
                } else {
                    resolve(results);
                }
            });
    })
}

/**
 *
 * @param model
 * @param where
 * @param columns
 * @param callback
 * @return {*}
 */
Cassandra.prototype.delete = function (model, where, columns, callback) {
    var self = this;
    var keys = where ? Object.keys(where) : [];

    if(keys.length == 0) {
        var error = {};
        error.ec = 422;
        error.message = 'where must be defined!';
        return callback(error);
    }

    var delQueryInfo = this.buildDelete(model, where, columns);
    var query = delQueryInfo.query;
    var params = delQueryInfo.params;

    return new Promise(function (resolve, reject) {
        self.cassandraClient.execute(query, params, {prepare: true}, function (error, results) {
                if(error) {
                    logger.log('error', TAG + 'deleteRows: ' + JSON.stringify(error), {query: query, params: params});
                    logger.log(error);
                }

                callback && callback(error, results);
                if(error) {
                    reject(error);
                } else {
                    resolve(results);
                }
            });
    })
}

Cassandra.prototype.buildWhere = function (model, where) {
    where = where || {}
    var expressions = [];
    var params = [];
    for(var key in where) {
        if(typeof where[key] == 'object') {
            var value = where[key];
            for(var operator in value) {
                switch(operator) {
                    case '$lt':
                        expressions.push(key + ' < ?')
                        params.push(value[operator])
                        break;
                    case '$lte':
                        expressions.push(key + ' <= ?')
                        params.push(value[operator])
                        break;
                    case '$gt':
                        expressions.push(key + ' > ?')
                        params.push(value[operator])
                        break;
                    case '$gte':
                        expressions.push(key + ' >= ?')
                        params.push(value[operator])
                        break;
                    case '$eq':
                        expressions.push(key + ' = ?')
                        params.push(value[operator])
                        break;
                    default:
                        expressions.push(key + '.' + operator + ' = ?')
                        params.push(value[operator])
                        break;
                }
            }
        } else {
            expressions.push(key + ' = ?')
            params.push(where[key])
        }
    }

    if(expressions.length > 0) {
        var whereClause = expressions.join(' AND ');
        return {
            where: whereClause,
            params: params
        }
    } else {
        return {
            where: null,
            params: []
        }
    }
}

Cassandra.prototype.buildCreate = function (model, data) {
    var modelDefine = this.getModelDefinition(model);
    var props = modelDefine.model.definition.rawProperties;
    var fields = cassandraUtil.generateFields(props, data);
    var values = cassandraUtil.generateValues(props, data);
    var params = cassandraUtil.generateParams(props, data);

    var tableName = this.getTableName(model);

    var query = 'INSERT INTO ' + tableName + ' ' + fields + ' VALUES ' + values;

    return {
        query: query,
        params: params
    }
}

Cassandra.prototype.buildUpdate = function (model, data, checkExists) {
    var modelDefine = this.getModelDefinition(model);
    var partitionKeys = modelDefine.model.definition.settings.partitionKeys || [];
    var clustering = modelDefine.model.definition.settings.clustering || [];
    var props = modelDefine.model.definition.rawProperties;

    var keys = partitionKeys.concat(clustering);
    var keyValues = [];
    for(var i = 0; i < keys.length; i ++) {
        if(!data[keys[i]]) {
            var error = {};
            error.ec = 422;
            error.message = 'partition or clustering key ' + keys[i] + ' must be defined!';
            return error
        }
        keyValues.push(data[keys[i]]);
        delete data[keys[i]];
    }

    var assignInfo = this.buildUpdateAssignment(props, data);
    var assignments = assignInfo.assign
    var params = assignInfo.params

    params = params.concat(keyValues);
    var whereClause = cassandraUtil.genWhereClause(keys);
    var tableName = this.getTableName(model);

    var query = 'UPDATE ' + tableName + ' SET ' + assignments + ' WHERE ' + whereClause + (checkExists ? ' IF EXISTS' : '')

    return {
        query: query,
        params: params
    }
}

Cassandra.prototype.buildDelete = function (model, where, columns) {
    var whereClauseInfo = this.buildWhere(model, where);
    var whereClause = whereClauseInfo.where;
    var params = whereClauseInfo.params;

    var selectColumn = cassandraUtil.genColumn(columns) || '';
    var tableName = this.getTableName(model);

    return {
        query: 'DELETE ' + selectColumn + ' FROM ' + tableName + ' WHERE ' + whereClause,
        params: params
    }
}

Cassandra.prototype.buildUpdateAssignment = function (props, data) {
    var assignments = '';
    var params = [];

    for(var field in props) {
        if(typeof data[field] == 'object') {
            var value = data[field];
            if(value['$add']) {
                assignments += field + ' = ' + field + ' + ?,';
                params.push(value['$add'])
            } else if(value['$sub']) {
                assignments += field + ' = ' + field + ' - ?,';
                params.push(value['$sub'])
            } else {
                assignments += field + ' = ?,';
                params.push(data[field])
            }
        } else if(data[field] != undefined) {
            assignments += field + ' = ?,';
            params.push(data[field])
        }
    }

    return {
        assign: assignments.substr(0, assignments.length - 1),
        params: params
    }
}

Cassandra.prototype.getTableName = function (model) {
    var modelDefine = this.getModelDefinition(model);
    return modelDefine.settings.tableName || model;
}

Cassandra.prototype.getCassandraType = function (type) {
    var cassandraTypeMapping = {
        'string': 'text'
    }
    return cassandraTypeMapping[type] || type;
}

function escapeIdentifier(str) {
    var escaped = '"';
    for (var i = 0; i < str.length; i++) {
        var c = str[i];
        if (c === '"') {
            escaped += c + c;
        } else {
            escaped += c;
        }
    }
    escaped += '"';
    return escaped;
}

function escapeLiteral(str) {
    var hasBackslash = false;
    var escaped = '\'';
    for (var i = 0; i < str.length; i++) {
        var c = str[i];
        if (c === '\'') {
            escaped += c + c;
        } else if (c === '\\') {
            escaped += c + c;
            hasBackslash = true;
        } else {
            escaped += c;
        }
    }
    escaped += '\'';
    if (hasBackslash === true) {
        escaped = ' E' + escaped;
    }
    return escaped;
}

/*!
 * Escape the name for Cassandra DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
Cassandra.prototype.escapeName = function (name) {
    if (!name) {
        return name;
    }
    return escapeIdentifier(name);
};

Cassandra.prototype.escapeValue = function (value) {
    if (typeof value === 'string') {
        return escapeLiteral(value);
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
        return value;
    }
    return value;
};

/**
 * Disconnect from Cassandra
 * @param {Function} [cb] The callback function
 */
Cassandra.prototype.disconnect = function disconnect(cb) {
    if (this.cassandra) {
        if (this.settings.debug) {
            debug('Disconnecting from ' + this.settings.hostname);
        }
        var cassandra = this.cassandra;
        this.cassandra = null;
        cassandra.end();  // This is sync
    }

    if (cb) {
        process.nextTick(cb);
    }
};

Cassandra.prototype.ping = function (cb) {
    this.execute('SELECT count(*) FROM system.schema_keyspaces', [], cb);
};

function BatchExecutor(model, cassandraConnector) {
    this.model = model;
    this.batchQuery = [];
    this.connector = cassandraConnector;
}

BatchExecutor.prototype.create = function (data) {
    var createInfo = this.connector.buildCreate(this.model, data);
    this.batchQuery.push({
        query: createInfo.query,
        params: createInfo.params
    })

    return this;
}

BatchExecutor.prototype.update = function (data, checkExists) {
    var updateQueryInfo = this.connector.buildUpdate(this.model, data, checkExists);
    this.batchQuery.push({
        query: updateQueryInfo.query,
        params: updateQueryInfo.params
    })

    return this;
}

BatchExecutor.prototype.delete = function (where, columns) {
    var delQueryInfo = this.connector.buildDelete(this.model, where, columns);
    this.batchQuery.push({
        query: delQueryInfo.query,
        params: delQueryInfo.params
    })

    return this;
}

BatchExecutor.prototype.execute = function (callback) {
    var self = this;
    return new Promise(function (resolve, reject) {
        if(self.batchQuery.length > 0) {
            self.connector.cassandraClient.batch(self.batchQuery, {prepare: true}, function (error, results) {
                if(error) {
                    logger.log('error', TAG + 'deleteRows: ' + JSON.stringify(error));
                    logger.log(error);
                }

                callback && callback(error, results);
                if(error) {
                    reject(error);
                } else {
                    resolve(results);
                }
            });
        } else {
            callback && callback(null);
            resolve();
        }
    })
}

require('./migration')(Cassandra);

