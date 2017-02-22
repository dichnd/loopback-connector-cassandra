/*!
 * Cassandra connector for LoopBack
 */
var cassandra = require('cassandra-driver');
var Connector = require('loopback-connector').Connector;
var util = require('util');
var debug = require('debug')('loopback:connector:cassandra');
var cassandraUtil = require('../lib/cassandra_util');
var logger = console;

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
            connector.create(this.modelName, data, callback);
        };

        DataAccessObject.findOne = function (keyValues, columns, callback) {
            connector.findOne(this.modelName, keyValues, columns, callback);
        };

        DataAccessObject.find = function (keyValues, columns, orderBy, limit, callback) {
            connector.find(this.modelName, keyValues, columns, orderBy, limit, callback);
        };

        DataAccessObject.patch = function (data, callback) {
            connector.patch(this.modelName, data, false, callback);
        };

        DataAccessObject.update = function (data, callback) {
            connector.update(this.modelName, data, true, callback);
        };

        DataAccessObject.delete = function (keyValues, columns, callback) {
            connector.delete(this.modelName, keyValues, columns, callback);
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
    console.log(settings);
    // this.name = 'cassandra';
    // this._models = {};
    // this.settings = settings;
    this.constructor.super_.call(this, 'cassandra', settings);
    this.clientConfig = settings;
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
    console.log(this.settings);
    var cassandraClient = this.cassandraClient = require('../lib/cassandra_client')(self.settings)
    process.nextTick(function () {
        callback && callback(null, cassandraClient);
    });
};

Cassandra.prototype.getTypes = function() {
    return ['db', 'nosql', 'cassandra'];
};

Cassandra.prototype.create = function (model, data, callback) {
    var modelDefine = this.getModelDefinition(model);
    var props = modelDefine.model.definition.rawProperties;
    var fields = cassandraUtil.generateFields(props, data);
    var values = cassandraUtil.generateValues(props, data);
    var params = cassandraUtil.generateParams(props, data);

    var tableName = this.getTableName(model);

    this.cassandraClient.execute('INSERT INTO ' + tableName + ' ' + fields + ' VALUES ' + values,
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'insertRow: ' + JSON.stringify(error), {table_name: tableName, instance: data});
                logger.log(error);
            }

            callback(error, results);
        });
}

Cassandra.prototype.findOne = function (model, keyValues, columns, callback) {
    console.log(model);
    if(typeof columns == 'function') {
        callback = columns;
        columns = null;
    }

    var modelDefine = this.getModelDefinition(model);
    var partitionKeys = modelDefine.model.definition.settings.partitionKeys;
    var clustering = modelDefine.model.definition.settings.clustering;

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

    this.cassandraClient.execute('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : ''),
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'selectRows: ' + JSON.stringify(error), {table_name: tableName, where: whereClause, params: params});
                console.log(error);
            }

            cassandraUtil.responseOneRow(error, results, callback);
        });
}

Cassandra.prototype.find = function (model, keyValues, columns, orderBy, limit, callback) {
    if(typeof columns == 'function') {
        callback = columns;
        columns = null;
    }

    var keys = keyValues ? Object.keys(keyValues) : [];

    var whereClause = cassandraUtil.genWhereClause(keys);
    var params = [];
    for(var i = 0; i < keys.length; i ++) {
        params.push(keyValues[keys[i]]);
    }
    var selectColumn = cassandraUtil.genColumn(columns) || '*';
    var tableName = this.getTableName(model);

    this.cassandraClient.execute('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : '') +
        (orderBy ? (' ORDER BY ' + orderBy) : '') +
        (limit ? (' LIMIT ' + limit) : ''),
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'selectRows: ' + JSON.stringify(error), {table_name: tableName, where: whereClause, params: params});
                console.log(error);
            }

            cassandraUtil.responseArray(error, results, callback);
        });
}

/**
 * update or create row
 * @param model
 * @param data
 * @param checkExists
 * @param {error} callback
 */
Cassandra.prototype.update = function (model, data, checkExists, callback) {
    var modelDefine = this.getModelDefinition(model);
    var partitionKeys = modelDefine.model.definition.settings.partitionKeys;
    var clustering = modelDefine.model.definition.settings.clustering;
    var props = modelDefine.model.definition.rawProperties;

    var keys = partitionKeys.concat(clustering);
    var keyValues = [];
    for(var i = 0; i < keys.length; i ++) {
        if(!data[keys[i]]) {
            var error = {};
            error.ec = 422;
            error.message = 'partition or clustering key ' + keys[i] + ' must be defined!';
            return callback(error);
        }
        keyValues.push(data[keys[i]]);
        delete data[keys[i]];
    }

    var assignments = cassandraUtil.generateAssignments(props, data);
    var params = cassandraUtil.generateParams(props, data);
    params = params.concat(keyValues);
    var whereClause = cassandraUtil.genWhereClause(keys);
    var tableName = this.getTableName(model);

    this.cassandraClient.execute('UPDATE ' + tableName + ' SET ' + assignments + ' WHERE ' + whereClause + (checkExists ? ' IF EXISTS' : ''),
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'updateRow: ' + JSON.stringify(error), {table_name: tableName, keys: keys, values: keyValues, update_info: data});
                logger.log(error);
            }

            callback(error, results);
        });
}

/**
 *
 * @param model
 * @param keyValues
 * @param columns
 * @param callback
 * @return {*}
 */
Cassandra.prototype.delete = function (model, keyValues, columns, callback) {
    var keys = keyValues ? Object.keys(keyValues) : [];

    if(keys.length == 0) {
        var error = {};
        error.ec = 422;
        error.message = 'keyValues must be defined!';
        return callback(error);
    }

    var whereClause = cassandraUtil.genWhereClause(keys);
    var params = [];
    for(var i = 0; i < keys.length; i ++) {
        params.push(keyValues[keys[i]]);
    }
    var selectColumn = cassandraUtil.genColumn(columns) || '';
    var tableName = this.getTableName(model);

    this.cassandraClient.execute('DELETE ' + selectColumn + ' FROM ' + tableName + ' WHERE ' + whereClause,
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'deleteRows: ' + JSON.stringify(error), {table_name: tableName, keys: keys, values: keyValues});
                logger.log(error);
            }

            callback(error, results);
        });
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

require('./migration')(Cassandra);

