/**
 * Created by Administrator on 24/07/2015.
 */
var cassandra = require('cassandra-driver');
var cassandraClient = {};
var logger = console;
var MasterProcessActions = require('../constants/MasterProcessActions');

var TAG = 'cassandra_util: ';

var _this = this;

var queryTimes = [];
var QUERY_TIME_FRAME = 10;
var queryTimeIndex = 0;

////check response time, if slow --> reconnect db
setInterval(function () {
    var total = 0;
    for(var i = 0; i < QUERY_TIME_FRAME; i ++) {
        total += queryTimes[i] ? queryTimes[i] : 0;
    }

    var time = total / QUERY_TIME_FRAME;

    logger.log('info', TAG + 'response time: ' + time);

    if(process.send) {
        process.send({ac: MasterProcessActions.UPDATE_RESPONSE_TIME, time: time});
    }
}, 60 * 1000);

process.on('message', function (data) {
    if(data.ac == MasterProcessActions.FETCH_RESPONSE_TIME) {
        var total = 0;
        for(var i = 0; i < QUERY_TIME_FRAME; i ++) {
            total += queryTimes[i] ? queryTimes[i] : 0;
        }

        var time = total / QUERY_TIME_FRAME;

        process.send({ac: MasterProcessActions.FETCH_RESPONSE_TIME, time: time});
    }
});

var generateFields = function (props, instance) {
    var result = '(';

    for(var field in props) {
        if(instance[field] != undefined) {
            result += field + ',';
        }
    }

    return result.substr(0, result.length - 1) + ')';
};

var generateValues = function (props, instance) {
    var result = '(';

    for(var field in props) {
        if(instance[field] != undefined) {
            result += '?,';
        }
    }

    return result.substr(0, result.length - 1) + ')';
};

var generateParams = function (props, instance) {
    var result = [];
    for(var field in props) {
        if(instance[field] != undefined) {
            result.push(instance[field]);
        }
    }

    return result;
};

var generateAssignments = function (props, instance) {
    var assignments = '';
    for(var field in props) {
        if(instance[field] != undefined) {
            assignments += field + ' = ?,';
        }
    }

    return assignments.substr(0, assignments.length - 1);
};

var generateAssignmentCounter = function (props, instance) {
    var assignments = '';
    for(var field in props) {
        if(instance[field] != undefined) {
            assignments += field + ' = ' + field + ' + ?,';
        }
    }

    return assignments.substr(0, assignments.length - 1);
};

var generateDecreaseCounter = function (props, instance) {
    var assignments = '';
    for(var field in props) {
        if(instance[field] != undefined) {
            assignments += field + ' = ' + field + ' - ?,';
        }
    }

    return assignments.substr(0, assignments.length - 1);
};

var genWhereClause = function (keys) {
    if(keys && keys.length > 0) {
        var result = keys[0] + ' = ?';
        for (var i = 1; i < keys.length; i++) {
            result += ' AND ' + keys[i] + ' = ?';
        }
        return result;
    } else {
        return '';
    }
};

var genColumn = function (columns) {
    if(!columns) return null;
    var result = columns[0];
    for(var i = 1; i < columns.length; i ++) {
        result += ', ' + columns[i]
    }
    return result;
};

var cloneInstance = function (props, instance) {
    var newInstance = {};
    for(var field in props) {
        if(instance[field] != undefined) {
            newInstance[field] = instance[field];
        }
    }
    return newInstance;
};

//======================================================================================================================

exports.generateFields = generateFields;
exports.generateValues = generateValues;
exports.generateParams = generateParams;
exports.generateAssignments = generateAssignments;
exports.generateAssignmentCounter = generateAssignmentCounter;
exports.generateDecreaseCounter = generateDecreaseCounter;
exports.genWhereClause = genWhereClause;
exports.genColumn = genColumn;
exports.cloneInstance = cloneInstance;

exports.generateCounterUpdate = function (props, instance) {
    var assignments = '';
    for(var field in props) {
        if(instance[field] != undefined) {
            assignments += field + ' = ' + field + ' + ?,';
        }
    }

    return assignments.substr(0, assignments.length - 1);
};

exports.responseArray = function (error, results, callback, hook) {
    if(callback) {
        if (error) {
            callback(error, null);
        } else {
            if (results.rows) {
                if (hook) {
                    var rows = results.rows.map(hook);
                    callback(null, rows);
                } else {
                    callback(null, results.rows);
                }
            } else {
                callback(null, []);
            }
        }
    }
};

exports.responseOneRow = function (error, results, callback, hook) {
    if(callback) {
        if (error) {
            callback(error, null);
        } else {
            if (results.rows) {
                if (hook) {
                    var rows = results.rows.map(hook);
                    callback(null, rows[0]);
                } else {
                    callback(null, results.rows[0]);
                }
            } else {
                callback(null, null);
            }
        }
    }
};

exports.getNewTimeUuid = function () {
    return cassandra.types.TimeUuid.now().toString();
};

exports.getTimeUuidFromTimestamp = function (timestamp) {
    return cassandra.types.TimeUuid.fromDate(new Date(timestamp));
};

exports.getTimestampFromTimeUuid = function (timeuuid) {
    return cassandra.types.TimeUuid.fromString(timeuuid).getDate().getTime();
};

exports.isTimeUuid = function (id) {
    try {
        return cassandra.types.TimeUuid.fromString(id);
    } catch(e) {
        return false;
    }
};

/**
 *
 * @param tableName
 * @param model
 * @param instance
 * @param {error, results} callback
 */
exports.insertRow = function (tableName, model, instance, callback) {
    var time1 = new Date().getTime();
    if(!instance.time_save) {
        instance.time_save = _this.getNewTimeUuid();
    }

    var fields = generateFields(model, instance);
    var values = generateValues(model, instance);
    var params = generateParams(model, instance);

    cassandraClient.execute('INSERT INTO ' + tableName + ' ' + fields + ' VALUES ' + values,
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'insertRow: ' + JSON.stringify(error), {table_name: tableName, instance: instance});
                console.log(error);
            }

            var time2 = new Date().getTime();

            queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
            queryTimes[queryTimeIndex] = time2 - time1;

            //queryTimeLogger.log('info', TAG + 'insertRow: ' + (time2 - time1), {table_name: tableName, time: (time2 - time1)});

            callback(error, results);

            //if(!error && TABLES_NOT_BACKUP.indexOf(tableName) == -1) {
            //    backupModel.addInstance(tableName, instance, function (error) {
            //        if(error) {
            //            logger.log('error', TAG + 'insertRow --> addInstance: ' + error, {table_name: tableName});
            //        }
            //    });
            //}
        });
};

/**
 *
 * @param tableName
 * @param model
 * @param instance
 * @return {query, params}
 */
exports.getInsertQuery = function (tableName, model, instance) {
    if(!instance.time_save) {
        instance.time_save = _this.getNewTimeUuid();
    }

    var fields = _this.generateFields(model, instance);
    var values = _this.generateValues(model, instance);
    var params = _this.generateParams(model, instance);

    var query = 'INSERT INTO ' + tableName + ' ' + fields + ' VALUES ' + values;
    return {
        query: query,
        params: params
    }
};

/**
 *
 * @param tableName
 * @param model
 * @param instance
 * @param ttl
 * @param {error} callback
 */
exports.insertRowWithTTL = function (tableName, model, instance, ttl, callback) {
    var time1 = new Date().getTime();
    var fields = generateFields(model, instance);
    var values = generateValues(model, instance);
    var params = generateParams(model, instance);

    cassandraClient.execute('INSERT INTO ' + tableName + ' ' + fields + ' VALUES ' + values + ' USING TTL ' + ttl,
        params, {prepare: true}, function (error, results) {

            var time2 = new Date().getTime();

            queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
            queryTimes[queryTimeIndex] = time2 - time1;

            //queryTimeLogger.log('info', TAG + 'insertRowWithTTL: ' + (time2 - time1), {table_name: tableName, time: (time2 - time1)});

            callback(error, results);

            //if(!error && TABLES_NOT_BACKUP.indexOf(tableName) == -1) {
            //    backupModel.addInstance(tableName, instance, function (error) {
            //        if(error) {
            //            logger.log('error', TAG + 'insertRow --> addInstance: ' + error, {table_name: tableName});
            //        }
            //    });
            //}
        });
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @param model
 * @param updateInfo
 * @param checkExists
 * @param {error, results} callback
 */
exports.updateRow = function (tableName, keys, keyValues, model, updateInfo, checkExists, callback) {
    if(typeof checkExists == 'function') {
        callback = checkExists;
        checkExists = false;
    }

    var time1 = new Date().getTime();
    var assignments = generateAssignments(model, updateInfo);
    var params = generateParams(model, updateInfo);
    params = params.concat(keyValues);
    var whereClause = genWhereClause(keys);

    cassandraClient.execute('UPDATE ' + tableName + ' SET ' + assignments + ' WHERE ' + whereClause + (checkExists ? ' IF EXISTS' : ''),
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'updateRow: ' + JSON.stringify(error), {table_name: tableName, keys: keys, values: keyValues, update_info: updateInfo});
                logger.log(error);
            }

            var time2 = new Date().getTime();
            queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
            queryTimes[queryTimeIndex] = time2 - time1;

            //queryTimeLogger.log('info', TAG + 'updateRow: ' + (time2 - time1), {table_name: tableName, where: whereClause, time: (time2 - time1)});

            callback(error, results);
        });
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @param model
 * @param updateInfo
 * @return {query, params}
 * @param checkExists
 */
exports.getUpdateQuery = function (tableName, keys, keyValues, model, updateInfo, checkExists) {
    var assignments = _this.generateAssignments(model, updateInfo);
    var params = _this.generateParams(model, updateInfo);
    params = params.concat(keyValues);
    var whereClause = _this.genWhereClause(keys);

    var query = 'UPDATE ' + tableName + ' SET ' + assignments + ' WHERE ' + whereClause + (checkExists ? ' IF EXISTS' : '');
    return {
        query: query,
        params: params
    }
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @param model
 * @param updateInfo
 * @param {error, results} callback
 */
exports.updateCounterRow = function (tableName, keys, keyValues, model, updateInfo, callback) {
    var time1 = new Date().getTime();
    var assignments = generateAssignmentCounter(model, updateInfo);
    var params = generateParams(model, updateInfo);
    params = params.concat(keyValues);
    var whereClause = genWhereClause(keys);

    cassandraClient.execute('UPDATE ' + tableName + ' SET ' + assignments + ' WHERE ' + whereClause,
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'updateCounterRow: ' + JSON.stringify(error), {table_name: tableName, keys: keys, values: keyValues, update_info: updateInfo});
                logger.log(error);
            }

            var time2 = new Date().getTime();
            queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
            queryTimes[queryTimeIndex] = time2 - time1;

            //queryTimeLogger.log('info', TAG + 'updateCounterRow: ' + (time2 - time1), {table_name: tableName, where: whereClause, time: (time2 - time1)});

            callback(error, results);
        });
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @param model
 * @param updateInfo
 * @return {query, params}
 */
exports.getUpdateCounterRowQuery = function (tableName, keys, keyValues, model, updateInfo) {
    var assignments = _this.generateAssignmentCounter(model, updateInfo);
    var params = _this.generateParams(model, updateInfo);
    params = params.concat(keyValues);
    var whereClause = _this.genWhereClause(keys);

    var query = 'UPDATE ' + tableName + ' SET ' + assignments + ' WHERE ' + whereClause;
    return {
        query: query,
        params: params
    }
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @param model
 * @param updateInfo
 * @return {{query: string, params}}
 */
exports.getDecreaseCounterRowQuery = function (tableName, keys, keyValues, model, updateInfo) {
    var assignments = _this.generateDecreaseCounter(model, updateInfo);
    var params = _this.generateParams(model, updateInfo);
    params = params.concat(keyValues);
    var whereClause = _this.genWhereClause(keys);

    var query = 'UPDATE ' + tableName + ' SET ' + assignments + ' WHERE ' + whereClause;
    return {
        query: query,
        params: params
    }
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @param setStatement
 * @param params
 * @param {error, results} callback
 */
exports.updateBySet = function (tableName, keys, keyValues, setStatement, params, callback) {
    var time1 = new Date().getTime();
    params = params ? params : [];
    params = params.concat(keyValues);
    var whereClause = genWhereClause(keys);

    cassandraClient.execute('UPDATE ' + tableName + ' SET ' + setStatement + ' WHERE ' + whereClause,
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'updateBySet: ' + JSON.stringify(error), {table_name: tableName, keys: keys, values: keyValues, params: params});
                logger.log(error);
            }

            var time2 = new Date().getTime();
            queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
            queryTimes[queryTimeIndex] = time2 - time1;

            //queryTimeLogger.log('info', TAG + 'updateBySet: ' + (time2 - time1), {table_name: tableName, where: whereClause, time: (time2 - time1)});

            callback(error, results);
        });
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @param {error, results} callback
 */
exports.deleteRows = function (tableName, keys, keyValues, callback) {
    var time1 = new Date().getTime();
    var whereClause = genWhereClause(keys);

    cassandraClient.execute('DELETE FROM ' + tableName + ' WHERE ' + whereClause,
        keyValues, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'deleteRows: ' + JSON.stringify(error), {table_name: tableName, keys: keys, values: keyValues});
                logger.log(error);
            }

            var time2 = new Date().getTime();
            queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
            queryTimes[queryTimeIndex] = time2 - time1;

            //queryTimeLogger.log('info', TAG + 'deleteRows: ' + (time2 - time1), {table_name: tableName, where: whereClause, time: (time2 - time1)});

            callback(error, results);
        });
};

/**
 *
 * @param tableName
 * @param keys
 * @param keyValues
 * @returns {{query: string, params: *}}
 */
exports.getDeleteQuery = function (tableName, keys, keyValues) {
    var whereClause = _this.genWhereClause(keys);
    var query = 'DELETE FROM ' + tableName + ' WHERE ' + whereClause;
    return {
        query: query,
        params: keyValues
    };
};

/**
 *
 * @param tableName
 * @param whereClause
 * @param params
 * @param columns
 * @param orderBy
 * @param limit
 * @param isCache
 * @param {error, results} callback
 */
exports.selectRows = function (tableName, whereClause, params, columns, orderBy, limit, isCache, callback) {
    var time1 = new Date().getTime();
    if(columns && columns.length > 0) {
        var selectColumn = genColumn(columns);
    } else {
        selectColumn = '*'
    }

    cassandraClient.execute('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : '') +
            (orderBy ? (' ORDER BY ' + orderBy) : '') +
            (limit ? (' LIMIT ' + limit) : ''),
        params, {prepare: true}, function (error, results) {
            if(error) {
                logger.log('error', TAG + 'selectRows: ' + JSON.stringify(error), {table_name: tableName, where: whereClause, params: params});
                console.log(error);
            }

            var time2 = new Date().getTime();
            queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
            queryTimes[queryTimeIndex] = time2 - time1;

            //queryTimeLogger.log('info', TAG + 'selectRows: ' + (time2 - time1), {table_name: tableName, where: whereClause, time: (time2 - time1)});

            callback(error, results);
        });
};

/**
 *
 * @param batchQueries
 * @param {error} callback
 */
exports.batch = function (batchQueries, callback) {
    //var queries = [];
    //var backupBatch = [];
    //for(var i = 0; i < batchQueries.length; i ++) {
    //    queries.push({
    //        query: batchQueries[i].query,
    //        params: batchQueries[i].params
    //    });
    //
    //    if(batchQueries[i].operator == 'insert' && TABLES_NOT_BACKUP.indexOf(batchQueries[i].table_name) == -1) {
    //        var timeSave;
    //        if(!batchQueries[i].instance.time_save) {
    //            batchQueries[i].instance.time_save = _this.getNewTimeUuid();
    //        }
    //        timeSave = batchQueries[i].instance.time_save;
    //
    //        var backupInfo = {};
    //        backupInfo[BackupFields.TABLE_NAME] = batchQueries[i].table_name;
    //        backupInfo[BackupFields.TIME_SAVE] = timeSave;
    //        backupInfo[BackupFields.DATA] = JSON.stringify(batchQueries[i].instance);
    //        backupInfo[BackupFields.STATE] = ObjectState.ACTIVE;
    //
    //        var fields = generateFields(BackupFields, backupInfo);
    //        var values = generateValues(BackupFields, backupInfo);
    //        var params = generateParams(BackupFields, backupInfo);
    //
    //        backupBatch.push({
    //            query: 'INSERT INTO ' + BACKUP_TABLE_NAME + ' ' + fields + ' VALUES ' + values,
    //            params: params
    //        });
    //    }
    //}

    var time1 = new Date().getTime();
    cassandraClient.batch(batchQueries, {prepare: true}, function (error) {
        callback(error);

        var time2 = new Date().getTime();
        queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
        queryTimes[queryTimeIndex] = time2 - time1;

        //queryTimeLogger.log('info', TAG + 'batch: ' + (time2 - time1), {batch: batchQueries, time: (time2 - time1)});

        //if(!error && backupBatch.length > 0) {
        //    cassandraClient.batch(backupBatch, {prepare: true}, function (error) {
        //        if(error) {
        //            logger.log('error', TAG + 'batch --> backup batch: ' + error);
        //        }
        //    });
        //}
    });
};

/**
 *
 * @param tableName
 * @param whereClause
 * @param params
 * @param columns
 * @param options
 * @param getRowCb
 * @param endCb
 */
exports.selectEachRow = function (tableName, whereClause, params, columns, options, getRowCb, endCb) {
    if(columns && columns.length > 0) {
        var selectColumn = genColumn(columns);
    } else {
        selectColumn = '*'
    }

    cassandraClient.eachRow('SELECT ' + selectColumn + ' FROM ' + tableName + (whereClause ? (' WHERE ' + whereClause) : ''), params,
        options, getRowCb, endCb);
};

//======================================================================================================================