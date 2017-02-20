/**
 * Created by DICH on 2/20/2017.
 */
var cassandra = require('cassandra-driver');
var MasterProcessActions = require('../constants/MasterProcessActions');
var IPCClient = require('../lib/IPCClient');
var SystemConstants = require('../constants/SystemConstants');
var uid = require('uid-safe').sync;

/**
 * after send data between process then TimeUuid type become string, so can not invoke getDate() function.
 */
String.prototype.getDate = function () {
    try {
        return cassandra.types.TimeUuid.fromString(this.toString()).getDate();
    } catch (e) {
        throw new Error('getDate is not a function.');
    }
};

/**
 * after send data between process then Date type become string, so can not invoke getTime() function.
 */
String.prototype.getTime = function () {
    try {
        return Date.parse(this.toString());
    } catch (e) {
        throw new Error('getTime is not a function.');
    }
};

var cassClients = [];
var cassServerCurrentCounts = [];

var executeQueue = [];
var batchQueue = [];

module.exports = function (options) {
    options.numOfServer = options.numOfServer || 1;
    var count = 0;
    var toBarrier = function () {
        count += 1;
        if(count == options.numOfServer) {
            for(var i = 0; i < executeQueue.length; i ++) {
                execute.apply(null, executeQueue[i]);
            }

            for(var i = 0; i < batchQueue.length; i ++) {
                batch.apply(null, batchQueue[i]);
            }
        }
    };

    var startCassandraServer = function () {
        var serviceId = 'cassandra-' + uid(5);
        options.id = serviceId;
        var childPs = cp.fork('./cassandra_server', {env: options}, {
            silent: true
        });

        childPs.on('message', function (data) {
            switch (data.ac) {
                case MasterProcessActions.READY:
                    cassClients[i] = new IPCClient(serviceId, function () {
                        toBarrier();
                    });
                    cassServerCurrentCounts.push(0);
                    break;
            }
        });
    }

    for (var i = 0; i < options.numOfServer; i++) {
        startCassandraServer();
    }

    return {
        execute: execute,
        batch: batch
    };
}

var getServerConcurrent = function (i) {
    cassClients[i].callMethod('getConcurrent', [], function (error, data) {
        if(data) {
            cassServerCurrentCounts[i] = data.result;
        }
    });
};

setInterval(function () {
    for(var i = 0; i < SystemConstants.NUM_CASS_SERVER; i ++) {
        getServerConcurrent(i);
    }
}, 500);

var execute = function (query, param, options, callback) {
    var min = Math.min.apply(Math, cassServerCurrentCounts);
    var index = cassServerCurrentCounts.indexOf(min);
    if(index != -1) {
        var cassClient = cassClients[index];
        cassServerCurrentCounts[index] += 1;
    } else {
        cassClient = cassClients[0];
    }

    if(cassClient && cassClient.isConnected()) {
        cassClient.callMethod('execute', [query, param, options], function (error, data) {
            if (data) {
                callback(error, data.result);
            } else {
                callback(error);
            }
        });
    } else {
        executeQueue.push([query, param, options, callback]);
    }
};

var batch = function (queries, options, callback) {
    var min = Math.min.apply(Math, cassServerCurrentCounts);
    var index = cassServerCurrentCounts.indexOf(min);
    if(index != -1) {
        var cassClient = cassClients[index];
        cassServerCurrentCounts[index] += 1;
    } else {
        cassClient = cassClients[0];
    }

    if(cassClient && cassClient.isConnected()) {
        cassClient.callMethod('batch', [queries, options], function (error, data) {
            if (data) {
                callback(error, data.result);
            } else {
                callback(error);
            }
        });
    } else {
        batchQueue.push([queries, options, callback]);
    }
};