/**
 * Created by DICH on 2/20/2017.
 */
var cassandra = require('cassandra-driver');
var MasterProcessActions = require('../constants/MasterProcessActions');
var IPCClient = require('../lib/IPCClient');
const cp = require('child_process');
var path = require('path');

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
var eachPageQueue = [];

module.exports = function (options) {
    options.numOfServer = options.numOfServer || 1;
    var count = 0;
    var toBarrier = function () {
        count += 1;
        if(count == options.numOfServer) {
            for(var i = 0; i < executeQueue.length; i ++) {
                var query = executeQueue.shift();
                execute.apply(null, query);
            }

            for(var i = 0; i < batchQueue.length; i ++) {
                var batchQuery = batchQueue.shift();
                batch.apply(null, batchQuery);
            }

            for(var i = 0; i < eachPageQueue.length; i ++) {
                var eachPageQuery = eachPageQueue.shift();
                eachPage.apply(null, eachPageQuery);
            }
        }
    };

    var startCassandraServer = function (index) {
        var serviceId = 'cassandra-' + index;
        options.id = serviceId;
        var childPs = cp.fork(path.join(__dirname, './cassandra_server.js'), {env: options}, {
            silent: true
        });

        childPs.on('message', function (data) {

            switch (data.ac) {
                case MasterProcessActions.READY:
                    var cassClient = new IPCClient(serviceId, function () {
                        toBarrier();
                    });
                    cassClient.once('disconnect', function () {
                        startCassandraServer(index);
                    })
                    cassClient.on('error', function (error) {
                        //TODO
                    })
                    cassClients[index] = cassClient;
                    cassServerCurrentCounts[index] = 0;
                    break;
            }
        });
    }

    var connectCassandraServer = function (index) {
        var serviceId = 'cassandra-' + index;

        var cassClient = new IPCClient(serviceId, function () {
            toBarrier();
        });

        cassClient.once('disconnect', function () {
            startCassandraServer(index);
        })
        cassClient.on('error', function (error) {
            //TODO
        })
        cassClients[index] = cassClient;
        cassServerCurrentCounts[index] = 0;
    }

    for (var i = 0; i < options.numOfServer; i++) {
        connectCassandraServer(i);
    }

    return {
        execute: execute,
        batch: batch,
        eachPage: eachPage
    };
}

var getServerConcurrent = function (i) {
    cassClients[i] && cassClients[i].callMethod('getConcurrent', [], function (error, data) {
        if(data) {
            cassServerCurrentCounts[i] = data.result;
        }
    });
};

setInterval(function () {
    for(var i = 0; i < cassClients.length; i ++) {
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

var eachPage = function (query, params, options, eachCb, endCb) {
    var min = Math.min.apply(Math, cassServerCurrentCounts);
    var index = cassServerCurrentCounts.indexOf(min);
    if(index != -1) {
        var cassClient = cassClients[index];
        cassServerCurrentCounts[index] += 1;
    } else {
        cassClient = cassClients[0];
    }

    var page = 0;
    var getPage = function (options, cb) {
        cassClient.callMethod('getPage', [query, params, options], function (error, data) {
            if (data) {
                cb(error, data.result);
            } else {
                cb(error);
            }
        });
    }

    if(cassClient && cassClient.isConnected()) {
        var responseCb = function (error, result) {
            if(error) {
                endCb(error);
            } else {
                var hasNext = result.hasNext;
                var pageState = result.pageState;
                eachCb(page, result.rows);
                page += 1;

                if(options.autoPage && hasNext) {
                    options.pageState = pageState;
                    getPage(options, responseCb)
                } else {
                    endCb(null, {
                        nextPage: hasNext ? function () {
                            options.pageState = pageState;
                            getPage(options, responseCb);
                        } : null
                    })
                }
            }
        }

        getPage(options, responseCb);
    } else {
        eachPageQueue.push([query, params, options, eachCb, endCb]);
    }
}

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