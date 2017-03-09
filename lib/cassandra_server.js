/**
 * Created by DICH on 7/6/2016.
 */
var cassandra = require('cassandra-driver');
var MasterProcessActions = require('../constants/MasterProcessActions');
var IPCServer = require('./IPCServer');

var options = {
    host: 'localhost',
    user: 'cassandra',
    password: 'newpassword',
    keyspace: 'test',
    id: 'cassandra-0'
}

if(process.env) {
    options.host = options.host || process.env.host;
    options.user = options.user || process.env.user || process.env.userName;
    options.password = options.password || process.env.password || process.env.pass;
    options.keyspace = options.keyspace || process.env.keyspace;
    options.id = options.id || process.env.id;
}

var reqCountIndex = -1;
var requestTimeMark = [];

var distance = cassandra.types.distance;
var pooling = {
    coreConnectionsPerHost: {}
};
pooling.coreConnectionsPerHost[distance.local] = 2;
pooling.coreConnectionsPerHost[distance.remote] = 1;
pooling.coreConnectionsPerHost[distance.ignored] = 0;

var config = {
    pooling: pooling,
    keyspace: options.keyspace
};
var host = options.host || options.hostname;
if (typeof host === 'string') {
    config.contactPoints = [host];
}
if(options.user) {
    config.authProvider = new cassandra.auth.PlainTextAuthProvider(options.user, options.password)
}

var client = new cassandra.Client(config);

client.connect(function(err, result) {
    console.log('Connected.');
    global['connect_time'] = new Date().getTime();
});

var concurQueryCount = 0;
var executeQueue = [];
var batchQueue = [];
var getPageQueue = [];

var queryTimes = [];
var QUERY_TIME_FRAME = 10;
var queryTimeIndex = 0;

var MAX_CONCURRENT_QUERY = 32768 / 1;
//var MAX_CONCURRENT_QUERY = 1000;

process.on('message', function (data) {
    if(data.ac == MasterProcessActions.FETCH_CASSANDRA_CONCURRENT_QUERY) {
        process.send({ac: MasterProcessActions.FETCH_CASSANDRA_CONCURRENT_QUERY, value: concurQueryCount});
    }
});

//check query time, if slow --> reconnect db
setInterval(function () {
    var total = 0;
    for(var i = 0; i < QUERY_TIME_FRAME; i ++) {
        total += queryTimes[i] ? queryTimes[i] : 0;
    }

    var time = total / QUERY_TIME_FRAME;

    if(time > 5 * 1000) {
        if(process.send) {
            process.send({ac: MasterProcessActions.CASSANDRA_SLOW_REPORT, time: time});
        }
    } else {
        if(process.send) {
            process.send({ac: MasterProcessActions.UPDATE_QUERY_TIME, time: time});
        }
    }
}, 60 * 1000);

process.on('message', function (data) {
    if(data.ac == MasterProcessActions.FETCH_QUERY_TIME) {
        var total = 0;
        for(var i = 0; i < QUERY_TIME_FRAME; i ++) {
            total += queryTimes[i] ? queryTimes[i] : 0;
        }

        var time = total / QUERY_TIME_FRAME;

        process.send({ac: MasterProcessActions.FETCH_QUERY_TIME, time: time});
    }
});

var startTime = [];
var queryId = -1;
var dbExecute = function () {
    if(concurQueryCount < MAX_CONCURRENT_QUERY) {
        if (executeQueue.length > 0) {
            concurQueryCount += 1;
            var arguments = executeQueue.shift();
            queryId = (queryId + 1) % MAX_CONCURRENT_QUERY;
            var tempId = queryId;
            startTime[tempId] = new Date().getTime();
            var callback = arguments[arguments.length - 1];
            arguments[arguments.length - 1] = function (error, results) {
                if(error) {
                    console.log(error);
                }
                callback(error, results, tempId);
            };
            client.execute.apply(client, arguments);
        }
    }
};

var dbGetPage = function () {
    if(concurQueryCount < MAX_CONCURRENT_QUERY) {
        if (getPageQueue.length > 0) {
            concurQueryCount += 1;
            var arguments = getPageQueue.shift();
            queryId = (queryId + 1) % MAX_CONCURRENT_QUERY;
            var tempId = queryId;
            startTime[tempId] = new Date().getTime();
            var callback = arguments[arguments.length - 1];

            var rows = [];
            delete arguments[2].autoPage;
            client.eachRow(arguments[0], arguments[1], arguments[2], function (n, row) {
                rows.push(row);
            }, function (error, result) {
                if(error) {
                    callback(error)
                } else {
                    var response = {};
                    response.rows = rows;
                    response.pageState = result.pageState;
                    response.hasNext = !!result.nextPage;
                    callback(null, response, tempId);
                }
            })
        }
    }
}

var dbBatch = function () {
    if(concurQueryCount < MAX_CONCURRENT_QUERY) {
        if (batchQueue.length > 0) {
            concurQueryCount += 1;
            var arguments = batchQueue.shift();
            queryId = (queryId + 1) % MAX_CONCURRENT_QUERY;
            var tempId = queryId;
            startTime[tempId] = new Date().getTime();
            var callback = arguments[arguments.length - 1];
            arguments[arguments.length - 1] = function (error, results) {
                callback(error, results, tempId);
            };
            client.batch.apply(client, arguments);
        }
    }
};

var execute = function (query, param, options, callback) {
    if(typeof options == 'function') {
        callback = options;
        options = null;
    }
    var altCallback = function (error, result, tempId) {
        var endTime = new Date().getTime();
        queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
        queryTimes[queryTimeIndex] = endTime - startTime[tempId];

        //console.log('execute time: ' + (endTime - startTime[tempId]));

        concurQueryCount -= 1;
        if(callback) {
            callback(error, result);
        }

        dbExecute();
        dbBatch();
        dbGetPage();
    };
    executeQueue.push([query, param, options, altCallback]);
    dbExecute();
};

var getPage = function (query, param, options, callback) {
    if(typeof options == 'function') {
        callback = options;
        options = null;
    }
    var altCallback = function (error, result, tempId) {
        var endTime = new Date().getTime();
        queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
        queryTimes[queryTimeIndex] = endTime - startTime[tempId];

        concurQueryCount -= 1;
        if(callback) {
            callback(error, result);
        }

        dbExecute();
        dbBatch();
        dbGetPage();
    };
    getPageQueue.push([query, param, options, altCallback]);
    dbGetPage()
}

var batch = function (queries, options, callback) {
    if(typeof options == 'function') {
        callback = options;
        options = null;
    }
    var altCallback = function (error, result, tempId) {
        var endTime = new Date().getTime();
        queryTimeIndex = (queryTimeIndex + 1) % QUERY_TIME_FRAME;
        queryTimes[queryTimeIndex] = endTime - startTime[tempId];

        //console.log('batch time: ' + (endTime - startTime[tempId]));

        concurQueryCount -= 1;
        if(callback) {
            callback(error, result);
        }

        dbExecute();
        dbBatch();
        dbGetPage();
    };
    batchQueue.push([queries, options, altCallback]);
    dbBatch();
};

/*
init IPC server user unix socket
 */
var cassServer = new IPCServer(options.id, function () {
    if(process.send) {
        process.send({ac: MasterProcessActions.READY});
    }
});
cassServer.addMethod('execute', function (params, callback) {
    //console.log();
    execute(params[0], params[1], params[2], function (error, results) {
        callback(error, results);
    });

    //count request
    reqCountIndex = (reqCountIndex + 1) % 5000;
    requestTimeMark[reqCountIndex] = new Date().getTime();
});

cassServer.addMethod('batch', function (params, callback) {
    batch(params[0], params[1], function (error, results) {
        callback(error, results);
    });

    //count request
    reqCountIndex = (reqCountIndex + 1) % 5000;
    requestTimeMark[reqCountIndex] = new Date().getTime();
});

cassServer.addMethod('getPage', function (params, callback) {
    getPage(params[0], params[1], params[2], function (error, results) {
        callback(error, results);
    });

    //count request
    reqCountIndex = (reqCountIndex + 1) % 5000;
    requestTimeMark[reqCountIndex] = new Date().getTime();
});

cassServer.addMethod('getConcurrent', function (params, callback) {
    callback(null, concurQueryCount);
});

cassServer.addMethod('countRequest', function (params, callback) {
    if(reqCountIndex == -1) {
        callback(null, 0);
    } else {
        var i = reqCountIndex;
        var count = 0;
        var currentTime = new Date().getTime();
        while(true) {
            if(requestTimeMark[i] && requestTimeMark[i] > currentTime - 10 * 1000) {
                count += 1;
            } else {
                break;
            }

            if(i < 0) {
                i = 5000;
            }
            i -= 1;
        }

        callback(null, count / 10);
    }
})

cassServer.start();

/*
handle count request from parent process
 */
process.on('message', function (data) {
    switch (data.ac) {
        case MasterProcessActions.COUNT_REQUEST:
            if(reqCountIndex == -1) {
                process.send({ac: MasterProcessActions.COUNT_REQUEST, count: 0});
            } else {
                var i = reqCountIndex;
                var count = 0;
                var currentTime = new Date().getTime();
                while(true) {
                    if(requestTimeMark[i] && requestTimeMark[i] > currentTime - 10 * 1000) {
                        count += 1;
                    } else {
                        break;
                    }

                    if(i < 0) {
                        i = 5000;
                    }
                    i -= 1;
                }

                process.send({ac: MasterProcessActions.COUNT_REQUEST, count: count / 10});
            }
            break;
    }
});
