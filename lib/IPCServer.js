/**
 * Created by Dich on 3/13/2016.
 */
var IPCNetServer = require('./IPCNetServer');

var TAG = 'IPCServer: ';

var IPCServer = function (id, host, port, onStarted) {
    if(typeof host == 'function') {
        onStarted = host;
        host = port = null;
    } else if(typeof port == 'function') {
        onStarted = port;
        port = host;
        host = '127.0.0.1';
    }

    const RawIpc = require('node-ipc').IPC;
    this.ipc = new RawIpc;
    this.ipc.config.id = id;
    this.ipc.config.retry = 1000;
    this.ipc.config.silent = true;
    this.methods = {};
    var self = this;

    this.ipc.serve(function () {
        self.ipc.server.on('call', function (data, socket) {
            call.call(self, data, socket);
        });

        self.ipc.server.on('call_method', function (data, socket) {
            callMethod.call(self, data, socket);
        });

        if(onStarted) {
            onStarted();
        }
    });

    if(host && port) {
        this.netServer = new IPCNetServer('net-' + id, host, port);
    }
};

/**
 *
 * @param methodName
 * @param func
 * @param {error} callback
 */
IPCServer.prototype.addMethod = function (methodName, func, callback) {
    if(!callback) {
        callback = function () {

        };
    }

    var self = this;
    if(!self.methods[methodName]) {
        self.methods[methodName] = func;

        if(self.netServer) {
            self.netServer.addMethod(methodName, func, callback);
        } else {
            callback();
        }
    } else {
        var error = {};
        error.ec = 62;
        error.message = 'method existed';
        callback(error);
    }
};

IPCServer.prototype.start = function () {
    this.ipc.server.start();
    if(this.netServer) {
        this.netServer.start();
    }
};

/**
 *
 * @param data
 * @param socket
 */
var call = function (data, socket) {
    var self = this;

    var callId = data.call_id;
    var method = data.method;
    var params = data.params;
    params.push(function (error, result) {
        if(callId) {
            self.ipc.server.emit(socket, 'callback', {
                call_id: callId,
                res: {
                    error: error,
                    result: result
                }
            });
        }
    });
    if(self.methods[method]) {
        self.methods[method].apply(null, params);
    } else if(callId) {
        var error = {};
        error.ec = 3;
        error.message = 'method not exist';
        self.ipc.server.emit(socket, 'callback', {
            call_id: callId,
            res: {
                error: error
            }
        });
    }
};

/**
 *
 * @param data
 * @param socket
 */
var callMethod = function (data, socket) {
    var self = this;

    var callId = data.call_id;
    var method = data.method;
    var params = data.params;
    var cb = function (error, result) {
        if(callId) {
            self.ipc.server.emit(socket, 'cb_method', {
                call_id: callId,
                res: {
                    error: error,
                    result: result
                }
            });
        }
    };

    if(self.methods[method]) {
        self.methods[method](params, cb);
    } else if(callId) {
        var error = {};
        error.ec = 3;
        error.message = 'method not exist';
        self.ipc.server.emit(socket, 'cb_method', {
            call_id: callId,
            res: {
                error: error
            }
        });
    }
};

module.exports = IPCServer;