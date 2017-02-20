/**
 * Created by DICH on 7/9/2016.
 */
var TAG = 'IPCNetServer: ';
var SECRET_KEY = 'd963c4e0-f71f-11e6-bb0c-9d42d1a49e77';

var IPCNetServer = function (id, host, port, onStarted) {
    const RawIpc = require('node-ipc').IPC;
    this.ipc = new RawIpc;
    this.ipc.config.id = id;
    this.ipc.config.retry = 1000;
    this.ipc.config.silent = true;
    this.methods = {};
    var self = this;

    this.ipc.serveNet(host, port, function () {
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
};

/**
 *
 * @param methodName
 * @param func
 * @param {error} callback
 */
IPCNetServer.prototype.addMethod = function (methodName, func, callback) {
    if(!callback) {
        callback = function () {

        };
    }

    var self = this;
    if(!self.methods[methodName]) {
        self.methods[methodName] = func;
        callback();
    } else {
        var error = {};
        error.ec = 62;
        error.message = 'method existed';
        callback(error);
    }
};

IPCNetServer.prototype.start = function () {
    this.ipc.server.start();
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

    var key = params.splice(params.length - 1, 1)[0];
    if(key == SECRET_KEY) {
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
    } else {
        error = {};
        error.ec = 29;
        error.message = 'secret key not match';
        callback(error);
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

    var key = params.splice(params.length - 1, 1)[0];
    if(key == SECRET_KEY) {
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
    } else {
        error = {};
        error.ec = 29;
        error.message = 'secret key not match';
        callback(error);
    }
};

module.exports = IPCNetServer;