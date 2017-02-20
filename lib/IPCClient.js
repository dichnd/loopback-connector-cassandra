/**
 * Created by Dich on 3/14/2016.
 */

var IPCNetClient = require('./IPCNetClient');

var IPCClient = function (serverId, host, port, onConnect) {
    if(typeof host == 'function') {
        onConnect = host;
        host = port = null;
    } else if(typeof port == 'function') {
        onConnect = port;
        port = host;
        host = '127.0.0.1';
    }

    this.serverId = serverId;
    this.ipc = require('node-ipc');
    this.ipc.config.id   = serverId;
    this.ipc.config.retry= 1000;
    this.ipc.config.silent = true;
    this.cbMap = {};
    this.cbIndex = -1;
    this.connected = false;
    var self = this;

    this.ipc.connectTo(this.serverId, function () {
        self.ipc.of[self.serverId].on('connect', function () {
            console.log('connected to ' + serverId + ' server');
            self.server = self.ipc.of[self.serverId];
            self.connected = true;
            if(onConnect) {
                onConnect();
            }
        });

        self.ipc.of[self.serverId].on('callback', function (data) {
            var callId = data.call_id;
            var error = data.res.error;
            var result = data.res.result;
            if(callId && self.cbMap[callId]) {
                self.cbMap[callId](error, result);
                delete self.cbMap[callId];
            }
        });

        self.ipc.of[self.serverId].on('cb_method', function (data) {
            var callId = data.call_id;
            var error = data.res.error;
            if(callId && self.cbMap[callId]) {
                self.cbMap[callId](error, data.res);
                delete self.cbMap[callId];
            }
        });
    });

    if(host && port) {
        this.netClient = new IPCNetClient('net-' + serverId, host, port);
    }
};

/**
 *
 * @param method
 * @param arguments
 * @param params
 * @param {error, data} callback
 */
IPCClient.prototype.call = function (method, params, callback) {
    var self = this;

    if(!self.server) {
        //var error = {};
        //callback(error);
        //return;

        if(self.netClient) {
            return self.netClient.call(method, params, callback);
        } else {
            var error = {};
            return callback(error);
        }
    }

    var message = {
        method: method,
        params: params
    };

    if(callback && typeof callback === 'function') {
        self.cbIndex += 1;
        if (self.cbIndex < 0) {
            self.cbIndex = 0;
        }

        var callId = self.cbIndex + '';
        message.call_id = callId;
        self.cbMap[callId] = callback;
    } else {
        callback = function () {};
    }

    if(self.server) {
        self.server.emit('call', message);
    } else {
        callback();
    }
};

/**
 *
 * @param method
 * @param params
 * @param {error, data} callback
 */
IPCClient.prototype.callMethod = function (method, params, callback) {
    var self = this;

    if(!self.server) {
        //var error = {};
        //callback(error);
        //return;

        if(self.netClient) {
            return self.netClient.callMethod(method, params, callback);
        } else {
            var error = {};
            callback(error);
            return;
        }
    }

    var message = {
        method: method,
        params: params
    };

    if(callback && typeof callback === 'function') {
        self.cbIndex += 1;
        if (self.cbIndex < 0) {
            self.cbIndex = 0;
        }

        var callId = self.cbIndex + '';
        message.call_id = callId;
        self.cbMap[callId] = callback;
    } else {
        callback = function () {}
    }

    if(self.server) {
        self.server.emit('call_method', message);
    } else {
        callback({});
    }
};

/**
 *
 * @returns {boolean}
 */
IPCClient.prototype.isConnected = function () {
    return this.connected;
};

module.exports = IPCClient;