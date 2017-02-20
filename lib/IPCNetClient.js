/**
 * Created by DICH on 7/9/2016.
 */
var SECRET_KEY = 'd963c4e0-f71f-11e6-bb0c-9d42d1a49e77';

var IPCNetClient = function (serverId, host, port, onConnect) {
    this.serverId = serverId;
    this.ipc = require('node-ipc');
    this.ipc.config.id   = serverId;
    this.ipc.config.retry= 1000;
    this.ipc.config.silent = true;
    this.cbMap = {};
    this.cbIndex = -1;
    this.connected = false;
    var self = this;

    this.ipc.connectToNet(this.serverId, host, port, function () {
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
};

/**
 *
 * @param method
 * @param arguments
 * @param params
 * @param {error, data} callback
 */
IPCNetClient.prototype.call = function (method, params, callback) {
    var self = this;

    if(!self.server) {
        var error = {};
        callback(error);
        return;
    }

    if(!params) {
        params = [];
    }
    params.push(SECRET_KEY);

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
IPCNetClient.prototype.callMethod = function (method, params, callback) {
    var self = this;

    if(!self.server) {
        var error = {};
        callback(error);
        return;
    }

    if(!params) {
        params = [];
    }
    params.push(SECRET_KEY);

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
IPCNetClient.prototype.isConnected = function () {
    return this.connected;
};

module.exports = IPCNetClient;