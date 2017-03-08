'use strict';

module.exports = function(BaseCassandraModel) {
    // BaseCassandraModel.create = BaseCassandraModel.createRest = function (data, callback) {
    //     this.getConnector().create(this.modelName, data, function (error, info) {
    //         callback(error, info)
    //     })
    // }
    //
    // BaseCassandraModel.findOne = BaseCassandraModel.findOneRest = function (keyValues, columns, callback) {
    //     console.log('11111111111111111111111111111');
    //     console.log(this);
    //     this.getConnector().findOne(this.modelName, keyValues, columns, function (error, info) {
    //         callback(error, info)
    //     })
    // }
    //
    // BaseCassandraModel.find = BaseCassandraModel.findRest = function (keyValues, columns, orderBy, limit, callback) {
    //     this.getConnector().find(this.modelName, keyValues, columns, orderBy, limit, function (error, infos) {
    //         callback(error, infos)
    //     })
    // }
    //
    // BaseCassandraModel.patch = BaseCassandraModel.patchRest = function (data, callback) {
    //     this.getConnector().update(this.modelName, data, false, function (error) {
    //         callback(error)
    //     })
    // }
    //
    // BaseCassandraModel.update = BaseCassandraModel.updateRest = function (data, callback) {
    //     this.getConnector().update(this.modelName, data, true, function (error) {
    //         callback(error)
    //     })
    // }
    //
    // BaseCassandraModel.delete = BaseCassandraModel.deleteRest = function (keyValues, columns, callback) {
    //     this.getConnector().delete(this.modelName, keyValues, columns, function (error) {
    //         callback(error)
    //     })
    // }

    var originalSetup = BaseCassandraModel.setup;
    BaseCassandraModel.setup = function() { // this will be called everytime a // model is extended from this model.
        originalSetup.apply(this, arguments); // This is necessary if your
        // AnotherModel is based of another model, like PersistedModel.

        var partitionKeys = this.definition.settings.partitionKeys;
        var clustering = this.definition.settings.clustering || [];
        var indexes = this.definition.settings.indexes ? Object.keys(this.definition.settings.indexes) : [];
        var props = this.definition.rawProperties;

        if(!partitionKeys || !partitionKeys.length) {
            throw new Error('partition keys must be defined in model ' + this.modelName)
        }

        var keys = partitionKeys.concat(clustering);
        var keysAndIndexs = keys.concat(indexes);

        var keysModel = {};
        for(var i = 0; i < keys.length; i ++) {
            keysModel[keys[i]] = props[keys[i]].type || props[keys[i]];
        }

        var keysAndIndexModel = {};
        for(var i = 0; i < keysAndIndexs.length; i ++) {
            keysAndIndexModel[keysAndIndexs[i]] = props[keysAndIndexs[i]].type || props[keysAndIndexs[i]];
        }

        var self = this;
        this.remoteMethod('findOne', {
            description: 'Find instance of cassandra model by full keys fields.',
            accessType: 'READ',
            http: {
                path: '/findOne',
                verb: 'get'
            },
            accepts: [
                {
                    arg: 'keyValues',
                    type: 'object',
                    description: JSON.stringify(keysModel)
                },
                {
                    arg: 'columns',
                    type: 'array'
                }
            ],
            returns: {
                type: 'object',
                root: true
            }
        })

        this.remoteMethod('find', {
            description: 'Find all instance of the model matched by keys fields',
            accessType: 'READ',
            http: {
                path: '/',
                verb: 'get'
            },
            accepts: [
                {
                    arg: 'keyValues',
                    type: 'object',
                    description: JSON.stringify(keysAndIndexModel)
                },
                {
                    arg: 'columns',
                    type: 'array'
                },
                {
                    arg: 'orderBy',
                    type: 'string',
                    description: JSON.stringify(clustering) + ' desc'
                },
                {
                    arg: 'limit',
                    type: 'number'
                }
            ],
            returns: {
                type: 'array',
                root: true
            }
        })

        this.remoteMethod('patch', {
            description: 'Patch an existing model instance or insert a new one into the data source.',
            accessType: 'WRITE',
            http: {
                path: '/',
                verb: 'patch'
            },
            accepts: [
                {
                    arg: 'data',
                    type: 'object',
                    model: self.modelName,
                    http: {
                        source: 'body'
                    }
                }
            ],
            returns: {
                type: 'object',
                root: true
            }
        })

        this.remoteMethod('update', {
            description: 'Update exist instance of cassandra model by full keys fields.',
            accessType: 'WRITE',
            http: {
                path: '/',
                verb: 'put'
            },
            accepts: [
                {
                    arg: 'data',
                    type: 'object',
                    model: self.modelName,
                    http: {
                        source: 'body'
                    }
                }
            ],
            returns: {
                type: 'object',
                root: true
            }
        })

        this.remoteMethod('delete', {
            description: 'Delete one or many instance by key fields',
            accessType: 'WRITE',
            http: {
                path: '/delete',
                verb: 'del'
            },
            accepts: [
                {
                    arg: 'keyValues',
                    type: 'object',
                    description: JSON.stringify(keysModel)
                },
                {
                    arg: 'columns',
                    type: 'array'
                }
            ],
            returns: {
                type: 'object',
                root: true
            }
        })

        this.remoteMethod('create', {
            description: 'Insert new instance of model and persist into the data source.',
            accessType: 'WRITE',
            http: {
                path: '/',
                verb: 'post'
            },
            accepts: [
                {
                    arg: 'data',
                    type: 'object',
                    model: self.modelName,
                    http: {
                        source: 'body'
                    }
                }
            ],
            returns: {
                type: 'object',
                root: true
            }
        })
    };
};
