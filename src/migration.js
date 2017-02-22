var async = require('async');
var cassandraUtil = require('../lib/cassandra_util');

module.exports = mixinMigration;

function mixinMigration(Cassandra) {
    /*!
     * Discover the properties from a table
     * @param {String} model The model name
     * @param {Function} cb The callback function
     */
    Cassandra.prototype.getTableColumns = function (model, cb) {
        var tableName = this.getTableName(model);
        var query = 'SELECT column_name,type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?';

        this.cassandraClient.execute(query, [this.settings.keyspace, tableName], {prepare: true}, function (error, results) {
            cassandraUtil.responseArray(error, results, function (error, columns) {
                var  currentColumns = {};
                for(var i = 0; i < columns.length; i ++) {
                    currentColumns[columns[i].column_name] = columns[i].type;
                }

                cb(error, currentColumns);
            })
        });
    }

    /**
     * get all index of table
     * @param model
     * @param cb
     */
    Cassandra.prototype.getTableIndexes = function (model, cb) {
        var tableName = this.getTableName(model);
        var query = 'SELECT index_name,options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?';
        this.cassandraClient.execute(query, [this.settings.keyspace, tableName], {prepare: true}, function (error, results) {
            cassandraUtil.responseArray(error, results, function (error, indexes) {
                var  currentIndexs = {};
                for(var i = 0; i < indexes.length; i ++) {
                    var column = indexes[i].options.target;
                    currentIndexs[column] = indexes[i].index_name;
                }

                cb(error, currentIndexs);
            })
        });
    }

    /**
     * Perform autoupdate for the given models
     * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
     * @param cb
     * @callback {Function} [callback] The callback function
     */
    Cassandra.prototype.autoupdate = function (models, cb) {
        var self = this;
        if ((!cb) && ('function' === typeof models)) {
            cb = models;
            models = undefined;
        }
        // First argument is a model name
        if ('string' === typeof models) {
            models = [models];
        }

        models = models || Object.keys(this._models);

        var updateIndex = function (model, done) {
            self.getTableIndexes(model, function (error, indexes) {
                self.alterIndexes(model, indexes, done);
            })
        }

        async.each(models, function (model, done) {
            if (!(model in self._models)) {
                return process.nextTick(function () {
                    done(new Error('Model not found: ' + model));
                });
            }
            self.getTableColumns(model, function (err, columns) {
                if (!err && Object.keys(columns).length > 0) {
                    self.alterTable(model, columns, function (error) {
                        if(error) {
                            throw error;
                        } else {
                            updateIndex(model, done);
                        }
                    });
                } else if(!err) {
                    self.createTable(model, function (error) {
                        if(error) {
                            throw error;
                        } else {
                            updateIndex(model, done);
                        }
                    });
                } else {
                    done(err);
                }
            });
        }, cb);
    };

    /*!
     * Alter the table for the given model
     * @param {String} model The model name
     * @param {Object[]} actualFields Actual columns in the table
     * @param {Function} [cb] The callback function
     */
    Cassandra.prototype.alterTable = function (model, columns, cb) {
        var self = this;
        var alterColumnsClause = self.getAlterColumns(model, columns);

        async.each(alterColumnsClause, function (query, done) {
            self.cassandraClient.execute(query, null, null, done);
        }, cb)
    };

    /**
     *
     * @param model
     * @param actualFields
     * @return {Array}
     */
    Cassandra.prototype.getAlterColumns = function (model, actualFields) {
        var alterColumnsClause = [];
        var self = this;
        var modelDefine = self.getModelDefinition(model);
        var props = modelDefine.model.definition.rawProperties;

        for(var column in props) {
            var type = props[column].type || props[column];
            if(!actualFields[column] && typeof type === 'string') {
                alterColumnsClause.push('ALTER TABLE ' + self.getTableName(model) + ' ' +
                    'ADD ' + column + ' ' + self.getCassandraType(type));
            }
        }

        return alterColumnsClause;
    }

    /**
     *
     * @param model
     * @param currentIndexes
     * @param callback
     */
    Cassandra.prototype.alterIndexes = function (model, currentIndexes, callback) {
        var self = this;
        var alterIndexClause = self.getAlterIndexs(model, currentIndexes);

        async.each(alterIndexClause, function (query, done) {
            self.cassandraClient.execute(query, null, null, done);
        }, callback)
    }

    /**
     *
     * @param model
     * @param currentIndexes
     * @return {Array}
     */
    Cassandra.prototype.getAlterIndexs = function (model, currentIndexes) {
        var alterIndexClause = [];
        var self = this;
        var modelDefine = self.getModelDefinition(model);
        var indexes = modelDefine.model.definition.settings.indexes || {};

        for(var column in indexes) {
            if(!currentIndexes[column]) {
                alterIndexClause.push(self.buildIndex(model, column));
            }
        }

        return alterIndexClause;
    }

    /*!
     * Create a table for the given model
     * @param {String} model The model name
     * @param {Function} [cb] The callback function
     */
    Cassandra.prototype.createTable = function (model, callback) {
        console.log(model);
        var modelDefine = this.getModelDefinition(model);
        var properties = modelDefine.model.definition.rawProperties;
        var partitionKeys = modelDefine.model.definition.settings.partitionKeys || [];
        var clusteringKeys = modelDefine.model.definition.settings.clustering || [];
        var orderColumns = {};
        var tableName = this.getTableName(model);

        var query = 'CREATE TABLE IF NOT EXISTS ' + tableName + ' ('
        for (var column in properties) {
            if (typeof properties[column] === 'object' && typeof properties[column].type === 'string') {
                query += column + ' ' + this.getCassandraType(properties[column].type) + ', ';

                if (properties[column].order) {
                    orderColumns[column] = properties[column].order;
                }
            } else if(typeof properties[column] === 'string') {
                query += column + ' ' + this.getCassandraType(properties[column]) + ', ';
            }
        }

        if (partitionKeys.length == 0) {
            throw new Error('cassandra table must have partition key!');
        }

        query += 'PRIMARY KEY ((' + partitionKeys[0];
        for (var i = 1; i < partitionKeys.length; i++) {
            query += ', ' + partitionKeys[i];
        }
        query += ')';
        for (var i = 0; i < clusteringKeys.length; i++) {
            query += ', ' + clusteringKeys[i];
        }
        query += '))';

        if (Object.keys(orderColumns).length > 0) {
            query += ' WITH CLUSTERING ORDER BY (' + clusteringKeys[0] + ' ' + (orderColumns[clusteringKeys[0]] || ' ASC');
            for (var i = 1; i < clusteringKeys.length; i++) {
                query += ', ' + clusteringKeys[i] + ' ' + (orderColumns[clusteringKeys[i]] || 'ASC');
            }

            query += ')';
        }

        query += ';';

        this.cassandraClient.execute(query, null, null, function (error) {
            callback(error);
        })
    }

    /**
     * build index clause for property
     * @param model
     * @param property
     * @return {*}
     */
    Cassandra.prototype.buildIndex = function (model, property) {
        var modelDefine = this.getModelDefinition(model);
        var indexes = modelDefine.model.definition.settings.indexes;
        if(indexes && indexes[property]) {
            return 'CREATE INDEX ' + (indexes[property].name || indexes[property])+ ' ON ' + this.getTableName(model) + ' ' +
                '(' + property + ');';
        } else {
            return null;
        }
    };

    /**
     * build all index clause for table
     * @param model
     * @return {Array}
     */
    Cassandra.prototype.buildIndexes = function (model) {
        var indexClauses = [];
        var modelDefine = this.getModelDefinition(model);
        var indexes = modelDefine.model.definition.settings.indexes || {};

        for(var property in indexes) {
            var indexClause = this.buildIndex(model, property);
            if(indexClause) {
                indexClauses.push(indexClause);
            }
        }

        return indexClauses;
    };
}
