## loopback-connector-cassandra

- loopback connector for cassandra database
- build method create, findOne, find, patch, update, delete
- To export Rest API from model
    + insert "loopback-connector-cassandra/models" to model-config.json to load BaseCassandraModel
    + extend BaseCassandraModel when create your custom model
- example model config
    {
      "name": "customModel",
      "base": "BaseCassandraModel",
      "idInjection": false,
      "options": {
        "validateUpsert": true
      },
      "properties": {
        "subject": {
          "type": "string",
          "required": true
        },
        "time": {
          "type": "timeuuid",
          "required": true,
          "order": "DESC"
        },
        "value": {
          "type": "string"
        }
      },
      "partitionKeys": ["subject"],
      "clustering": ["time"],
      "indexes": {
        "value": {
            "name": "value_custom_model"
        }
      }
      "validations": [],
      "relations": {},
      "acls": [],
      "methods": {}
    }

## Connector settings

