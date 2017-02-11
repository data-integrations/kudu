# Apache Kudu Batch Sink

Description
-----------

Writes incoming data to an Apache Kudu cluster.

Use Case
--------

This sink can be used to write data to an Apache Kudu cluster either from a 
data pipeline batch pipeline or a data streams streaming pipeline.

Properties
----------

**tableName:** Name of the Apache Kudu table to write to.

**masterAddresses:** Comma separated list of hostname:port of Apache Kudu master servers.

**schema:** Schema for incoming records into the Apache Kudu Batch Sink. Currently, only simple types 
such as integer, string, boolean, float, long, double and byte arrays are supported

**operationTimeoutMs:** Apache Kudu operation timeout in milliseconds. Defaults to 30000 ms.


Example
-------

This example lowercases the 'name' field and uppercases the 'id' field:

    {
      "name": "Kudu",
      "plugin": {
        "name": "Kudu",
        "type": "batchsink",
        "label": "Kudu",
        "artifact": {
          "name": "kudu-plugins",
          "version": "1.0-SNAPSHOT",
          "scope": "SYSTEM"
        },
        "properties": {
          "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}",
          "tableName": "customer",
          "masterAddresses": "quickstart.cloudera:7051",
          "operationTimeoutMs": "30000",
          "referenceName": "Kudu"
        }
      }
    }
