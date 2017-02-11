Kudu Sink
==========

CDAP Plugin for ingesting data into Apache Kudu. This plugin can be configured to be used in Batch and Real-time pipelines. 

Configuration
-------------

**Required Fields**
###################

* _**Table Name**_

 This configuration specifies the Kudu table name to which the records will be written. This plugin checks if the table already exists. If it exists, it compares the schema of the existing table with the write schema specified for the plugin, If they don't match an error is thrown at configuration time and If the table doesn't exist, the table is created. 

* _**Kudu Master Hosts**_

 Specifies the list of Kudu master hosts that this plugin will attempt connect to. It's a comma separated list of &lt;hostname&gt;:&lt;port&gt;. Connection is attempt after the plugin is initialized in the pipeline. 
 
* _**Schema**_
 
 Specifies the write schema to be used to write to Kudu. 

**Optional Fields**
###################

* _**Operation Timeout**_

 This configuration sets the timeout in milliseconds for user operations with Kudu. If you are writing large sized records it's recommended to increase the this time. It's defaulted to 30 seconds. 
 
* _**Administration Operation Timeout**_

 This configuration is used to set timeout in milliseconds for administrative operations like for creating table if table doesn't exist. This time is mainly used during initialize phase of the plugin when the table is created if it doesn't exist. 
 
* _**Columns to Hash**_

 Add a set of hash partitions to the above table. Each column specified here is part of tables primary key and a column will only appear in a single hash. 
 
* _**Hashing Seed**_

 The seed value specified is used to randomize mapping of rows to hash buckets. Setting the seed will ensure the hashed columns contain user provided values.
 
* _**Number of replicas**_

 Specifies the number of replicas for the above table. This will specify the number of replicas that each tablet will have. By default it will use the default set on the server side and that is generally 3. 
 
* _**Column Compression Algorithm**_

 Specifies the compression algorithm to be used for the columns. Following are different options available. 
  * Default Compression (Snappy)
  * No Compression
  * Snappy Compression
  * LZ4 Compression
  * ZLib Compression
  
* _**Encoding**_

 Specifies the block encoding for the column. Following are different options available. 
 
  * Auto Encoding
  * Plain Encoding
  * Prefix Encoding
  * Group Varint Encoding
  * Run Length Encoding (RLE)
  * Dictionary Encoding
  * Bit Shuffle Encoding


Build
-----
To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

UI Integration
--------------
The Cask Hydrator UI displays each plugin property as a simple textbox. To customize how the plugin properties
are displayed in the UI, you can place a configuration file in the ``widgets`` directory.
The file must be named following a convention of ``[plugin-name]-[plugin-type].json``.

See [Plugin Widget Configuration](http://docs.cdap.io/cdap/current/en/hydrator-manual/developing-plugins/packaging-plugins.html#plugin-widget-json)
for details on the configuration file.

The UI will also display a reference doc for your plugin if you place a file in the ``docs`` directory
that follows the convention of ``[plugin-name]-[plugin-type].md``.

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json file under the ``target`` directory. This file is deployed along with your .jar file to add your
plugins to CDAP.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'my-plugins-1.0.0':

    > load artifact target/my-plugins-1.0.0.jar config-file target/my-plugins-1.0.0.json
