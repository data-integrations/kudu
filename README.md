Kudu Sink
==========

CDAP Plugin for ingesting data into Apache Kudu. This plugin can be configured to be used in Batch and Real-time pipelines. 

Configurations
---------------

Required Fields
****************

* Table Name
* Kudu Master Hosts
* Schema

Optional Fields
****************
* Operation Timeout (milliseconds)
* Administration Operation Timeout (milliseconds)
* Columns to Hash
* Hashing Seed
* Column Compression Algorithm
  * Default Compression (Snappy)
  * No Compression
  * Snappy Compression
  * LZ4 Compression
  * ZLib Compression
* Encoding
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
