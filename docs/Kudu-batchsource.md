Kudu Source
==========

CDAP Plugin for reading data from Apache Kudu table.


Usage Notes
-----------

This plugin is configured to pull rows from a Kudu table using the Kudu native client. When configuring this plugin in a pipeline one can use the **"Generate Table Schema"** to automatically create the CDAP schema from the Kudu table. In order to generate the schema, you would have to first specify the ```Table name``` and the ```Master address```.

In case, one of the configuration (either Table name or Master address) is a macro the **"Generate Table Schema"** will not work. The best way is to provide a non-macros configuration to generate the schema first and then change it the configuration use macro.

The plugin also supports projecting columns efficiently when it's specified. If '\*' is specified then all the columns in the table are projected else only the fields specified will be projected. Please make sure the columns to be projected are specified before schema is generated.

Plugin Configuration
---------------------

| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Table Name** | **Y** | N/A | This configuration specifies the Kudu table name to which the records will be written. This plugin checks if the table already exists. If it exists, it compares the schema of the existing table with the write schema specified for the plugin, If they don't match an error is thrown at configuration time and If the table doesn't exist, the table is created.|
| **Kudu Master Host** | **Y** | N/A | Specifies the list of Kudu master hosts that this plugin will attempt connect to. It's a comma separated list of &lt;hostname&gt;:&lt;port&gt;. Connection is attempt after the plugin is initialized in the pipeline.  |
| **Column Projection** | **N** | '*' | Specifies the columns to be projected. By default it's '*' for projecting all columns in the table.
| **Operation Timeout** | N | 30000 | This configuration sets the timeout in milliseconds for user operations with Kudu. If you are writing large sized records it's recommended to increase the this time. It's defaulted to 30 seconds. |
