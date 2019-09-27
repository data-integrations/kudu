/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.kudu;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.io.NullWritable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.Path;

/**
 * A Kudu Source Plugin,
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Kudu")
@Description("Plugin for reading data from Apache KuduSource.")
public class KuduSource extends ReferenceBatchSource<NullWritable, RowResult, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSource.class);
  private final KuduSourceConfig kuduSourceConfig;
  private Schema schema;
  private final static int MASK = 0xff;

  public KuduSource(KuduSourceConfig kuduSourceConfig) {
    super(new ReferencePluginConfig(kuduSourceConfig.getReferenceName()));
    this.kuduSourceConfig = kuduSourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    FailureCollector failureCollector = configurer.getStageConfigurer().getFailureCollector();
    kuduSourceConfig.validate(failureCollector);

    configurer.getStageConfigurer().setOutputSchema(kuduSourceConfig.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    kuduSourceConfig.validate(failureCollector);
    failureCollector.getOrThrowException();

    context.setInput(Input.of(kuduSourceConfig.getReferenceName(), new KuduInputFormatProvider(kuduSourceConfig)));
  }

  /**
   * Convert the row type from {@link RowResult} to {@link StructuredRecord}.
   *
   * @param input record read from the Kudu source.
   * @param emitter to emit the structured record.
   */
  @Override
  public void transform(KeyValue<NullWritable, RowResult> input, Emitter<StructuredRecord> emitter) throws Exception {
    RowResult result = input.getValue();

    // Extract the schema from the result.
    org.apache.kudu.Schema kSchema = result.getSchema();
    StructuredRecord.Builder record = StructuredRecord.builder(kuduSourceConfig.getSchema());

    // Iterate through each column in the result and convert it to CDAP type.
    for (ColumnSchema column : kSchema.getColumns()) {
      Type type = column.getType();
      String name = column.getName();
      if (result.isNull(name)) {
        record.set(name, null);
      } else {
        if (type == Type.BINARY) {
          record.set(name, result.getBinaryCopy(name));
        } else if (type == Type.BOOL) {
          record.set(name, result.getBoolean(name));
        } else if (type == Type.DOUBLE) {
          record.set(name, result.getDouble(name));
        } else if (type == Type.FLOAT) {
          record.set(name, result.getFloat(name));
        } else if (type == Type.INT32) {
          record.set(name, result.getInt(name));
        } else if (type == Type.INT64) {
          record.set(name, result.getLong(name));
        } else if (type == Type.INT16) {
          record.set(name, (int) result.getShort(name));
        } else if (type == Type.INT8) {
          record.set(name, (int) (result.getByte(name) & MASK));
        } else if (type == Type.STRING) {
          record.set(name, result.getString(name));
        } else {
          throw new Exception(
            String.format("Unsupported type '%s', field '%s'", type.toString(), name)
          );
        }
      }
    }

    // Emit the structured record.
    emitter.emit(record.build());
  }

  /**
   * Request object for retrieving schema from the Kudu table.
   */
  class Request {
    // Specifies the master list of servers.
    public String master;

    // Name of the table.
    public String name;

    // Columns to be projected.
    public String columns;
  }

  /**
   * Returns the Kudu table schema based on the configuration specified in {@link Request}
   *
   * @param request containing information to connect to Kudu to retrieve schema.
   * @return {@link Schema} of the database.
   */
  @Path("getSchema")
  public Schema getSchema(Request request) throws Exception {
    boolean isStar = false;
    Set<String> columnFilter = new HashSet<>();

    // We create a set of columns that are part of the projection list.
    // This is applicable only when the projection is not '*'.
    if (request.columns != null) {
      if (request.columns.equalsIgnoreCase("*")) {
        isStar = true;
      } else {
        for (String col : request.columns.split(",")) {
          columnFilter.add(col.trim().toLowerCase());
        }
      }
    }

    // Configure the connection to client.
    KuduClient client = new KuduClient.KuduClientBuilder(request.master)
      .defaultAdminOperationTimeoutMs(10000)
      .disableStatistics()
      .build();

    try {
      // Check if the table doesn't exist. If the table doesn't exit, then there is nothing
      // much we can do.
      if (!client.tableExists(request.name)) {
        throw new IllegalArgumentException(
          String.format("Table '%s' specified in the configuration does not exist.", request.name)
        );
      } else {
        // Open the table, extract schema and map it to CDAP schema.
        KuduTable table = client.openTable(request.name);
        org.apache.kudu.Schema schema = table.getSchema();
        List<ColumnSchema> columns = schema.getColumns();
        List<Schema.Field> fields = new ArrayList<>();

        // Iterate through each column coming from the Kudu table.
        // Check if it's available in the projection list, iff it's not '*'.
        for (ColumnSchema column : columns) {
          String kName = column.getName();

          // If projection is not for all fields, then filter out the fields
          // in the list.
          if (!isStar) {
            if (!columnFilter.contains(kName.toLowerCase().trim())) {
              continue;
            }
          }

          Type kType = column.getType();
          boolean kNullable = column.isNullable();

          // Convert the types to CDAP schema.
          Schema.Type type = Schema.Type.STRING;
          if (kType == Type.STRING) {
            type = Schema.Type.STRING;
          } else if (kType == Type.DOUBLE) {
            type = Schema.Type.DOUBLE;
          } else if (kType == Type.FLOAT) {
            type = Schema.Type.FLOAT;
          } else if (kType == Type.BINARY) {
            type = Schema.Type.BYTES;
          } else if (kType == Type.BOOL) {
            type = Schema.Type.BOOLEAN;
          } else if (kType == Type.INT32) {
            type = Schema.Type.INT;
          } else if (kType == Type.INT64) {
            type = Schema.Type.LONG;
          } else if (kType == Type.INT16) {
            type = Schema.Type.INT;
          } else if (kType == Type.INT8) {
            type = Schema.Type.INT;
          }
          // Make sure we set the field to nullable or not.
          Schema fieldType = null;
          if (kNullable) {
            fieldType = Schema.nullableOf(Schema.of(type));
          } else {
            fieldType = Schema.of(type);
          }
          Schema.Field field = Schema.Field.of(kName, fieldType);
          fields.add(field);
        }
        return Schema.recordOf("output", fields);
      }
    } catch (KuduException e) {
      throw new Exception(
        String.format("Unable to connect to Kudu to extract information for table '%s'. %s",
                      request.name, e.getMessage())
      );
    } catch (Exception e) {
      throw new Exception(
        String.format("Something unexpected happened while getting table '%s' schema. %s",
                      request.name, e.getMessage())
      );
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
