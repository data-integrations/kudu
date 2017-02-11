/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.kudu;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.io.NullWritable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.mapreduce.KuduTableOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link BatchSink} to write to Apache Kudu
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Kudu")
@Description("Writes to Kudu in text format.")
public class KuduSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Operation> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSink.class);

  private final Config config;

  private KuduClient kuduClient;
  private KuduTable kuduTable;
  private Schema outputSchema;

  public KuduSink(Config config) {
    super(config);
    this.config = config;
  }

  /**
   * Convert from {@link co.cask.cdap.api.data.schema.Schema.Type} to {@link Type}.
   *
   * @param type {@link StructuredRecord} field type.
   * @return {@link Type} Kudu type.
   * @throws TypeConversionException thrown when can't be converted.
   */
  private Type toKuduType(Schema.Type type) throws TypeConversionException {
    if (type == Schema.Type.STRING) {
      return Type.STRING;
    } else if (type == Schema.Type.INT) {
      return Type.INT32;
    } else if (type == Schema.Type.LONG) {
      return Type.INT64;
    } else if (type == Schema.Type.BYTES) {
      return Type.BINARY;
    } else if (type == Schema.Type.DOUBLE) {
      return Type.DOUBLE;
    } else if (type == Schema.Type.FLOAT) {
      return Type.FLOAT;
    } else if (type == Schema.Type.BOOLEAN) {
      return Type.BOOL;
    } else {
      throw new TypeConversionException("Field type specified is incompatible with Kudu field type.");
    }
  }

  /**
   * Converts from CDAP field types to Kudu types.
   *
   * @param schema CDAP Schema
   * @param columns List of columns that are considered as keys
   * @param algorithm Compression algorithm to be used for the column.
   * @param encoding Encoding type
   * @return List of {@link ColumnSchema}
   * @throws TypeConversionException thrown when CDAP schema cannot be converted to Kudu Schema.
   */
  private List<ColumnSchema> toKuduSchema(Schema schema, Set<String> columns,
                                                        ColumnSchema.CompressionAlgorithm algorithm,
                                                        ColumnSchema.Encoding encoding)
    throws TypeConversionException {
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      Type kuduType = toKuduType(field.getSchema().getType());
      ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, kuduType);
      if (field.getSchema().isNullable()) {
        builder.nullable(true);
      }
      builder.encoding(encoding);
      builder.compressionAlgorithm(algorithm);
      if (columns.contains(name)) {
        builder.key(true);
      }
      columnSchemas.add(builder.build());
    }
    return columnSchemas;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.optSchema), "Schema must be given as a property.");

    // Checks if the schema is a valid schema.
    Schema outputSchema;
    try {
      outputSchema = Schema.parseJson(config.optSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }

    // Check if the table exists in Kudu.
    configurer.getStageConfigurer().setOutputSchema(outputSchema);

    // Create a Kudu connection. A connection is attempted during the
    // deployment of the pipeline that contains this plugin.
    // NOTE: I am not sure if this is the right place for this to happen, but
    // not sure if it's the right place during initialization to create the
    // table if it doesn't exit.
    kuduClient = new KuduClient.KuduClientBuilder(config.optMasterAddresses)
      .defaultAdminOperationTimeoutMs(config.optAdminTimeoutMs)
      .disableStatistics()
      .build();

    // Check if the table exists, if table does not exist, then create one
    // with schema defined in the write schema.
    try {
      if (!kuduClient.tableExists(this.config.optTableName)) {
        List<ColumnSchema> columnSchemas = toKuduSchema(outputSchema, config.getColumns(),
                                                        config.getCompression(), config.getEncoding());
        org.apache.kudu.Schema kuduSchema = new org.apache.kudu.Schema(columnSchemas);
        CreateTableOptions tableOptions = new CreateTableOptions();
        tableOptions.addHashPartitions(new ArrayList<>(config.getColumns()), config.getBuckets());

        try {
          KuduTable table =
            kuduClient.createTable(config.optTableName, kuduSchema, tableOptions);
          LOG.info("Successfully create table '{}', Table ID '{}'", config.optTableName, table.getTableId());
        } catch (KuduException e) {
          throw new RuntimeException(
            String.format("Unable to create table '{}'. Reason : {}", config.optTableName, e.getMessage())
          );
        }
      }
    } catch (KuduException e) {
      LOG.warn(String.format("Unable to check if the table '%s' exists in kudu. Reason : %s",
                             config.optTableName, e.getMessage()));
    } catch (TypeConversionException e) {
      throw new RuntimeException(e.getCause());
    }

  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(config.referenceName, new KuduOutputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);

    // Parsing the schema should never fail here, because configure has validated it.
    outputSchema = Schema.parseJson(config.optSchema);
    kuduClient = new KuduClient.KuduClientBuilder(config.optMasterAddresses)
      .defaultOperationTimeoutMs(config.optOperationTimeoutMs)
      .defaultAdminOperationTimeoutMs(config.optAdminTimeoutMs)
      .build();
    kuduTable = kuduClient.openTable(config.optTableName);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Operation>> emitter) throws Exception {
    Insert insert = kuduTable.newInsert();
    PartialRow row = insert.getRow();
    List<Schema.Field> fields = outputSchema.getFields();
    for (Schema.Field field : fields) {
      Object val = input.get(field.getName());
      Schema schema = field.getSchema();
      if (!schema.isSimpleOrNullableSimple()) {
        // don't know what to do with it.
        continue;
      }
      switch (schema.getType()) {
        case BOOLEAN:
          row.addBoolean(field.getName(), (Boolean) val);
          break;
        case INT:
          row.addInt(field.getName(), (Integer) val);
          break;
        case LONG:
          row.addLong(field.getName(), (Long) val);
          break;
        case FLOAT:
          row.addFloat(field.getName(), (Float) val);
          break;
        case DOUBLE:
          row.addDouble(field.getName(), (Double) val);
          break;
        case BYTES:
          if (val instanceof ByteBuffer) {
            row.addBinary(field.getName(), (ByteBuffer) val);
          } else {
            row.addBinary(field.getName(), (byte[]) val);
          }
          break;
        case STRING:
          row.addString(field.getName(), (String) val);
          break;
        default:
          throw new IllegalArgumentException(
            "Field " + field.getName() + " is of unsupported type " + schema.getType()
          );
      }
    }
    emitter.emit(new KeyValue<NullWritable, Operation>(NullWritable.get(), insert));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    try {
      kuduClient.close();
    } catch (KuduException e) {
      LOG.warn("Error closing Kudu Client.", e);
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  private class KuduOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    KuduOutputFormatProvider(Config config) throws IOException {
      this.conf = new HashMap<>();
      this.conf.put("kudu.mapreduce.master.addresses", config.optMasterAddresses);
      this.conf.put("kudu.mapreduce.output.table", config.optTableName);
      this.conf.put("kudu.mapreduce.operation.timeout.ms", String.valueOf(config.optOperationTimeoutMs));
      this.conf.put("kudu.mapreduce.buffer.row.count", String.valueOf(config.optFlushRows));
    }

    @Override
    public String getOutputFormatClassName() {
      return KuduTableOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

}
