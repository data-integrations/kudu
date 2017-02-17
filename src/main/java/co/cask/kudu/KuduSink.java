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
import org.apache.hadoop.yarn.webapp.NotFoundException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link BatchSink} to write to Apache Kudu
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Kudu")
@Description("Writes to Kudu in text format.")
public class KuduSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Operation> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSink.class);

  private final KuduConfig config;

  private KuduClient kuduClient;
  private KuduTable kuduTable;
  private Schema outputSchema;

  public KuduSink(KuduConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.schema), "Schema must be given as a property.");

    Schema outputSchema;
    try {
      outputSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
    kuduClient = new KuduClient.KuduClientBuilder(config.masterAddresses)
      .defaultOperationTimeoutMs(config.operationTimeoutMs)
      .build();
    // Check if the table exists in Kudu.
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    try {
      if (! kuduClient.tableExists(this.config.tableName)) {
        throw new NotFoundException(
          String.format("Kudu table '%s' does not exist.", config.tableName)
        );
      }
    } catch (KuduException e) {
      LOG.warn(String.format("Unable to check if the table '%s' exists in kudu. Reason : %s",
                             config.tableName, e.getMessage()));
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(config.referenceName, new KuduOutputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    outputSchema = Schema.parseJson(config.schema);
    kuduClient = new KuduClient.KuduClientBuilder(config.masterAddresses)
      .defaultOperationTimeoutMs(config.operationTimeoutMs)
      .build();
    kuduTable = kuduClient.openTable(config.tableName);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Operation>> emitter) throws Exception {
    Insert insert = kuduTable.newInsert();
    PartialRow row = insert.getRow();
    List<Schema.Field> fields = outputSchema.getFields();
    for (Schema.Field field : fields) {
      Object val = input.get(field.getName());
      Schema schema = field.getSchema();
      if (schema.isNullable()) {
        schema = schema.getNonNullable();
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
      if (kuduClient != null) {
        kuduClient.close();
      }
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

    KuduOutputFormatProvider(KuduConfig config) throws IOException {
      this.conf = new HashMap<>();
      this.conf.put("kudu.mapreduce.master.addresses", config.masterAddresses);
      this.conf.put("kudu.mapreduce.output.table", config.tableName);
      this.conf.put("kudu.mapreduce.operation.timeout.ms", String.valueOf(config.operationTimeoutMs));
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
