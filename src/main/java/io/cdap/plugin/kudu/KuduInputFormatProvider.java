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

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import org.apache.kudu.mapreduce.KuduTableInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Input Format provider for Kudu Table Input Format.
 */
public class KuduInputFormatProvider implements InputFormatProvider {
  private static final Logger LOG = LoggerFactory.getLogger(KuduInputFormatProvider.class);

  private final Map<String, String> conf = new HashMap<>();

  public KuduInputFormatProvider(KuduSourceConfig kuduSourceConfig) throws IOException {
    // Specifies the input table
    conf.put("kudu.mapreduce.input.table", kuduSourceConfig.optTableName);

    // Specifies where the kudu masters are.
    conf.put("kudu.mapreduce.master.address", kuduSourceConfig.optMasterAddresses);

    // Specifies how long we wait for operations to complete (default: 10s)
    conf.put("kudu.mapreduce.operation.timeout.ms", kuduSourceConfig.optOperationTimeout);

    // Specifies the column projection as a comma-separated list of column names.
    // '*' means to project all columns
    // 'empty string' means to project no columns
    conf.put("kudu.mapreduce.column.projection", kuduSourceConfig.optColumnProjection);
  }

  /**
   * @return Name of the class.
   */
  @Override
  public String getInputFormatClassName() {
    return KuduTableInputFormat.class.getName();
  }

  /**
   * @return Map of configurations.
   */
  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }
}
