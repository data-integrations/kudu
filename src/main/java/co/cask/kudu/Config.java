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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.kudu.ColumnSchema;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Plugin {@link co.cask.cdap.api.Config} for Apache Kudu
 */
public class Config extends ReferencePluginConfig {

  // Required Fields.

  @Name("name")
  @Description("Name of the Kudu table to write to.")
  @Macro
  public String optTableName;

  @Name("master-address")
  @Description("Comma-separated list of hostname:port for Kudu masters")
  @Macro
  public String optMasterAddresses;

  @Name("schema")
  @Description("Output schema for the kudu table.")
  public String optSchema;


  // Options Fields

  @Name("opt-timeout")
  @Description("Timeout for Kudu operations in milliseconds. Defaults is  '30000 ms'.")
  @Nullable
  @Macro
  public long optOperationTimeoutMs;

  @Name("admin-timeout")
  @Description("Administration operation time out. Default is '30000 ms'.")
  @Nullable
  @Macro
  public long optAdminTimeoutMs;

  @Name("seed")
  @Description("Seed to be used for hashing. Default is 'random seed'.")
  @Nullable
  @Macro
  public String optSeed;

  @Name("columns")
  @Description("List of columns that you would like to distribute data by. Default is 'all columns'")
  @Nullable
  @Macro
  public String optColumns;

  @Name("replicas")
  @Description("Specifies the number of replicas for the above table")
  @Nullable
  @Macro
  public int optReplicas;

  @Name("compression-algo")
  @Description("Compression algorithm to be applied on the columns. Default is 'snappy'")
  @Nullable
  @Macro
  public String optCompressionAlgorithm;

  @Name("encoding")
  @Description("Specifies the encoding to be applied on the schema. Default is 'auto'")
  @Nullable
  @Macro
  public String optEncoding;

  @Name("row-flush")
  @Description("Number of rows that are buffered before flushing to the tablet server")
  @Nullable
  @Macro
  public int optFlushRows;

  @Name("buckets")
  @Description("Specifies the number of buckets to split the table into.")
  @Macro
  public int optBucketsCounts;

  public Config(ColumnSchema.CompressionAlgorithm compression) {
    this("kudu");
  }

  public Config(String referenceName) {
    super(referenceName);
    this.optOperationTimeoutMs = 30000;
  }

  public ColumnSchema.CompressionAlgorithm getCompression() {
    return ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION;
  }

  public ColumnSchema.Encoding getEncoding() {
    return ColumnSchema.Encoding.AUTO_ENCODING;
  }

  public Set<String> getColumns() {
    return new HashSet<>();
  }

  public int getBuckets() {
    return 16;
  }
}