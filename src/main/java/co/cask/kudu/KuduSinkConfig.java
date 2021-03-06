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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.kudu.ColumnSchema;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Plugin {@link co.cask.cdap.api.Config} for Apache Kudu
 */
public class KuduSinkConfig extends ReferencePluginConfig {

  // Required Fields.

  @Name("name")
  @Description("Name of the Kudu table to write to.")
  @Macro
  public String optTableName;

  @Name("master")
  @Description("Comma-separated list of hostname:port for Kudu masters")
  @Macro
  public String optMasterAddresses;

  @Name("schema")
  @Description("Output schema for the kudu table.")
  @Macro
  public String optSchema;


  // Options Fields

  @Name("opt-timeout")
  @Description("Timeout for Kudu operations in milliseconds. Defaults is  '30000 ms'.")
  @Nullable
  public String optOperationTimeoutMs;

  @Name("admin-timeout")
  @Description("Administration operation time out. Default is '30000 ms'.")
  @Nullable
  public String optAdminTimeoutMs;

  @Name("seed")
  @Description("Seed to be used for hashing. Default is 0")
  @Nullable
  public String optSeed;

  @Name("columns")
  @Description("List of columns that you would like to distribute data by. Default is 'all columns'")
  @Nullable
  public String optColumns;

  @Name("replicas")
  @Description("Specifies the number of replicas for the above table")
  @Nullable
  public String optReplicas;

  @Name("compression-algo")
  @Description("Compression algorithm to be applied on the columns. Default is 'snappy'")
  @Nullable
  public String optCompressionAlgorithm;

  @Name("encoding")
  @Description("Specifies the encoding to be applied on the schema. Default is 'auto'")
  @Nullable
  public String optEncoding;

  @Name("row-flush")
  @Description("Number of rows that are buffered before flushing to the tablet server")
  @Nullable
  public String optFlushRows;

  @Name("buckets")
  @Description("Specifies the number of buckets to split the table into.")
  @Nullable
  public String optBucketsCounts;

  @Name("boss-threads")
  @Description("Specifies the number of boss threads to be used by the client.")
  @Nullable
  private String optBossThreads;

  public KuduSinkConfig(ColumnSchema.CompressionAlgorithm compression) {
    this("kudu");
  }

  public KuduSinkConfig(String referenceName) {
    super(referenceName);
  }

  /**
   * @return cleaned up master address.
   */
  public String getMasterAddress() {
    return optMasterAddresses.trim();
  }

  /**
   * @return cleaned up table name.
   */
  public String getTableName() {
    return optTableName.trim();
  }

  /**
   * @return Compression algorithm to be associated with all the fields.
   */
  public ColumnSchema.CompressionAlgorithm getCompression() {
    ColumnSchema.CompressionAlgorithm algorithm = ColumnSchema.CompressionAlgorithm.SNAPPY;

    switch(optCompressionAlgorithm.toLowerCase()) {
      case "snappy":
        algorithm = ColumnSchema.CompressionAlgorithm.SNAPPY;
        break;

      case "lz4":
        algorithm = ColumnSchema.CompressionAlgorithm.LZ4;
        break;

      case "zlib":
        algorithm = ColumnSchema.CompressionAlgorithm.ZLIB;
        break;

      case "backend configured":
        algorithm = ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION;
        break;

      case "No Compression":
        algorithm = ColumnSchema.CompressionAlgorithm.NO_COMPRESSION;
        break;
    }
    return algorithm;
  }

  /**
   * @return Encoding to be applied to all the columns.
   */
  public ColumnSchema.Encoding getEncoding() {
    ColumnSchema.Encoding encoding = ColumnSchema.Encoding.AUTO_ENCODING;
    switch(optEncoding.toLowerCase()) {
      case "auto":
        encoding = ColumnSchema.Encoding.AUTO_ENCODING;
        break;

      case "plain":
        encoding = ColumnSchema.Encoding.PLAIN_ENCODING;
        break;

      case "prefix":
        encoding = ColumnSchema.Encoding.PREFIX_ENCODING;
        break;

      case "group variant":
        encoding = ColumnSchema.Encoding.GROUP_VARINT;
        break;

      case "rle":
        encoding = ColumnSchema.Encoding.RLE;
        break;

      case "dictionary":
        encoding = ColumnSchema.Encoding.DICT_ENCODING;
        break;

      case "bit shuffle":
        encoding = ColumnSchema.Encoding.BIT_SHUFFLE;
        break;
    }
    return encoding;
  }

  /**
   * @return List of columns to be used in hash.
   */
  public Set<String> getColumns() {
    Set<String> c = new HashSet<>();
    String[] columns = optColumns.split(",");
    for (String column : columns) {
      c.add(column);
    }
    return c;
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(optSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
  }

  /**
   * @return Number of replicas of a table on tablet servers.
   */
  public int getReplicas() {
    return (optReplicas != null) ? Integer.parseInt(optReplicas) : 1;
  }

  /**
   * @return Timeout for user operations.
   */
  public int getOperationTimeout() {
    return (optOperationTimeoutMs != null) ? Integer.parseInt(optOperationTimeoutMs) : 30000;
  }

  /**
   * @return Number of rows to be cached before being flushed.
   */
  public int getCacheRowCount() {
    return (optFlushRows != null) ? Integer.parseInt(optFlushRows) : 30000;
  }

  /**
   * @return Timeout for admin operations.
   */
  public int getAdministrationTimeout() {
    return (optAdminTimeoutMs != null) ? Integer.parseInt(optAdminTimeoutMs) : 30000;
  }

  /**
   * @return Number of buckets to be used for storing the rows.
   */
  public int getBuckets() {
    return (optBucketsCounts != null) ? Integer.parseInt(optBucketsCounts) : 16;
  }

  /**
   * @return Seed to be used for randomizing rows into hashed buckets.
   */
  public int getSeed() {
    return (optSeed != null) ? Integer.parseInt(optSeed) : 0;
  }

  /**
   * @return Number of boss threads to be used.
   */
  public int getThreads() {
    return (optBossThreads != null) ? Integer.parseInt(optBossThreads) : 1;
  }
}
