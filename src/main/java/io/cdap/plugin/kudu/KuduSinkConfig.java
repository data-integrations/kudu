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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.kudu.ColumnSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Plugin {@link io.cdap.cdap.api.Config} for Apache Kudu
 */
public class KuduSinkConfig extends ReferencePluginConfig {

  public static final String MASTER = "master";
  public static final String NAME = "name";
  public static final String SCHEMA = "schema";
  public static final String REFERENCE_NAME = "referenceName";

  // Required Fields.
  @Name(NAME)
  @Description("Name of the Kudu table to write to.")
  @Macro
  private String optTableName;

  @Name(MASTER)
  @Description("Comma-separated list of hostname:port for Kudu masters")
  @Macro
  private String optMasterAddresses;

  @Name(SCHEMA)
  @Description("Output schema for the kudu table.")
  @Macro
  private String optSchema;

  // Options Fields
  @Name("opt-timeout")
  @Description("Timeout for Kudu operations in milliseconds. Defaults is  '30000 ms'.")
  @Nullable
  private String optOperationTimeoutMs;

  @Name("admin-timeout")
  @Description("Administration operation time out. Default is '30000 ms'.")
  @Nullable
  private String optAdminTimeoutMs;

  @Name("seed")
  @Description("Seed to be used for hashing. Default is 0")
  @Nullable
  private String optSeed;

  @Name("columns")
  @Description("List of columns that you would like to distribute data by. Default is 'all columns'")
  @Nullable
  private String optColumns;

  @Name("replicas")
  @Description("Specifies the number of replicas for the above table")
  @Nullable
  private String optReplicas;

  @Name("compression-algo")
  @Description("Compression algorithm to be applied on the columns. Default is 'snappy'")
  @Nullable
  private String optCompressionAlgorithm;

  @Name("encoding")
  @Description("Specifies the encoding to be applied on the schema. Default is 'auto'")
  @Nullable
  private String optEncoding;

  @Name("row-flush")
  @Description("Number of rows that are buffered before flushing to the tablet server")
  @Nullable
  private String optFlushRows;

  @Name("buckets")
  @Description("Specifies the number of buckets to split the table into.")
  @Nullable
  private String optBucketsCounts;

  @Name("boss-threads")
  @Description("Specifies the number of boss threads to be used by the client.")
  @Nullable
  private String optBossThreads;

  public KuduSinkConfig(String referenceName, String optTableName, String optMasterAddresses, String optSchema,
                        @Nullable String optOperationTimeoutMs, @Nullable String optAdminTimeoutMs,
                        @Nullable String optSeed, @Nullable String optColumns, @Nullable String optReplicas,
                        @Nullable String optCompressionAlgorithm, @Nullable String optEncoding,
                        @Nullable String optFlushRows, @Nullable String optBucketsCounts,
                        @Nullable String optBossThreads) {
    super(referenceName);
    this.optTableName = optTableName;
    this.optMasterAddresses = optMasterAddresses;
    this.optSchema = optSchema;
    this.optOperationTimeoutMs = optOperationTimeoutMs;
    this.optAdminTimeoutMs = optAdminTimeoutMs;
    this.optSeed = optSeed;
    this.optColumns = optColumns;
    this.optReplicas = optReplicas;
    this.optCompressionAlgorithm = optCompressionAlgorithm;
    this.optEncoding = optEncoding;
    this.optFlushRows = optFlushRows;
    this.optBucketsCounts = optBucketsCounts;
    this.optBossThreads = optBossThreads;
  }

  private KuduSinkConfig(Builder builder) {
    super(builder.referenceName);
    optTableName = builder.optTableName;
    optMasterAddresses = builder.optMasterAddresses;
    optSchema = builder.optSchema;
    optOperationTimeoutMs = builder.optOperationTimeoutMs;
    optAdminTimeoutMs = builder.optAdminTimeoutMs;
    optSeed = builder.optSeed;
    optColumns = builder.optColumns;
    optReplicas = builder.optReplicas;
    optCompressionAlgorithm = builder.optCompressionAlgorithm;
    optEncoding = builder.optEncoding;
    optFlushRows = builder.optFlushRows;
    optBucketsCounts = builder.optBucketsCounts;
    optBossThreads = builder.optBossThreads;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(KuduSinkConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setOptTableName(copy.optTableName)
      .setOptMasterAddresses(copy.optMasterAddresses)
      .setOptSchema(copy.optSchema)
      .setOptOperationTimeoutMs(copy.optOperationTimeoutMs)
      .setOptAdminTimeoutMs(copy.optAdminTimeoutMs)
      .setOptSeed(copy.optSeed)
      .setOptColumns(copy.optColumns)
      .setOptReplicas(copy.optReplicas)
      .setOptCompressionAlgorithm(copy.optCompressionAlgorithm)
      .setOptEncoding(copy.optEncoding)
      .setOptFlushRows(copy.optFlushRows)
      .setOptBucketsCounts(copy.optBucketsCounts)
      .setOptBossThreads(copy.optBossThreads);
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

  public void validate(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(referenceName, failureCollector);

    try {
      Schema.parseJson(optSchema);
    } catch (IOException e) {
      failureCollector.addFailure(String.format("Unable to parse schema '%s'", optSchema),
                                  "Provide correct schema.")
        .withStacktrace(e.getStackTrace())
        .withConfigProperty(SCHEMA);
    }

    if (!containsMacro(MASTER)) {
      Arrays.stream(optMasterAddresses.split(",")).forEach(masterAddress -> {
        if (masterAddress.split(":").length != 2) {
          failureCollector.addFailure(String.format("Master Address '%s' has incorrect format.", masterAddress),
                                      "Provide Master Address in format - <hostname>:<port>")
            .withConfigProperty(MASTER);
        }
      });
    }
  }

  public static final class Builder {
    private String referenceName;
    // Required Fields.
    private String optTableName;
    private String optMasterAddresses;
    private String optSchema;
    // Options Fields
    private String optOperationTimeoutMs;
    private String optAdminTimeoutMs;
    private String optSeed;
    private String optColumns;
    private String optReplicas;
    private String optCompressionAlgorithm;
    private String optEncoding;
    private String optFlushRows;
    private String optBucketsCounts;
    private String optBossThreads;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setOptTableName(String optTableName) {
      this.optTableName = optTableName;
      return this;
    }

    public Builder setOptMasterAddresses(String optMasterAddresses) {
      this.optMasterAddresses = optMasterAddresses;
      return this;
    }

    public Builder setOptSchema(String optSchema) {
      this.optSchema = optSchema;
      return this;
    }

    public Builder setOptOperationTimeoutMs(String optOperationTimeoutMs) {
      this.optOperationTimeoutMs = optOperationTimeoutMs;
      return this;
    }

    public Builder setOptAdminTimeoutMs(String optAdminTimeoutMs) {
      this.optAdminTimeoutMs = optAdminTimeoutMs;
      return this;
    }

    public Builder setOptSeed(String optSeed) {
      this.optSeed = optSeed;
      return this;
    }

    public Builder setOptColumns(String optColumns) {
      this.optColumns = optColumns;
      return this;
    }

    public Builder setOptReplicas(String optReplicas) {
      this.optReplicas = optReplicas;
      return this;
    }

    public Builder setOptCompressionAlgorithm(String optCompressionAlgorithm) {
      this.optCompressionAlgorithm = optCompressionAlgorithm;
      return this;
    }

    public Builder setOptEncoding(String optEncoding) {
      this.optEncoding = optEncoding;
      return this;
    }

    public Builder setOptFlushRows(String optFlushRows) {
      this.optFlushRows = optFlushRows;
      return this;
    }

    public Builder setOptBucketsCounts(String optBucketsCounts) {
      this.optBucketsCounts = optBucketsCounts;
      return this;
    }

    public Builder setOptBossThreads(String optBossThreads) {
      this.optBossThreads = optBossThreads;
      return this;
    }

    public KuduSinkConfig build() {
      return new KuduSinkConfig(this);
    }
  }
}
