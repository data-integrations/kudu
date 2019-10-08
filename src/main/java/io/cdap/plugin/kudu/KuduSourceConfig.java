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
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;

import java.io.IOException;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Configuration class for the plugin.
 */
public final class KuduSourceConfig extends PluginConfig {
  public static final String REFERENCE_NAME = "referenceName";
  public static final String MASTER = "master";
  public static final String NAME = "name";
  public static final String SCHEMA = "schema";
  public static final String COLUMNS = "columns";
  public static final String OPT_TIMEOUT = "opt-timeout";

  // Required Fields.
  @Name(REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
  private String referenceName;

  @Name(MASTER)
  @Description("Comma separated list of <hostname>:<port>[,<hostanme>:<port>]* of Apache Kudu Masters.")
  @Macro
  private String optMasterAddresses;

  @Name(NAME)
  @Description("Name of the Kudu table.")
  @Macro
  private String optTableName;

  @Name(SCHEMA)
  @Description("Output schema from Kudu source")
  private String optSchema;

  // Optional Fields
  @Name(COLUMNS)
  @Description("Columns to be projected from the table")
  @Nullable
  private String optColumnProjection;

  @Name(OPT_TIMEOUT)
  @Description("Specifies the user operation timeout in milliseconds.")
  @Nullable
  private String optOperationTimeout;

  public KuduSourceConfig(String referenceName, String optMasterAddresses, String optTableName, String optSchema,
                          @Nullable String optColumnProjection, @Nullable String optOperationTimeout) {
    this.referenceName = referenceName;
    this.optMasterAddresses = optMasterAddresses;
    this.optTableName = optTableName;
    this.optSchema = optSchema;
    this.optColumnProjection = optColumnProjection;
    this.optOperationTimeout = optOperationTimeout;
  }

  private KuduSourceConfig(Builder builder) {
    referenceName = builder.referenceName;
    optMasterAddresses = builder.optMasterAddresses;
    optTableName = builder.optTableName;
    optSchema = builder.optSchema;
    optColumnProjection = builder.optColumnProjection;
    optOperationTimeout = builder.optOperationTimeout;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(KuduSourceConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setOptMasterAddresses(copy.optMasterAddresses)
      .setOptTableName(copy.optTableName)
      .setOptSchema(copy.optSchema)
      .setOptColumnProjection(copy.optColumnProjection)
      .setOptOperationTimeout(copy.optOperationTimeout);
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getOptMasterAddresses() {
    return optMasterAddresses;
  }

  public String getOptTableName() {
    return optTableName;
  }

  public String getOptSchema() {
    return optSchema;
  }

  @Nullable
  public String getOptColumnProjection() {
    return optColumnProjection;
  }

  @Nullable
  public String getOptOperationTimeout() {
    return optOperationTimeout;
  }

  /**
   * Validates the configuration fields.
   *
   * @throws IllegalArgumentException throw when there is issue with the configuration.
   */
  public void validate(FailureCollector failureCollector) throws IllegalArgumentException {
    IdUtils.validateReferenceName(referenceName, failureCollector);
    if (!containsMacro(MASTER)) {
      Arrays.stream(optMasterAddresses.split(",")).forEach(masterAddress -> {
        if (masterAddress.split(":").length != 2) {
          failureCollector.addFailure(String.format("Master address '%s' has incorrect format.", masterAddress),
                                      "Provide Master address in format - <hostname>:<port>")
            .withConfigProperty(MASTER);
        }
      });
    }

    try {
      Schema.parseJson(optSchema);
    } catch (IOException e) {
      failureCollector.addFailure(String.format("Unable to parse schema '%s'", optSchema),
                                  "Provide correct schema.")
        .withStacktrace(e.getStackTrace())
        .withConfigProperty(SCHEMA);
    }
  }

  /**
   * @return {@link Schema} object of the JSON.
   */
  public Schema getSchema() {
    try {
      return Schema.parseJson(optSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse schema '%s'. Reason: %s", optSchema, e.getMessage())
      );
    }
  }


  public static final class Builder {
    // Required Fields.
    private String referenceName;
    private String optMasterAddresses;
    private String optTableName;
    private String optSchema;
    // Optional Fields
    private String optColumnProjection;
    private String optOperationTimeout;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setOptMasterAddresses(String optMasterAddresses) {
      this.optMasterAddresses = optMasterAddresses;
      return this;
    }

    public Builder setOptTableName(String optTableName) {
      this.optTableName = optTableName;
      return this;
    }

    public Builder setOptSchema(String optSchema) {
      this.optSchema = optSchema;
      return this;
    }

    public Builder setOptColumnProjection(String optColumnProjection) {
      this.optColumnProjection = optColumnProjection;
      return this;
    }

    public Builder setOptOperationTimeout(String optOperationTimeout) {
      this.optOperationTimeout = optOperationTimeout;
      return this;
    }

    public KuduSourceConfig build() {
      return new KuduSourceConfig(this);
    }
  }
}
